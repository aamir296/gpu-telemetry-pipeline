package streamer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/client"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/clock"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/observability"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/shard"
)

type Runner struct {
	cfg     config.Streamer
	clock   clock.Clock
	client  brokerClient
	metrics *observability.Metrics
}

type brokerClient interface {
	AcquireCycle(context.Context, string, string) (protocol.CycleResponse, error)
	CompleteCycle(context.Context, string, string) error
	Publish(context.Context, []model.Telemetry) (protocol.PublishResponse, error)
}

func New(cfg config.Streamer, c clock.Clock) *Runner {
	return &Runner{cfg: cfg, clock: c, client: client.New(cfg.QueueAddr, 10*time.Second), metrics: observability.New("streamer")}
}

func (r *Runner) MetricsHandler() http.Handler { return r.metrics.Handler() }

func (r *Runner) Run(ctx context.Context) error {
	defer r.metrics.SetReady(false)
	for ctx.Err() == nil {
		started := time.Now()
		cycle, err := r.client.AcquireCycle(ctx, r.cfg.DatasetID, r.cfg.StreamerID)
		r.metrics.Observe("acquire_cycle", err, 0, started)
		if err != nil {
			r.metrics.SetReady(false)
			r.wait(ctx, r.cfg.RetryMin)
			continue
		}
		r.metrics.SetReady(true)
		if !cycle.Ready {
			r.wait(ctx, maxDuration(time.Duration(cycle.WaitMillis)*time.Millisecond, 50*time.Millisecond))
			continue
		}
		started = time.Now()
		if err := r.runCycle(ctx, cycle.CycleID, cycle.Generation, cycle.Members); err != nil {
			r.metrics.Observe("cycle", err, 0, started)
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("stream cycle interrupted", "cycle", cycle.CycleID, "error", err)
			r.wait(ctx, r.cfg.RetryMin)
			continue
		}
		r.metrics.Observe("cycle", nil, 0, started)
		started = time.Now()
		if err := r.client.CompleteCycle(ctx, r.cfg.DatasetID, r.cfg.StreamerID); err != nil {
			r.metrics.Observe("complete_cycle", err, 0, started)
			slog.Warn("complete cycle", "error", err)
		} else {
			r.metrics.Observe("complete_cycle", nil, 0, started)
		}
	}
	return nil
}

func (r *Runner) runCycle(ctx context.Context, cycleID, generation uint64, members []string) error {
	f, err := os.Open(r.cfg.CSVPath)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder, err := NewDecoder(f, r.cfg.DatasetID)
	if err != nil {
		return err
	}
	interval := time.Second / time.Duration(r.cfg.RowsPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	owners := make(map[string]string)
	batch := make([]model.Telemetry, 0, r.cfg.BatchSize)
	heartbeatInterval := r.cfg.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}
	nextHeartbeat := time.Now().Add(heartbeatInterval)
	heartbeat := func() error {
		if time.Now().Before(nextHeartbeat) {
			return nil
		}
		started := time.Now()
		current, err := r.client.AcquireCycle(ctx, r.cfg.DatasetID, r.cfg.StreamerID)
		r.metrics.Observe("heartbeat", err, 0, started)
		if err != nil {
			return fmt.Errorf("producer heartbeat: %w", err)
		}
		if !current.Ready || current.CycleID != cycleID || current.Generation != generation || !slices.Equal(current.Members, members) {
			return fmt.Errorf("producer assignment changed during cycle %d", cycleID)
		}
		nextHeartbeat = time.Now().Add(heartbeatInterval)
		return nil
	}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		started := time.Now()
		records := len(batch)
		if err := r.publishWithRetry(ctx, batch); err != nil {
			r.metrics.Observe("publish", err, 0, started)
			return err
		}
		r.metrics.Observe("publish", nil, records, started)
		batch = batch[:0]
		return nil
	}
	for {
		if err := heartbeat(); err != nil {
			return err
		}
		t, rowHash, err := decoder.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		key := t.GPUKey()
		owner, exists := owners[key]
		if !exists {
			owner = shard.Owner("gpu", key, members)
			owners[key] = owner
		}
		if owner != r.cfg.StreamerID {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if err := heartbeat(); err != nil {
			return err
		}
		t.ObservedAt = r.clock.Now()
		t.StreamerID = r.cfg.StreamerID
		t.CycleID = cycleID
		t.AssignmentGeneration = generation
		t.SourceEventKey = SourceEventKey(r.cfg.DatasetID, cycleID, t.RowNumber, rowHash)
		batch = append(batch, t)
		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	slog.Info("stream cycle completed", "cycle", cycleID)
	return nil
}

func (r *Runner) publishWithRetry(ctx context.Context, events []model.Telemetry) error {
	delay := r.cfg.RetryMin
	for {
		if _, err := r.client.Publish(ctx, events); err == nil {
			return nil
		} else {
			// Membership changes fence old producers. Reacquire instead of retrying a
			// generation which can never become valid again.
			if strings.Contains(err.Error(), "stale or invalid producer assignment") {
				return err
			}
			slog.Warn("publish retry", "records", len(events), "error", err, "delay", delay)
		}
		if err := r.wait(ctx, delay+time.Duration(rand.Int64N(int64(maxDuration(delay/2, time.Millisecond))))); err != nil {
			return err
		}
		delay = minDuration(delay*2, r.cfg.RetryMax)
	}
}

func (r *Runner) wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
