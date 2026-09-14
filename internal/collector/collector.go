package collector

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/client"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/observability"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/storage"
)

type Runner struct {
	cfg     config.Collector
	client  brokerClient
	repo    repository
	metrics *observability.Metrics
}

type brokerClient interface {
	RegisterConsumer(context.Context, string, string) (protocol.MembershipResponse, error)
	Fetch(context.Context, protocol.FetchRequest) ([]protocol.Delivery, error)
	Commit(context.Context, protocol.CommitRequest) error
}

type repository interface {
	PersistBatch(context.Context, []protocol.Delivery) (map[uint32]uint64, error)
}

func New(cfg config.Collector, repo *storage.Repository) *Runner {
	return &Runner{cfg: cfg, client: client.New(cfg.QueueAddr, 10*time.Second), repo: repo, metrics: observability.New("collector")}
}

func (r *Runner) MetricsHandler() http.Handler { return r.metrics.Handler() }

func newWithDependencies(cfg config.Collector, broker brokerClient, repo repository) *Runner {
	return &Runner{cfg: cfg, client: broker, repo: repo}
}

func (r *Runner) Run(ctx context.Context) error {
	const group = "telemetry-collectors"
	defer r.metrics.SetReady(false)
	var membership protocol.MembershipResponse
	for ctx.Err() == nil {
		if membership.IncarnationID == "" {
			started := time.Now()
			m, err := r.client.RegisterConsumer(ctx, group, r.cfg.CollectorID)
			r.metrics.Observe("register", err, 0, started)
			if err != nil {
				r.metrics.SetReady(false)
				r.wait(ctx)
				continue
			}
			membership = m
			r.metrics.SetReady(true)
		}
		started := time.Now()
		items, err := r.client.Fetch(ctx, protocol.FetchRequest{Group: group, MemberID: r.cfg.CollectorID, IncarnationID: membership.IncarnationID, Generation: membership.Generation, MaxRecords: r.cfg.BatchSize})
		r.metrics.Observe("fetch", err, len(items), started)
		if err != nil {
			slog.Warn("fetch failed; re-registering", "error", err)
			membership = protocol.MembershipResponse{}
			r.wait(ctx)
			continue
		}
		if len(items) == 0 {
			r.wait(ctx)
			continue
		}
		started = time.Now()
		next, err := r.repo.PersistBatch(ctx, items)
		r.metrics.Observe("persist", err, len(items), started)
		if err != nil {
			slog.Error("persist batch", "error", err)
			r.wait(ctx)
			continue
		}
		started = time.Now()
		if err := r.client.Commit(ctx, protocol.CommitRequest{Group: group, MemberID: r.cfg.CollectorID, IncarnationID: membership.IncarnationID, Generation: membership.Generation, Offsets: next}); err != nil {
			r.metrics.Observe("commit", err, 0, started)
			slog.Warn("database committed but broker offset commit failed", "error", err)
			membership = protocol.MembershipResponse{}
			continue
		}
		r.metrics.Observe("commit", nil, len(items), started)
		slog.Info("telemetry persisted", "records", len(items))
	}
	return nil
}

func (r *Runner) wait(ctx context.Context) {
	timer := time.NewTimer(r.cfg.PollWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
