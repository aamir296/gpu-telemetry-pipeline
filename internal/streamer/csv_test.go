package streamer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/clock"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

const csvData = `metric_name,timestamp,value,Hostname,gpu_id,device,modelName,uuid,container,pod,namespace,labels_raw
temp,2024-01-01T00:00:00Z,42,node-a,0,nvidia0,A100,GPU-a,worker,pod-a,compute,"gpu=0"
power,2024-01-01T00:00:01Z,120,node-b,1,nvidia1,H100,GPU-b,worker,pod-b,compute,"gpu=1"
`

func TestDecoderMapsHeaderAndSourceIdentity(t *testing.T) {
	decoder, err := NewDecoder(strings.NewReader(csvData), "dataset")
	if err != nil {
		t.Fatal(err)
	}
	first, hash, err := decoder.Next()
	if err != nil {
		t.Fatal(err)
	}
	if first.MetricName != "temp" || first.GPUKey() != "GPU-a" || first.RowNumber != 1 || first.DatasetID != "dataset" || hash == "" {
		t.Fatalf("decoded=%+v hash=%q", first, hash)
	}
	key := SourceEventKey("dataset", 2, first.RowNumber, hash)
	if key != SourceEventKey("dataset", 2, first.RowNumber, hash) {
		t.Fatal("source key is not deterministic")
	}
	if key == SourceEventKey("dataset", 3, first.RowNumber, hash) {
		t.Fatal("cycle was omitted from source key")
	}
}

func TestDecoderRejectsInvalidInput(t *testing.T) {
	for _, test := range []struct{ name, data string }{
		{"missing header", "timestamp,metric_name\n2024-01-01T00:00:00Z,temp\n"},
		{"timestamp", strings.Replace(csvData, "2024-01-01T00:00:00Z", "bad", 1)},
		{"value", strings.Replace(csvData, ",42,", ",bad,", 1)},
		{"metric", strings.Replace(csvData, "temp,", ",", 1)},
		{"identity", strings.Replace(strings.Replace(csvData, "node-a", "", 1), "GPU-a", "", 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			decoder, err := NewDecoder(strings.NewReader(test.data), "dataset")
			if err == nil {
				_, _, err = decoder.Next()
			}
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

type fakeBroker struct {
	cycle         protocol.CycleResponse
	acquireErr    error
	acquireCalls  int
	onAcquire     func(int)
	publishErr    error
	publishErrors []error
	publishCalls  int
	published     []model.Telemetry
	complete      func()
	completeErr   error
}

func (f *fakeBroker) AcquireCycle(context.Context, string, string) (protocol.CycleResponse, error) {
	f.acquireCalls++
	if f.onAcquire != nil {
		f.onAcquire(f.acquireCalls)
	}
	return f.cycle, f.acquireErr
}
func (f *fakeBroker) CompleteCycle(context.Context, string, string) error {
	if f.complete != nil {
		f.complete()
	}
	return f.completeErr
}
func (f *fakeBroker) Publish(_ context.Context, values []model.Telemetry) (protocol.PublishResponse, error) {
	index := f.publishCalls
	f.publishCalls++
	if index < len(f.publishErrors) && f.publishErrors[index] != nil {
		return protocol.PublishResponse{}, f.publishErrors[index]
	}
	if f.publishErr != nil {
		return protocol.PublishResponse{}, f.publishErr
	}
	f.published = append(f.published, values...)
	return protocol.PublishResponse{Accepted: len(values)}, nil
}

func TestRunHandlesAcquireWaitAndCompletionErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeBroker{acquireErr: errors.New("offline"), onAcquire: func(int) { cancel() }}
	runner := &Runner{cfg: config.Streamer{RetryMin: time.Hour}, client: fake}
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	fake = &fakeBroker{cycle: protocol.CycleResponse{Ready: false, WaitMillis: 1000}, onAcquire: func(int) { cancel() }}
	runner = &Runner{cfg: config.Streamer{RetryMin: time.Millisecond}, client: fake}
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "input.csv")
	if err := os.WriteFile(path, []byte(csvData), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	fake = &fakeBroker{
		cycle:       protocol.CycleResponse{Ready: true, CycleID: 1, Generation: 1, Members: []string{"producer"}},
		complete:    cancel,
		completeErr: errors.New("completion unavailable"),
	}
	runner = &Runner{cfg: config.Streamer{CSVPath: path, DatasetID: "dataset", StreamerID: "producer", RowsPerSecond: 10000, BatchSize: 10, RetryMin: time.Millisecond, RetryMax: time.Millisecond}, clock: clock.System{}, client: fake}
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPublishRetriesTransientFailure(t *testing.T) {
	fake := &fakeBroker{publishErrors: []error{errors.New("temporary"), nil}}
	runner := &Runner{cfg: config.Streamer{RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond}, client: fake}
	if err := runner.publishWithRetry(context.Background(), []model.Telemetry{{UUID: "GPU-1"}}); err != nil {
		t.Fatal(err)
	}
	if fake.publishCalls != 2 {
		t.Fatalf("publish calls=%d", fake.publishCalls)
	}
}

func TestDurationHelpersAndCancelledWait(t *testing.T) {
	if minDuration(time.Second, 2*time.Second) != time.Second || minDuration(2*time.Second, time.Second) != time.Second {
		t.Fatal("minDuration")
	}
	if maxDuration(time.Second, 2*time.Second) != 2*time.Second || maxDuration(2*time.Second, time.Second) != 2*time.Second {
		t.Fatal("maxDuration")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&Runner{}).wait(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error=%v", err)
	}
}

func TestRunCycleAssignsDynamicTimeAndMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.csv")
	if err := os.WriteFile(path, []byte(csvData), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	fake := &fakeBroker{}
	runner := &Runner{cfg: config.Streamer{CSVPath: path, DatasetID: "dataset", StreamerID: "producer", RowsPerSecond: 10000, BatchSize: 1, RetryMin: time.Millisecond, RetryMax: time.Millisecond}, clock: clock.Fixed{Time: now}, client: fake}
	if err := runner.runCycle(context.Background(), 7, 3, []string{"producer"}); err != nil {
		t.Fatal(err)
	}
	if len(fake.published) != 2 {
		t.Fatalf("published=%d", len(fake.published))
	}
	for _, item := range fake.published {
		if !item.ObservedAt.Equal(now) || item.CycleID != 7 || item.AssignmentGeneration != 3 || item.StreamerID != "producer" || item.SourceEventKey == "" {
			t.Fatalf("metadata=%+v", item)
		}
		if item.ObservedAt.Equal(item.SourceTime) {
			t.Fatal("source timestamp reused as observation time")
		}
	}
}

func TestRunCycleHeartbeatsAndStopsOnAssignmentChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.csv")
	if err := os.WriteFile(path, []byte(csvData), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := &fakeBroker{cycle: protocol.CycleResponse{Ready: true, CycleID: 7, Generation: 4, Members: []string{"producer"}}}
	runner := &Runner{
		cfg: config.Streamer{
			CSVPath: path, DatasetID: "dataset", StreamerID: "producer",
			RowsPerSecond: 10000, BatchSize: 10, HeartbeatInterval: time.Nanosecond,
			RetryMin: time.Millisecond, RetryMax: time.Millisecond,
		},
		clock: clock.System{}, client: fake,
	}
	err := runner.runCycle(context.Background(), 7, 3, []string{"producer"})
	if err == nil || !strings.Contains(err.Error(), "assignment changed") {
		t.Fatalf("error=%v", err)
	}
	if fake.acquireCalls == 0 || len(fake.published) != 0 {
		t.Fatalf("acquires=%d published=%d", fake.acquireCalls, len(fake.published))
	}
}

func TestRunCompletesCycleAndFencedPublishStops(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.csv")
	if err := os.WriteFile(path, []byte(csvData), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeBroker{cycle: protocol.CycleResponse{Ready: true, CycleID: 1, Generation: 1, Members: []string{"producer"}}, complete: cancel}
	runner := &Runner{cfg: config.Streamer{CSVPath: path, DatasetID: "dataset", StreamerID: "producer", RowsPerSecond: 10000, BatchSize: 10, RetryMin: time.Millisecond, RetryMax: time.Millisecond}, clock: clock.System{}, client: fake}
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(fake.published) != 2 {
		t.Fatalf("published=%d", len(fake.published))
	}
	fake.publishErr = errors.New("broker_error: stale or invalid producer assignment")
	if err := runner.publishWithRetry(context.Background(), []model.Telemetry{{}}); err == nil {
		t.Fatal("fenced publish should stop retrying")
	}
}
