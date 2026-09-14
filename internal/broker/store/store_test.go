package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

func event(key, source string) model.Telemetry {
	return model.Telemetry{
		SourceEventKey: source, SourceTime: time.Unix(1, 0).UTC(), ObservedAt: time.Unix(2, 0).UTC(),
		MetricName: "temperature", UUID: key, GPUID: "0", Device: "nvidia0", ModelName: "A100",
		Hostname: "node", Value: 40, LabelsRaw: "gpu=0", DatasetID: "test", CycleID: 1,
		AssignmentGeneration: 1, RowNumber: 1, StreamerID: "producer",
	}
}

func TestAppendFetchCommitDedupAndReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 2, 400, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	events := []model.Telemetry{event("GPU-a", "one"), event("GPU-b", "two"), event("GPU-a", "three")}
	result, err := store.Append(context.Background(), events)
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted != 3 || result.Duplicates != 0 {
		t.Fatalf("unexpected append result %+v", result)
	}
	duplicate, err := store.Append(context.Background(), []model.Telemetry{event("GPU-a", "one")})
	if err != nil || duplicate.Duplicates != 1 {
		t.Fatalf("dedup result=%+v err=%v", duplicate, err)
	}

	partition := store.PartitionFor("GPU-a")
	items, err := store.Fetch("group", partition, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Offset != 0 || items[0].Event.EventID == "" {
		t.Fatalf("unexpected deliveries %#v", items)
	}
	if err := store.Commit("group", map[uint32]uint64{partition: 1}); err != nil {
		t.Fatal(err)
	}
	if got := store.Committed("group", partition); got != 1 {
		t.Fatalf("committed=%d", got)
	}
	items, err = store.Fetch("group", partition, 10)
	if err != nil || len(items) != 1 || items[0].Offset != 1 {
		t.Fatalf("post-commit fetch=%#v err=%v", items, err)
	}
	if err := store.Commit("group", map[uint32]uint64{partition: 0}); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit("group", map[uint32]uint64{99: 1}); err == nil {
		t.Fatal("expected invalid partition commit")
	}
	if err := store.Commit("group", map[uint32]uint64{partition: 999}); err == nil {
		t.Fatal("expected invalid offset commit")
	}
	if _, err := store.Fetch("group", 99, 1); err == nil {
		t.Fatal("expected invalid fetch partition")
	}
	if values := store.HighWatermarks(); len(values) != 2 {
		t.Fatalf("watermarks=%#v", values)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, 2, 400, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Committed("group", partition) != 1 {
		t.Fatal("commit was not recovered")
	}
	result, err = reopened.Append(context.Background(), []model.Telemetry{event("GPU-a", "three")})
	if err != nil || result.Duplicates != 1 {
		t.Fatalf("dedup was not recovered: %+v %v", result, err)
	}
	if count, _ := filepath.Glob(filepath.Join(dir, "partition-*", "*.log")); len(count) < 2 {
		t.Fatalf("expected partition segment files, got %v", count)
	}
}

func TestValidationCancellationAndTornTailRecovery(t *testing.T) {
	if _, err := Open(t.TempDir(), 0, 1000, 1000); err == nil {
		t.Fatal("expected partition validation")
	}
	dir := t.TempDir()
	store, err := Open(dir, 1, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), []model.Telemetry{{}}); err == nil {
		t.Fatal("expected identity validation")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Append(cancelled, []model.Telemetry{event("GPU-a", "one")}); err == nil {
		t.Fatal("expected cancellation")
	}
	if _, err := store.Append(context.Background(), []model.Telemetry{event("GPU-a", "one")}); err != nil {
		t.Fatal(err)
	}
	if items, err := store.Fetch("group", 0, 0); err != nil || items != nil {
		t.Fatalf("zero fetch=%v err=%v", items, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	logs, _ := filepath.Glob(filepath.Join(dir, "partition-0000", "*.log"))
	file, err := os.OpenFile(logs[len(logs)-1], os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.Write([]byte{1, 2, 3, 4, 5})
	_ = file.Close()
	recovered, err := Open(dir, 1, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	items, err := recovered.Fetch("group", 0, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("recovery items=%d err=%v", len(items), err)
	}
}

func TestChecksumCorruptionIsRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 1, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), []model.Telemetry{event("GPU-a", "one")}); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	logs, _ := filepath.Glob(filepath.Join(dir, "partition-0000", "*.log"))
	file, err := os.OpenFile(logs[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, recordHeaderSize+3); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := Open(dir, 1, 1<<20, 1<<20); err == nil {
		t.Fatal("expected checksum error")
	}
}

func TestLoadCommitValidation(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "commits.json"), []byte(`{"g":{"invalid":1}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, 1, 1000, 1000); err == nil {
		t.Fatal("expected invalid commit partition")
	}
}

func TestCapacityBackpressureStillAllowsDedup(t *testing.T) {
	dir := t.TempDir()
	probe, err := Open(dir, 1, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	one := event("GPU-a", "one")
	encoded, _ := protocol.EncodeTelemetry(one)
	_ = probe.Close()
	store, err := OpenWithLimit(t.TempDir(), 1, 1<<20, 1<<20, int64(recordHeaderSize+len(encoded)+64))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Append(context.Background(), []model.Telemetry{one}); err != nil {
		t.Fatal(err)
	}
	if result, err := store.Append(context.Background(), []model.Telemetry{one}); err != nil || result.Duplicates != 1 {
		t.Fatalf("dedup at capacity=%+v err=%v", result, err)
	}
	if _, err := store.Append(context.Background(), []model.Telemetry{event("GPU-b", "two")}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("expected capacity error, got %v", err)
	}
}

func TestCommitReclaimsOnlyConsumedClosedSegments(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenWithLimit(dir, 1, 300, 1<<20, 10<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for index := 0; index < 5; index++ {
		if _, err := store.Append(context.Background(), []model.Telemetry{event("GPU-a", fmt.Sprintf("source-%d", index))}); err != nil {
			t.Fatal(err)
		}
	}
	logs, _ := filepath.Glob(filepath.Join(dir, "partition-0000", "*.log"))
	if len(logs) < 2 {
		t.Fatalf("expected rotated segments, got %v", logs)
	}
	if err := store.Commit("group", map[uint32]uint64{0: 5}); err != nil {
		t.Fatal(err)
	}
	remaining, _ := filepath.Glob(filepath.Join(dir, "partition-0000", "*.log"))
	if len(remaining) != 1 {
		t.Fatalf("expected only active segment after reclaim, got %v", remaining)
	}
}

func TestPartitionCountIsImmutable(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 4, 4096, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, 8, 4096, 1<<20); err == nil || !strings.Contains(err.Error(), "partition count is immutable") {
		t.Fatalf("expected immutable partition error, got %v", err)
	}
}
