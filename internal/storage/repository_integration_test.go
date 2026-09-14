//go:build integration

package storage

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

func TestPostgreSQLRepositoryEndToEnd(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var migrations sync.WaitGroup
	migrationErrors := make(chan error, 4)
	for range 4 {
		migrations.Add(1)
		go func() {
			defer migrations.Done()
			migrationErrors <- Migrate(ctx, databaseURL)
		}()
	}
	migrations.Wait()
	close(migrationErrors)
	for err := range migrationErrors {
		if err != nil {
			t.Fatalf("concurrent migration: %v", err)
		}
	}
	repo, err := Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	now := time.Now().UTC().Truncate(time.Microsecond)
	key := "integration-" + now.Format("20060102150405.000000")
	deliveries := []protocol.Delivery{
		{Partition: 1, Offset: 100, Event: model.Telemetry{EventID: key + "-1", SourceEventKey: key + "-source-1", SourceTime: now.Add(-time.Hour), ObservedAt: now, MetricName: "temp", UUID: key, GPUID: "0", Device: "nvidia0", ModelName: "A100", Hostname: "node", Value: 40, LabelsRaw: "gpu=0", DatasetID: "integration", CycleID: 1, RowNumber: 1, StreamerID: "test"}},
		{Partition: 1, Offset: 101, Event: model.Telemetry{EventID: key + "-2", SourceEventKey: key + "-source-2", SourceTime: now.Add(-time.Hour), ObservedAt: now.Add(time.Second), MetricName: "power", UUID: key, GPUID: "0", Device: "nvidia0", ModelName: "A100", Hostname: "node", Value: 100, LabelsRaw: "gpu=0", DatasetID: "integration", CycleID: 2, RowNumber: 1, StreamerID: "test"}},
	}
	offsets, err := repo.PersistBatch(ctx, deliveries)
	if err != nil || offsets[1] != 102 {
		t.Fatalf("offsets=%v err=%v", offsets, err)
	}
	if _, err := repo.PersistBatch(ctx, deliveries); err != nil {
		t.Fatalf("idempotent write: %v", err)
	}
	gpus, err := repo.ListGPUs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, gpu := range gpus {
		found = found || gpu.ID == key
	}
	if !found {
		t.Fatal("inserted GPU not listed")
	}
	page, err := repo.QueryTelemetry(ctx, TelemetryQuery{GPUKey: key, Limit: 1})
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("first page=%+v err=%v", page, err)
	}
	second, err := repo.QueryTelemetry(ctx, TelemetryQuery{GPUKey: key, Limit: 1, Cursor: page.NextCursor})
	if err != nil || len(second.Items) != 1 || second.Items[0].CycleID != 2 {
		t.Fatalf("second page=%+v err=%v", second, err)
	}
	exact, err := repo.QueryTelemetry(ctx, TelemetryQuery{GPUKey: key, EventID: key + "-1"})
	if err != nil || len(exact.Items) != 1 {
		t.Fatalf("exact=%+v err=%v", exact, err)
	}
}
