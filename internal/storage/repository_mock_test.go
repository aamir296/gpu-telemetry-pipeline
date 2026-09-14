package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
)

func mockRepository(t *testing.T) (*Repository, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		mock.Close()
	})
	return &Repository{pool: mock}, mock
}

func storedDelivery(offset uint64) protocol.Delivery {
	return protocol.Delivery{
		Partition: 2,
		Offset:    offset,
		Event: model.Telemetry{
			EventID: "event-" + time.Unix(int64(offset), 0).Format("05"), SourceEventKey: "source-" + time.Unix(int64(offset), 0).Format("05"),
			SourceTime: time.Unix(1, 0).UTC(), ObservedAt: time.Unix(int64(offset+10), 0).UTC(),
			MetricName: "temperature", UUID: "GPU-1", GPUID: "0", Device: "nvidia0", ModelName: "H100",
			Hostname: "node-1", Container: "trainer", Pod: "pod-1", Namespace: "ml", Value: 42,
			LabelsRaw: "gpu=0", DatasetID: "dataset", CycleID: 1, AssignmentGeneration: 2, RowNumber: offset + 1, StreamerID: "producer",
		},
	}
}

func anyArgs(count int) []any {
	values := make([]any, count)
	for index := range values {
		values[index] = pgxmock.AnyArg()
	}
	return values
}

func TestPersistBatchTransactionAndOffsets(t *testing.T) {
	repository, mock := mockRepository(t)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO gpus").WithArgs(anyArgs(8)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("INSERT INTO telemetry_events").WithArgs(anyArgs(23)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("INSERT INTO telemetry_events").WithArgs(anyArgs(23)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("INSERT INTO telemetry_partition_watermarks").WithArgs(int32(2), int64(11)).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	offsets, err := repository.PersistBatch(context.Background(), []protocol.Delivery{storedDelivery(10), storedDelivery(11)})
	if err != nil || offsets[2] != 12 {
		t.Fatalf("offsets=%v err=%v", offsets, err)
	}
	if offsets, err := repository.PersistBatch(context.Background(), nil); err != nil || offsets != nil {
		t.Fatalf("empty offsets=%v err=%v", offsets, err)
	}
}

func TestPersistBatchRejectsMissingIdentity(t *testing.T) {
	repository, mock := mockRepository(t)
	mock.ExpectBegin()
	mock.ExpectRollback()
	_, err := repository.PersistBatch(context.Background(), []protocol.Delivery{{Event: model.Telemetry{SourceEventKey: "source"}}})
	if err == nil || err.Error() != "telemetry missing GPU identity" {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestPersistBatchRollsBackDatabaseErrors(t *testing.T) {
	tests := []struct {
		name   string
		setUp  func(pgxmock.PgxPoolIface)
		commit bool
	}{
		{"begin", func(mock pgxmock.PgxPoolIface) { mock.ExpectBegin().WillReturnError(errors.New("begin")) }, false},
		{"gpu", func(mock pgxmock.PgxPoolIface) {
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO gpus").WithArgs(anyArgs(8)...).WillReturnError(errors.New("gpu"))
			mock.ExpectRollback()
		}, false},
		{"event", func(mock pgxmock.PgxPoolIface) {
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO gpus").WithArgs(anyArgs(8)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
			mock.ExpectExec("INSERT INTO telemetry_events").WithArgs(anyArgs(23)...).WillReturnError(errors.New("event"))
			mock.ExpectRollback()
		}, false},
		{"watermark", func(mock pgxmock.PgxPoolIface) {
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO gpus").WithArgs(anyArgs(8)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
			mock.ExpectExec("INSERT INTO telemetry_events").WithArgs(anyArgs(23)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
			mock.ExpectExec("INSERT INTO telemetry_partition_watermarks").WithArgs(anyArgs(2)...).WillReturnError(errors.New("watermark"))
			mock.ExpectRollback()
		}, false},
		{"commit", func(mock pgxmock.PgxPoolIface) {
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO gpus").WithArgs(anyArgs(8)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
			mock.ExpectExec("INSERT INTO telemetry_events").WithArgs(anyArgs(23)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
			mock.ExpectExec("INSERT INTO telemetry_partition_watermarks").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
			mock.ExpectCommit().WillReturnError(errors.New("commit"))
		}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository, mock := mockRepository(t)
			test.setUp(mock)
			if _, err := repository.PersistBatch(context.Background(), []protocol.Delivery{storedDelivery(10)}); err == nil {
				t.Fatal("expected database error")
			}
		})
	}
}

func TestListGPUs(t *testing.T) {
	repository, mock := mockRepository(t)
	now := time.Unix(20, 0).UTC()
	rows := pgxmock.NewRows([]string{"gpu_key", "uuid", "hostname", "gpu_id", "device", "model_name", "first_seen", "last_seen"}).
		AddRow("GPU-1", "GPU-1", "node", "0", "nvidia0", "H100", now, now)
	mock.ExpectQuery("SELECT gpu_key").WillReturnRows(rows)
	items, err := repository.ListGPUs(context.Background())
	if err != nil || len(items) != 1 || items[0].ID != "GPU-1" {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}

func TestPruneBefore(t *testing.T) {
	repository, mock := mockRepository(t)
	before := time.Unix(100, 0).UTC()
	mock.ExpectExec("WITH doomed").WithArgs(before, 100).WillReturnResult(pgxmock.NewResult("DELETE", 3))
	deleted, err := repository.PruneBefore(context.Background(), before, 100)
	if err != nil || deleted != 3 {
		t.Fatalf("deleted=%d err=%v", deleted, err)
	}
	if _, err := repository.PruneBefore(context.Background(), before, 0); err == nil {
		t.Fatal("expected invalid batch size")
	}
}

func telemetryRows(deliveries ...protocol.Delivery) *pgxmock.Rows {
	rows := pgxmock.NewRows([]string{
		"event_id", "source_event_key", "source_timestamp", "observed_at", "ingested_at", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container_name", "pod_name", "namespace_name", "metric_value", "labels_raw", "dataset_id", "cycle_id", "assignment_generation", "row_number", "streamer_id", "queue_partition", "queue_offset", "ingest_seq",
	})
	for index, delivery := range deliveries {
		event := delivery.Event
		rows.AddRow(event.EventID, event.SourceEventKey, event.SourceTime, event.ObservedAt, event.ObservedAt.Add(time.Second), event.MetricName, event.GPUID, event.Device, event.UUID, event.ModelName, event.Hostname, event.Container, event.Pod, event.Namespace, event.Value, event.LabelsRaw, event.DatasetID, int64(event.CycleID), int64(event.AssignmentGeneration), int64(event.RowNumber), event.StreamerID, int32(delivery.Partition), int64(delivery.Offset), int64(index+1))
	}
	return rows
}

func TestQueryTelemetryPaginationAndFilters(t *testing.T) {
	repository, mock := mockRepository(t)
	first, second := storedDelivery(10), storedDelivery(11)
	mock.ExpectQuery("SELECT queue_partition").WithArgs("GPU-1").WillReturnRows(pgxmock.NewRows([]string{"queue_partition"}).AddRow(int32(2)))
	mock.ExpectQuery("SELECT highest_offset").WithArgs(int32(2)).WillReturnRows(pgxmock.NewRows([]string{"highest_offset"}).AddRow(int64(99)))
	mock.ExpectQuery("SELECT event_id").WithArgs(anyArgs(8)...).WillReturnRows(telemetryRows(first, second))
	start, end := first.Event.ObservedAt.Add(-time.Second), second.Event.ObservedAt.Add(time.Second)
	page, err := repository.QueryTelemetry(context.Background(), TelemetryQuery{GPUKey: "GPU-1", Start: &start, End: &end, EventID: first.Event.EventID, MetricName: "temperature", Limit: 1})
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" || page.Items[0].Container != "trainer" {
		t.Fatalf("page=%+v err=%v", page, err)
	}

	mock.ExpectQuery("SELECT queue_partition").WithArgs("GPU-1").WillReturnRows(pgxmock.NewRows([]string{"queue_partition"}).AddRow(int32(2)))
	mock.ExpectQuery("SELECT event_id").WithArgs(anyArgs(6)...).WillReturnRows(telemetryRows(second))
	next, err := repository.QueryTelemetry(context.Background(), TelemetryQuery{GPUKey: "GPU-1", Cursor: page.NextCursor, Limit: 1})
	if err != nil || len(next.Items) != 1 || next.Items[0].EventID != second.Event.EventID {
		t.Fatalf("next=%+v err=%v", next, err)
	}
}

func TestQueryTelemetryMissingWatermarkAndCursorPartition(t *testing.T) {
	repository, mock := mockRepository(t)
	mock.ExpectQuery("SELECT queue_partition").WithArgs("GPU-1").WillReturnRows(pgxmock.NewRows([]string{"queue_partition"}).AddRow(int32(2)))
	mock.ExpectQuery("SELECT highest_offset").WithArgs(int32(2)).WillReturnError(pgx.ErrNoRows)
	page, err := repository.QueryTelemetry(context.Background(), TelemetryQuery{GPUKey: "GPU-1"})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("page=%+v err=%v", page, err)
	}

	cursor, err := encodeCursor(Cursor{Version: 1, Partition: 7, Watermark: 9, Observed: time.Now().UTC(), Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT queue_partition").WithArgs("GPU-1").WillReturnRows(pgxmock.NewRows([]string{"queue_partition"}).AddRow(int32(2)))
	if _, err := repository.QueryTelemetry(context.Background(), TelemetryQuery{GPUKey: "GPU-1", Cursor: cursor}); err == nil {
		t.Fatal("expected cursor partition mismatch")
	}
}
