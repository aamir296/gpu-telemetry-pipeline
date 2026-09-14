package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pool interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Close()
}

type Repository struct{ pool pool }

func Open(ctx context.Context, databaseURL string) (*Repository, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() { r.pool.Close() }

// PersistBatch inserts events and advances each partition watermark atomically.
func (r *Repository) PersistBatch(ctx context.Context, deliveries []protocol.Delivery) (map[uint32]uint64, error) {
	if len(deliveries) == 0 {
		return nil, nil
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	seenGPU := make(map[string]model.Telemetry)
	nextOffsets := make(map[uint32]uint64)
	for _, item := range deliveries {
		t := item.Event
		key := t.GPUKey()
		if key == "" {
			return nil, errors.New("telemetry missing GPU identity")
		}
		t.EventID = item.Event.EventID
		t.QueuePartition, t.QueueOffset = item.Partition, item.Offset
		seenGPU[key] = t
		if next := item.Offset + 1; next > nextOffsets[item.Partition] {
			nextOffsets[item.Partition] = next
		}
	}
	for key, t := range seenGPU {
		_, err = tx.Exec(ctx, `
			INSERT INTO gpus(gpu_key,uuid,hostname,gpu_id,device,model_name,queue_partition,first_seen,last_seen)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8)
			ON CONFLICT(gpu_key) DO UPDATE SET
			  uuid=EXCLUDED.uuid, hostname=EXCLUDED.hostname, gpu_id=EXCLUDED.gpu_id,
			  device=EXCLUDED.device, model_name=EXCLUDED.model_name,
			  queue_partition=EXCLUDED.queue_partition,
			  last_seen=GREATEST(gpus.last_seen,EXCLUDED.last_seen)`,
			key, t.UUID, t.Hostname, t.GPUID, t.Device, t.ModelName, int32(t.QueuePartition), t.ObservedAt)
		if err != nil {
			return nil, err
		}
	}
	for _, item := range deliveries {
		t := item.Event
		_, err = tx.Exec(ctx, `
			INSERT INTO telemetry_events(
			 event_id,source_event_key,queue_partition,queue_offset,source_timestamp,observed_at,
			 metric_name,gpu_key,gpu_id,device,uuid,model_name,hostname,container_name,pod_name,
			 namespace_name,metric_value,labels_raw,dataset_id,cycle_id,assignment_generation,row_number,streamer_id)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
			ON CONFLICT(source_event_key) DO NOTHING`,
			t.EventID, t.SourceEventKey, int32(item.Partition), int64(item.Offset), t.SourceTime, t.ObservedAt,
			t.MetricName, t.GPUKey(), t.GPUID, t.Device, t.UUID, t.ModelName, t.Hostname, t.Container, t.Pod,
			t.Namespace, t.Value, t.LabelsRaw, t.DatasetID, int64(t.CycleID), int64(t.AssignmentGeneration), int64(t.RowNumber), t.StreamerID)
		if err != nil {
			return nil, err
		}
	}
	for partition, next := range nextOffsets {
		highest := int64(next - 1)
		_, err = tx.Exec(ctx, `
			INSERT INTO telemetry_partition_watermarks(queue_partition,highest_offset)
			VALUES($1,$2)
			ON CONFLICT(queue_partition) DO UPDATE SET
			 highest_offset=GREATEST(telemetry_partition_watermarks.highest_offset,EXCLUDED.highest_offset),
			 updated_at=clock_timestamp()`, int32(partition), highest)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return nextOffsets, nil
}

func (r *Repository) ListGPUs(ctx context.Context) ([]model.GPU, error) {
	rows, err := r.pool.Query(ctx, `SELECT gpu_key,uuid,hostname,gpu_id,device,model_name,first_seen,last_seen FROM gpus WHERE EXISTS (SELECT 1 FROM telemetry_events WHERE telemetry_events.gpu_key=gpus.gpu_key) ORDER BY gpu_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []model.GPU
	for rows.Next() {
		var value model.GPU
		if err := rows.Scan(&value.ID, &value.UUID, &value.Hostname, &value.GPUID, &value.Device, &value.ModelName, &value.FirstSeen, &value.LastSeen); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

// PruneBefore deletes a bounded batch of events older than the configured
// retention boundary. Retention is opt-in so the exercise defaults to complete
// history while production operators can enforce a data lifecycle policy.
func (r *Repository) PruneBefore(ctx context.Context, before time.Time, batchSize int) (int64, error) {
	if batchSize < 1 {
		return 0, errors.New("retention batch size must be positive")
	}
	result, err := r.pool.Exec(ctx, `
		WITH doomed AS (
			SELECT ingest_seq FROM telemetry_events
			WHERE ingested_at < $1 ORDER BY ingest_seq LIMIT $2
		)
		DELETE FROM telemetry_events WHERE ingest_seq IN (SELECT ingest_seq FROM doomed)`, before, batchSize)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

type TelemetryQuery struct {
	GPUKey     string
	Start      *time.Time
	End        *time.Time
	EventID    string
	MetricName string
	Limit      int
	Cursor     string
}

type Cursor struct {
	Version   uint8     `json:"v"`
	Partition uint32    `json:"p"`
	Watermark uint64    `json:"w"`
	Observed  time.Time `json:"t"`
	Sequence  int64     `json:"s"`
}

type TelemetryPage struct {
	Items      []model.Telemetry `json:"items"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

func (r *Repository) QueryTelemetry(ctx context.Context, query TelemetryQuery) (TelemetryPage, error) {
	var partition int32
	if err := r.pool.QueryRow(ctx, `SELECT queue_partition FROM gpus WHERE gpu_key=$1`, query.GPUKey).Scan(&partition); err != nil {
		return TelemetryPage{}, err
	}
	var cursor Cursor
	if query.Cursor != "" {
		decoded, err := decodeCursor(query.Cursor)
		if err != nil {
			return TelemetryPage{}, err
		}
		cursor = decoded
		if cursor.Partition != uint32(partition) {
			return TelemetryPage{}, errors.New("cursor does not belong to requested GPU")
		}
	} else {
		var watermark int64
		if err := r.pool.QueryRow(ctx, `SELECT highest_offset FROM telemetry_partition_watermarks WHERE queue_partition=$1`, partition).Scan(&watermark); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return TelemetryPage{Items: []model.Telemetry{}}, nil
			}
			return TelemetryPage{}, err
		}
		cursor.Version, cursor.Partition, cursor.Watermark = 1, uint32(partition), uint64(watermark)
	}
	conditions := []string{"gpu_key=$1", "queue_partition=$2", "queue_offset <= $3"}
	args := []any{query.GPUKey, partition, int64(cursor.Watermark)}
	add := func(condition string, value any) {
		args = append(args, value)
		conditions = append(conditions, fmt.Sprintf(condition, len(args)))
	}
	if query.Start != nil {
		add("observed_at >= $%d", *query.Start)
	}
	if query.End != nil {
		add("observed_at <= $%d", *query.End)
	}
	if query.EventID != "" {
		add("event_id = $%d", query.EventID)
	}
	if query.MetricName != "" {
		add("metric_name = $%d", query.MetricName)
	}
	if !cursor.Observed.IsZero() {
		args = append(args, cursor.Observed, cursor.Sequence)
		conditions = append(conditions, fmt.Sprintf("(observed_at,ingest_seq) > ($%d,$%d)", len(args)-1, len(args)))
	}
	limit := ""
	if query.Limit > 0 {
		args = append(args, query.Limit+1)
		limit = fmt.Sprintf(" LIMIT $%d", len(args))
	}
	sql := `SELECT event_id,source_event_key,source_timestamp,observed_at,ingested_at,metric_name,gpu_id,device,uuid,model_name,hostname,container_name,pod_name,namespace_name,metric_value,labels_raw,dataset_id,cycle_id,assignment_generation,row_number,streamer_id,queue_partition,queue_offset,ingest_seq FROM telemetry_events WHERE ` + strings.Join(conditions, " AND ") + ` ORDER BY observed_at,ingest_seq` + limit
	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return TelemetryPage{}, err
	}
	defer rows.Close()
	var page TelemetryPage
	var sequences []int64
	for rows.Next() {
		var t model.Telemetry
		var cycle, generation, row int64
		var partition int32
		var offset, seq int64
		if err := rows.Scan(&t.EventID, &t.SourceEventKey, &t.SourceTime, &t.ObservedAt, &t.IngestedAt, &t.MetricName, &t.GPUID, &t.Device, &t.UUID, &t.ModelName, &t.Hostname, &t.Container, &t.Pod, &t.Namespace, &t.Value, &t.LabelsRaw, &t.DatasetID, &cycle, &generation, &row, &t.StreamerID, &partition, &offset, &seq); err != nil {
			return TelemetryPage{}, err
		}
		t.CycleID, t.AssignmentGeneration, t.RowNumber, t.QueuePartition, t.QueueOffset = uint64(cycle), uint64(generation), uint64(row), uint32(partition), uint64(offset)
		page.Items = append(page.Items, t)
		sequences = append(sequences, seq)
	}
	if err := rows.Err(); err != nil {
		return TelemetryPage{}, err
	}
	if query.Limit > 0 && len(page.Items) > query.Limit {
		page.Items = page.Items[:query.Limit]
		last := page.Items[len(page.Items)-1]
		cursor.Observed, cursor.Sequence = last.ObservedAt, sequences[query.Limit-1]
		page.NextCursor, _ = encodeCursor(cursor)
	}
	if page.Items == nil {
		page.Items = []model.Telemetry{}
	}
	return page, nil
}

func encodeCursor(cursor Cursor) (string, error) {
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeCursor(value string) (Cursor, error) {
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return Cursor{}, errors.New("invalid cursor encoding")
	}
	var cursor Cursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return Cursor{}, errors.New("invalid cursor")
	}
	if cursor.Version != 1 || cursor.Observed.IsZero() || cursor.Sequence < 1 {
		return Cursor{}, errors.New("invalid or unsupported cursor")
	}
	return cursor, nil
}
