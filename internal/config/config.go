package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Common struct {
	LogLevel string
	Metrics  string
}

type Queue struct {
	ListenAddr      string
	MetricsAddr     string
	DataDir         string
	Partitions      uint32
	SegmentBytes    int64
	MaxPendingBytes int64
	MemberTTL       time.Duration
	CycleInterval   time.Duration
	MaxBatchRecords int
	MaxRecordBytes  int
}

type Streamer struct {
	QueueAddr         string
	MetricsAddr       string
	CSVPath           string
	DatasetID         string
	StreamerID        string
	RowsPerSecond     int
	BatchSize         int
	HeartbeatInterval time.Duration
	RetryMin          time.Duration
	RetryMax          time.Duration
}

type Collector struct {
	QueueAddr   string
	MetricsAddr string
	DatabaseURL string
	CollectorID string
	BatchSize   int
	PollWait    time.Duration
}

type API struct {
	ListenAddr           string
	DatabaseURL          string
	QueryTimeout         time.Duration
	WriteTimeout         time.Duration
	MaxResultRows        int
	MaxResultBytes       int
	AllResultsConcurrent int
}

func QueueFromEnv() (Queue, error) {
	p := parser{}
	c := Queue{
		ListenAddr:      env("QUEUE_LISTEN_ADDR", ":7000"),
		MetricsAddr:     env("METRICS_LISTEN_ADDR", ":9090"),
		DataDir:         env("QUEUE_DATA_DIR", "./tmp/queue"),
		Partitions:      uint32(p.integer("QUEUE_PARTITIONS", 8)),
		SegmentBytes:    int64(p.integer("QUEUE_SEGMENT_BYTES", 8<<20)),
		MaxPendingBytes: int64(p.integer("QUEUE_MAX_PENDING_BYTES", 128<<20)),
		MemberTTL:       p.duration("QUEUE_MEMBER_TTL", 15*time.Second),
		CycleInterval:   p.duration("STREAM_CYCLE_INTERVAL", 10*time.Second),
		MaxBatchRecords: p.integer("QUEUE_MAX_BATCH_RECORDS", 500),
		MaxRecordBytes:  p.integer("QUEUE_MAX_RECORD_BYTES", 1<<20),
	}
	if p.err != nil {
		return c, p.err
	}
	if c.Partitions == 0 || c.Partitions > 1024 {
		return c, errors.New("QUEUE_PARTITIONS must be between 1 and 1024")
	}
	if c.SegmentBytes < 4096 {
		return c, errors.New("QUEUE_SEGMENT_BYTES must be at least 4096")
	}
	if c.MaxPendingBytes < c.SegmentBytes || c.MaxBatchRecords < 1 || c.MaxRecordBytes < 128 || c.MemberTTL <= 0 || c.CycleInterval <= 0 {
		return c, errors.New("queue limits and durations must be positive; pending bytes must cover at least one segment")
	}
	return c, nil
}

func StreamerFromEnv() (Streamer, error) {
	host, _ := os.Hostname()
	p := parser{}
	c := Streamer{
		QueueAddr:         env("QUEUE_ADDR", "127.0.0.1:7000"),
		MetricsAddr:       env("METRICS_LISTEN_ADDR", ":9090"),
		CSVPath:           env("CSV_PATH", "input/real-dcgm.csv"),
		DatasetID:         env("DATASET_ID", "dcgm-sample"),
		StreamerID:        env("STREAMER_ID", host),
		RowsPerSecond:     p.integer("STREAM_ROWS_PER_SECOND", 100),
		BatchSize:         p.integer("STREAM_BATCH_SIZE", 100),
		HeartbeatInterval: p.duration("STREAM_HEARTBEAT_INTERVAL", 5*time.Second),
		RetryMin:          p.duration("STREAM_RETRY_MIN", 100*time.Millisecond),
		RetryMax:          p.duration("STREAM_RETRY_MAX", 5*time.Second),
	}
	if p.err != nil {
		return c, p.err
	}
	if c.DatasetID == "" || c.StreamerID == "" {
		return c, errors.New("DATASET_ID and STREAMER_ID are required")
	}
	if c.RowsPerSecond < 1 || c.BatchSize < 1 {
		return c, errors.New("stream rate and batch size must be positive")
	}
	if c.HeartbeatInterval <= 0 || c.RetryMin <= 0 || c.RetryMax < c.RetryMin {
		return c, errors.New("stream heartbeat and retry durations must be positive and retry max must be at least min")
	}
	if time.Second/time.Duration(c.RowsPerSecond) <= 0 {
		return c, errors.New("STREAM_ROWS_PER_SECOND is too large")
	}
	return c, nil
}

func CollectorFromEnv() (Collector, error) {
	host, _ := os.Hostname()
	p := parser{}
	c := Collector{
		QueueAddr:   env("QUEUE_ADDR", "127.0.0.1:7000"),
		MetricsAddr: env("METRICS_LISTEN_ADDR", ":9090"),
		DatabaseURL: env("DATABASE_URL", "postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable"),
		CollectorID: env("COLLECTOR_ID", host),
		BatchSize:   p.integer("COLLECTOR_BATCH_SIZE", 200),
		PollWait:    p.duration("COLLECTOR_POLL_WAIT", 250*time.Millisecond),
	}
	if p.err != nil {
		return c, p.err
	}
	if c.DatabaseURL == "" || c.CollectorID == "" || c.BatchSize < 1 || c.PollWait <= 0 {
		return c, errors.New("valid DATABASE_URL, COLLECTOR_ID, and COLLECTOR_BATCH_SIZE are required")
	}
	return c, nil
}

func APIFromEnv() (API, error) {
	p := parser{}
	c := API{
		ListenAddr:           env("API_LISTEN_ADDR", ":8080"),
		DatabaseURL:          env("DATABASE_URL", "postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable"),
		QueryTimeout:         p.duration("API_QUERY_TIMEOUT", 5*time.Second),
		WriteTimeout:         p.duration("API_WRITE_TIMEOUT", 30*time.Second),
		MaxResultRows:        p.integer("API_MAX_RESULT_ROWS", 100000),
		MaxResultBytes:       p.integer("API_MAX_RESULT_BYTES", 16<<20),
		AllResultsConcurrent: p.integer("API_ALL_RESULTS_CONCURRENCY", 2),
	}
	if p.err != nil {
		return c, p.err
	}
	if c.MaxResultRows < 1 || c.MaxResultBytes < 1024 || c.AllResultsConcurrent < 1 || c.QueryTimeout <= 0 || c.WriteTimeout <= 0 {
		return c, errors.New("API result limits must be positive")
	}
	return c, nil
}

func env(name, fallback string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return fallback
}

type parser struct{ err error }

func (p *parser) integer(name string, fallback int) int {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		if p.err == nil {
			p.err = fmt.Errorf("%s must be an integer: %w", name, err)
		}
		return fallback
	}
	return n
}

func (p *parser) duration(name string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		if p.err == nil {
			p.err = fmt.Errorf("%s must be a duration: %w", name, err)
		}
		return fallback
	}
	return d
}
