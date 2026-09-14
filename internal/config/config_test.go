package config

import (
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	for _, name := range []string{"QUEUE_PARTITIONS", "STREAM_ROWS_PER_SECOND", "COLLECTOR_BATCH_SIZE", "API_MAX_RESULT_ROWS"} {
		t.Setenv(name, "")
	}
	queue, err := QueueFromEnv()
	if err != nil || queue.Partitions != 8 {
		t.Fatalf("queue=%+v err=%v", queue, err)
	}
	streamer, err := StreamerFromEnv()
	if err != nil || streamer.RowsPerSecond != 100 || streamer.HeartbeatInterval != 5*time.Second {
		t.Fatalf("streamer=%+v err=%v", streamer, err)
	}
	collector, err := CollectorFromEnv()
	if err != nil || collector.BatchSize != 200 {
		t.Fatalf("collector=%+v err=%v", collector, err)
	}
	api, err := APIFromEnv()
	if err != nil || api.QueryTimeout != 5*time.Second {
		t.Fatalf("api=%+v err=%v", api, err)
	}
}

func TestOverridesAndInvalidValuesReturnErrors(t *testing.T) {
	t.Setenv("QUEUE_PARTITIONS", "16")
	t.Setenv("QUEUE_MEMBER_TTL", "2s")
	queue, err := QueueFromEnv()
	if err != nil || queue.Partitions != 16 || queue.MemberTTL != 2*time.Second {
		t.Fatalf("queue=%+v err=%v", queue, err)
	}
	t.Setenv("QUEUE_PARTITIONS", "invalid")
	if _, err := QueueFromEnv(); err == nil {
		t.Fatal("expected integer parse error")
	}
	t.Setenv("QUEUE_PARTITIONS", "8")
	t.Setenv("QUEUE_MEMBER_TTL", "invalid")
	if _, err := QueueFromEnv(); err == nil {
		t.Fatal("expected duration parse error")
	}
	t.Setenv("STREAM_ROWS_PER_SECOND", "0")
	if _, err := StreamerFromEnv(); err == nil {
		t.Fatal("expected stream validation error")
	}
	t.Setenv("STREAM_ROWS_PER_SECOND", "100")
	t.Setenv("STREAM_HEARTBEAT_INTERVAL", "0s")
	if _, err := StreamerFromEnv(); err == nil {
		t.Fatal("expected heartbeat validation error")
	}
	t.Setenv("COLLECTOR_POLL_WAIT", "0s")
	if _, err := CollectorFromEnv(); err == nil {
		t.Fatal("expected collector validation error")
	}
	t.Setenv("API_MAX_RESULT_BYTES", "10")
	if _, err := APIFromEnv(); err == nil {
		t.Fatal("expected API validation error")
	}
}
