package server

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

func TestServerClientEndToEnd(t *testing.T) {
	server, err := New("127.0.0.1:0", t.TempDir(), 4, 1<<20, 10<<20, 10, 1<<20, time.Second, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer server.store.Close()
	ctx := context.Background()
	var cycle protocol.CycleResponse
	err = control(server, protocol.OpAcquireCycle, protocol.CycleRequest{DatasetID: "dataset", MemberID: "producer"}, &cycle)
	if err != nil || !cycle.Ready || cycle.CycleID != 1 {
		t.Fatalf("cycle=%+v err=%v", cycle, err)
	}
	event := model.Telemetry{
		SourceEventKey: "source-1", SourceTime: time.Unix(1, 0), ObservedAt: time.Now().UTC(),
		MetricName: "temp", UUID: "GPU-1", GPUID: "0", Device: "nvidia0", ModelName: "A100",
		Hostname: "node", Value: 50, DatasetID: "dataset", CycleID: cycle.CycleID,
		AssignmentGeneration: cycle.Generation, RowNumber: 1, StreamerID: "producer",
	}
	payload, _ := protocol.EncodeTelemetryBatch([]model.Telemetry{event})
	frame, err := server.handle(ctx, protocol.Frame{Operation: protocol.OpPublish, Payload: payload})
	var result protocol.PublishResponse
	_ = json.Unmarshal(frame.Payload, &result)
	if err != nil || result.Accepted != 1 {
		t.Fatalf("publish=%+v err=%v", result, err)
	}
	frame, err = server.handle(ctx, protocol.Frame{Operation: protocol.OpPublish, Payload: payload})
	_ = json.Unmarshal(frame.Payload, &result)
	if err != nil || result.Duplicates != 1 {
		t.Fatalf("dedup publish=%+v err=%v", result, err)
	}
	invalid := event
	invalid.SourceEventKey = "source-2"
	invalid.AssignmentGeneration++
	payload, _ = protocol.EncodeTelemetryBatch([]model.Telemetry{invalid})
	if _, err := server.handle(ctx, protocol.Frame{Operation: protocol.OpPublish, Payload: payload}); err == nil {
		t.Fatal("expected producer fencing")
	}

	var membership protocol.MembershipResponse
	err = control(server, protocol.OpRegisterConsumer, protocol.MembershipRequest{Group: "collectors", MemberID: "consumer"}, &membership)
	if err != nil {
		t.Fatal(err)
	}
	fetchRequest := protocol.FetchRequest{Group: "collectors", MemberID: "consumer", IncarnationID: membership.IncarnationID, Generation: membership.Generation, MaxRecords: 10}
	fetchPayload, _ := json.Marshal(fetchRequest)
	frame, err = server.handle(ctx, protocol.Frame{Operation: protocol.OpFetch, Payload: fetchPayload})
	items, decodeErr := protocol.DecodeDeliveries(frame.Payload, 10, 1<<20)
	if err == nil {
		err = decodeErr
	}
	if err != nil || len(items) != 1 || items[0].Event.SourceEventKey != event.SourceEventKey {
		t.Fatalf("fetch=%+v err=%v", items, err)
	}
	commit := protocol.CommitRequest{Group: "collectors", MemberID: "consumer", IncarnationID: membership.IncarnationID, Generation: membership.Generation, Offsets: map[uint32]uint64{items[0].Partition: items[0].Offset + 1}}
	if err := control(server, protocol.OpCommit, commit, nil); err != nil {
		t.Fatal(err)
	}
	frame, err = server.handle(ctx, protocol.Frame{Operation: protocol.OpFetch, Payload: fetchPayload})
	items, decodeErr = protocol.DecodeDeliveries(frame.Payload, 10, 1<<20)
	if err == nil {
		err = decodeErr
	}
	if err != nil || len(items) != 0 {
		t.Fatalf("fetch after commit=%+v err=%v", items, err)
	}
	if err := control(server, protocol.OpCompleteCycle, protocol.CycleRequest{DatasetID: "dataset", MemberID: "producer"}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(6 * time.Millisecond)
	var cycle2 protocol.CycleResponse
	err = control(server, protocol.OpAcquireCycle, protocol.CycleRequest{DatasetID: "dataset", MemberID: "producer"}, &cycle2)
	if err != nil || cycle2.CycleID != 2 {
		t.Fatalf("next cycle=%+v err=%v", cycle2, err)
	}
}

func control(server *Server, operation protocol.Operation, input, output any) error {
	payload, err := json.Marshal(input)
	if err != nil {
		return err
	}
	response, err := server.handle(context.Background(), protocol.Frame{Operation: operation, Payload: payload})
	if err != nil {
		return err
	}
	if output != nil {
		return json.Unmarshal(response.Payload, output)
	}
	return nil
}

func TestMembershipExpiryAndAssignments(t *testing.T) {
	base := time.Unix(100, 0)
	m := newMemberships(time.Second)
	generation, members := m.touch("g", "b", base)
	if generation != 1 || len(members) != 1 {
		t.Fatalf("generation=%d members=%v", generation, members)
	}
	generation, members = m.touch("g", "a", base)
	if generation != 2 || strings.Join(members, ",") != "a,b" {
		t.Fatalf("generation=%d members=%v", generation, members)
	}
	generation, members = m.snapshot("g", base.Add(2*time.Second))
	if generation != 3 || len(members) != 0 {
		t.Fatalf("expiry generation=%d members=%v", generation, members)
	}
	if generation, members = m.snapshot("missing", base); generation != 0 || members != nil {
		t.Fatalf("missing snapshot=%d %v", generation, members)
	}
	owned := append(assignedPartitions("a", []string{"a", "b"}, 32), assignedPartitions("b", []string{"a", "b"}, 32)...)
	if len(owned) != 32 {
		t.Fatalf("partitions not fully assigned: %v", owned)
	}
	if !containsPartition(owned, 0) || containsPartition(owned, 99) {
		t.Fatal("partition membership check failed")
	}
}

func TestCycleMembershipChangeReplaysCurrentCycle(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(100, 0).UTC()
	cycles, err := openCycles(dir, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	first, err := cycles.acquire("d", "a", 2, []string{"a", "b"}, now, "inc")
	if err != nil || first.CycleID != 1 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if err := cycles.complete("d", "a"); err != nil {
		t.Fatal(err)
	}

	// If b disappears, a's completion belongs to the old two-member assignment.
	// The same cycle must be replayed rather than incorrectly advanced.
	replayed, err := cycles.acquire("d", "a", 3, []string{"a"}, now.Add(time.Second), "inc")
	if err != nil || !replayed.Ready || replayed.CycleID != 1 {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
	if cycles.values["d"].Completed["a"] {
		t.Fatal("completion from the previous assignment was retained")
	}
	if err := cycles.complete("d", "a"); err != nil {
		t.Fatal(err)
	}
	next, err := cycles.acquire("d", "a", 3, []string{"a"}, now.Add(2*time.Second), "inc")
	if err != nil || next.CycleID != 2 {
		t.Fatalf("next=%+v err=%v", next, err)
	}
}

func TestMembershipScaleDownToOneBumpsGeneration(t *testing.T) {
	base := time.Unix(100, 0)
	m := newMemberships(time.Second)
	_, _ = m.touch("g", "a", base)
	before, _ := m.touch("g", "b", base)
	after, members := m.touch("g", "a", base.Add(2*time.Second))
	if after != before+1 || strings.Join(members, ",") != "a" {
		t.Fatalf("before=%d after=%d members=%v", before, after, members)
	}
}

func TestMembershipTouchDoesNotExpireTheCaller(t *testing.T) {
	members := newMemberships(time.Second)
	started := time.Now()
	generation, active := members.touch("producer:dataset", "streamer-a", started)
	if generation != 1 || !slices.Equal(active, []string{"streamer-a"}) {
		t.Fatalf("initial generation=%d members=%v", generation, active)
	}
	generation, active = members.touch("producer:dataset", "streamer-a", started.Add(2*time.Second))
	if generation != 1 || !slices.Equal(active, []string{"streamer-a"}) {
		t.Fatalf("renewed generation=%d members=%v", generation, active)
	}
}

func TestCyclesPersistAndFence(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(100, 0).UTC()
	cycles, err := openCycles(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	first, err := cycles.acquire("d", "a", 1, []string{"a"}, now, "inc")
	if err != nil || !first.Ready || !cycles.validate("d", 1, 1, "a", "GPU") {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if cycles.validate("d", 1, 2, "a", "GPU") {
		t.Fatal("accepted stale generation")
	}
	if err := cycles.complete("missing", "a"); err == nil {
		t.Fatal("expected missing cycle")
	}
	if err := cycles.complete("d", "a"); err != nil {
		t.Fatal(err)
	}
	waiting, err := cycles.acquire("d", "a", 1, []string{"a"}, now.Add(500*time.Millisecond), "inc")
	if err != nil || waiting.Ready || waiting.WaitMillis <= 0 {
		t.Fatalf("waiting=%+v err=%v", waiting, err)
	}
	second, err := cycles.acquire("d", "a", 1, []string{"a"}, now.Add(time.Second), "inc")
	if err != nil || !second.Ready || second.CycleID != 2 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	reopened, err := openCycles(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reopened.acquire("d", "a", 1, []string{"a"}, now.Add(2*time.Second), "new-inc")
	if err != nil || replayed.CycleID != 2 {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
}
