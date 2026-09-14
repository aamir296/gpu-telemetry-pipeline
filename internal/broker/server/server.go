package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/store"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/observability"
)

type Server struct {
	addr          string
	store         *store.Store
	members       *memberships
	cycles        *cycles
	incarnationID string
	maxBatch      int
	maxRecord     int
	listener      net.Listener
	wg            sync.WaitGroup
	metrics       *observability.Metrics
}

func New(addr, dataDir string, partitions uint32, segmentBytes, maxPendingBytes int64, maxBatch, maxRecord int, memberTTL, cycleInterval time.Duration) (*Server, error) {
	st, err := store.OpenWithLimit(dataDir, partitions, segmentBytes, maxRecord, maxPendingBytes)
	if err != nil {
		return nil, err
	}
	cy, err := openCycles(dataDir, cycleInterval)
	if err != nil {
		st.Close()
		return nil, err
	}
	return &Server{addr: addr, store: st, members: newMemberships(memberTTL), cycles: cy, incarnationID: randomID(), maxBatch: maxBatch, maxRecord: maxRecord, metrics: observability.New("queue")}, nil
}

func (s *Server) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	s.metrics.SetReady(true)
	defer s.metrics.SetReady(false)
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(ctx, conn)
		}()
	}
	s.wg.Wait()
	return s.store.Close()
}

func (s *Server) MetricsHandler() http.Handler { return s.metrics.Handler() }

func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	for {
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
		frame, err := protocol.ReadFrame(conn, protocol.DefaultMaxFrame)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Warn("broker connection closed", "error", err)
			}
			return
		}
		started := time.Now()
		response, err := s.handle(ctx, frame)
		s.metrics.Observe(operationName(frame.Operation), err, 0, started)
		if err != nil {
			payload, _ := json.Marshal(protocol.ErrorResponse{Code: "broker_error", Message: err.Error()})
			response = protocol.Frame{Operation: protocol.OpError, RequestID: frame.RequestID, Payload: payload}
		}
		if err := protocol.WriteFrame(conn, response); err != nil {
			return
		}
	}
}

func operationName(operation protocol.Operation) string {
	switch operation {
	case protocol.OpAcquireCycle:
		return "acquire_cycle"
	case protocol.OpCompleteCycle:
		return "complete_cycle"
	case protocol.OpPublish:
		return "publish"
	case protocol.OpRegisterConsumer:
		return "register_consumer"
	case protocol.OpHeartbeat:
		return "heartbeat"
	case protocol.OpFetch:
		return "fetch"
	case protocol.OpCommit:
		return "commit"
	case protocol.OpStats:
		return "stats"
	default:
		return "unknown"
	}
}

func (s *Server) handle(ctx context.Context, f protocol.Frame) (protocol.Frame, error) {
	respondJSON := func(value any) (protocol.Frame, error) {
		data, err := json.Marshal(value)
		return protocol.Frame{Operation: protocol.OpResponse, RequestID: f.RequestID, Payload: data}, err
	}
	now := time.Now().UTC()
	switch f.Operation {
	case protocol.OpRegisterConsumer, protocol.OpHeartbeat:
		var req protocol.MembershipRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return protocol.Frame{}, err
		}
		generation, members := s.members.touch("consumer:"+req.Group, req.MemberID, now)
		return respondJSON(protocol.MembershipResponse{IncarnationID: s.incarnationID, Generation: generation, Members: members, Partitions: assignedPartitions(req.MemberID, members, s.store.PartitionCount())})
	case protocol.OpAcquireCycle:
		var req protocol.CycleRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return protocol.Frame{}, err
		}
		generation, members := s.members.touch("producer:"+req.DatasetID, req.MemberID, now)
		cycle, err := s.cycles.acquire(req.DatasetID, req.MemberID, generation, members, now, s.incarnationID)
		if err != nil {
			return protocol.Frame{}, err
		}
		return respondJSON(cycle)
	case protocol.OpCompleteCycle:
		var req protocol.CycleRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return protocol.Frame{}, err
		}
		if err := s.cycles.complete(req.DatasetID, req.MemberID); err != nil {
			return protocol.Frame{}, err
		}
		return respondJSON(map[string]bool{"ok": true})
	case protocol.OpPublish:
		events, err := protocol.DecodeTelemetryBatch(f.Payload, s.maxBatch, s.maxRecord)
		if err != nil {
			return protocol.Frame{}, err
		}
		for _, event := range events {
			if !s.cycles.validate(event.DatasetID, event.CycleID, event.AssignmentGeneration, event.StreamerID, event.GPUKey()) {
				return protocol.Frame{}, fmt.Errorf("stale or invalid producer assignment for %s", event.GPUKey())
			}
		}
		result, err := s.store.Append(ctx, events)
		if err != nil {
			return protocol.Frame{}, err
		}
		return respondJSON(protocol.PublishResponse{Accepted: result.Accepted, Duplicates: result.Duplicates})
	case protocol.OpFetch:
		var req protocol.FetchRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return protocol.Frame{}, err
		}
		generation, members := s.members.touch("consumer:"+req.Group, req.MemberID, now)
		if req.IncarnationID != s.incarnationID || req.Generation != generation {
			return protocol.Frame{}, errors.New("stale consumer generation")
		}
		partitions := assignedPartitions(req.MemberID, members, s.store.PartitionCount())
		var result []protocol.Delivery
		for _, partitionID := range partitions {
			remaining := req.MaxRecords - len(result)
			if remaining <= 0 {
				break
			}
			items, err := s.store.Fetch(req.Group, partitionID, remaining)
			if err != nil {
				return protocol.Frame{}, err
			}
			result = append(result, items...)
		}
		payload, err := protocol.EncodeDeliveries(result)
		return protocol.Frame{Operation: protocol.OpResponse, RequestID: f.RequestID, Payload: payload}, err
	case protocol.OpCommit:
		var req protocol.CommitRequest
		if err := json.Unmarshal(f.Payload, &req); err != nil {
			return protocol.Frame{}, err
		}
		generation, members := s.members.touch("consumer:"+req.Group, req.MemberID, now)
		if req.IncarnationID != s.incarnationID || req.Generation != generation {
			return protocol.Frame{}, errors.New("stale consumer generation")
		}
		owned := assignedPartitions(req.MemberID, members, s.store.PartitionCount())
		for partitionID := range req.Offsets {
			if !containsPartition(owned, partitionID) {
				return protocol.Frame{}, errors.New("commit for unowned partition")
			}
		}
		if err := s.store.Commit(req.Group, req.Offsets); err != nil {
			return protocol.Frame{}, err
		}
		return respondJSON(map[string]bool{"ok": true})
	case protocol.OpStats:
		return respondJSON(map[string]any{"incarnation_id": s.incarnationID, "high_watermarks": s.store.HighWatermarks()})
	default:
		return protocol.Frame{}, fmt.Errorf("unknown operation %d", f.Operation)
	}
}

func containsPartition(values []uint32, target uint32) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
