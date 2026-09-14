package client

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

func oneShotBroker(t *testing.T, respond func(protocol.Frame) protocol.Frame) *Client {
	t.Helper()
	clientConnection, serverConnection := net.Pipe()
	t.Cleanup(func() { _ = clientConnection.Close(); _ = serverConnection.Close() })
	go func() {
		defer serverConnection.Close()
		request, err := protocol.ReadFrame(serverConnection, protocol.DefaultMaxFrame)
		if err != nil {
			return
		}
		response := respond(request)
		_ = protocol.WriteFrame(serverConnection, response)
	}()
	client := New("pipe", time.Second)
	client.dial = func(context.Context) (net.Conn, error) { return clientConnection, nil }
	return client
}

func jsonResponse(t *testing.T, request protocol.Frame, value any) protocol.Frame {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Frame{Operation: protocol.OpResponse, RequestID: request.RequestID, Payload: payload}
}

func TestControlOperations(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
		op   protocol.Operation
	}{
		{"acquire", func(client *Client) error {
			response, err := client.AcquireCycle(context.Background(), "dataset", "producer")
			if err == nil && response.CycleID != 9 {
				t.Fatalf("cycle=%+v", response)
			}
			return err
		}, protocol.OpAcquireCycle},
		{"complete", func(client *Client) error {
			return client.CompleteCycle(context.Background(), "dataset", "producer")
		}, protocol.OpCompleteCycle},
		{"register", func(client *Client) error {
			response, err := client.RegisterConsumer(context.Background(), "group", "consumer")
			if err == nil && response.Generation != 3 {
				t.Fatalf("membership=%+v", response)
			}
			return err
		}, protocol.OpRegisterConsumer},
		{"commit", func(client *Client) error {
			return client.Commit(context.Background(), protocol.CommitRequest{Group: "group", Offsets: map[uint32]uint64{1: 2}})
		}, protocol.OpCommit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := oneShotBroker(t, func(request protocol.Frame) protocol.Frame {
				if request.Operation != test.op {
					t.Errorf("operation=%d want=%d", request.Operation, test.op)
				}
				switch test.op {
				case protocol.OpAcquireCycle:
					return jsonResponse(t, request, protocol.CycleResponse{CycleID: 9})
				case protocol.OpRegisterConsumer:
					return jsonResponse(t, request, protocol.MembershipResponse{Generation: 3})
				default:
					return jsonResponse(t, request, map[string]bool{"ok": true})
				}
			})
			if err := test.call(client); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPublishAndFetch(t *testing.T) {
	publishClient := oneShotBroker(t, func(request protocol.Frame) protocol.Frame {
		if request.Operation != protocol.OpPublish {
			t.Errorf("operation=%d", request.Operation)
		}
		events, err := protocol.DecodeTelemetryBatch(request.Payload, 10, protocol.DefaultMaxFrame)
		if err != nil || len(events) != 1 {
			t.Errorf("events=%v err=%v", events, err)
		}
		return jsonResponse(t, request, protocol.PublishResponse{Accepted: 1})
	})
	event := model.Telemetry{UUID: "GPU-1", SourceEventKey: "source-1"}
	result, err := publishClient.Publish(context.Background(), []model.Telemetry{event})
	if err != nil || result.Accepted != 1 {
		t.Fatalf("publish=%+v err=%v", result, err)
	}

	fetchClient := oneShotBroker(t, func(request protocol.Frame) protocol.Frame {
		payload, err := protocol.EncodeDeliveries([]protocol.Delivery{{Partition: 1, Offset: 2, Event: event}})
		if err != nil {
			t.Error(err)
		}
		return protocol.Frame{Operation: protocol.OpResponse, RequestID: request.RequestID, Payload: payload}
	})
	items, err := fetchClient.Fetch(context.Background(), protocol.FetchRequest{MaxRecords: 10})
	if err != nil || len(items) != 1 || items[0].Offset != 2 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}

func TestClientRejectsBrokerErrorsAndInvalidResponses(t *testing.T) {
	errorClient := oneShotBroker(t, func(request protocol.Frame) protocol.Frame {
		payload, _ := json.Marshal(protocol.ErrorResponse{Code: "stale", Message: "generation"})
		return protocol.Frame{Operation: protocol.OpError, RequestID: request.RequestID, Payload: payload}
	})
	if _, err := errorClient.AcquireCycle(context.Background(), "d", "m"); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("unexpected error %v", err)
	}

	mismatchClient := oneShotBroker(t, func(request protocol.Frame) protocol.Frame {
		return protocol.Frame{Operation: protocol.OpResponse, RequestID: request.RequestID + 1, Payload: []byte("{}")}
	})
	if err := mismatchClient.CompleteCycle(context.Background(), "d", "m"); err == nil || !strings.Contains(err.Error(), "request ID") {
		t.Fatalf("unexpected error %v", err)
	}

	invalidClient := oneShotBroker(t, func(request protocol.Frame) protocol.Frame {
		return protocol.Frame{Operation: protocol.OpResponse, RequestID: request.RequestID, Payload: []byte("not-json")}
	})
	if _, err := invalidClient.RegisterConsumer(context.Background(), "g", "m"); err == nil {
		t.Fatal("expected invalid JSON response error")
	}
}

func TestDialFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	client := New("unavailable", 100*time.Millisecond)
	client.dial = func(context.Context) (net.Conn, error) { return nil, errors.New("dial failed") }
	if _, err := client.Fetch(ctx, protocol.FetchRequest{MaxRecords: 1}); err == nil {
		t.Fatal("expected dial failure")
	}
}
