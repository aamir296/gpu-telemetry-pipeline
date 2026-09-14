package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

type Client struct {
	addr    string
	timeout time.Duration
	nextID  atomic.Uint64
	dial    func(context.Context) (net.Conn, error)
}

func New(addr string, timeout time.Duration) *Client { return &Client{addr: addr, timeout: timeout} }

func (c *Client) AcquireCycle(ctx context.Context, dataset, member string) (protocol.CycleResponse, error) {
	var out protocol.CycleResponse
	err := c.control(ctx, protocol.OpAcquireCycle, protocol.CycleRequest{DatasetID: dataset, MemberID: member}, &out)
	return out, err
}

func (c *Client) CompleteCycle(ctx context.Context, dataset, member string) error {
	return c.control(ctx, protocol.OpCompleteCycle, protocol.CycleRequest{DatasetID: dataset, MemberID: member, Complete: true}, nil)
}

func (c *Client) Publish(ctx context.Context, events []model.Telemetry) (protocol.PublishResponse, error) {
	var out protocol.PublishResponse
	payload, err := protocol.EncodeTelemetryBatch(events)
	if err != nil {
		return out, err
	}
	resp, err := c.do(ctx, protocol.Frame{Operation: protocol.OpPublish, RequestID: c.nextID.Add(1), Payload: payload})
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(resp.Payload, &out); err != nil {
		return out, err
	}
	return out, nil
}

func (c *Client) RegisterConsumer(ctx context.Context, group, member string) (protocol.MembershipResponse, error) {
	var out protocol.MembershipResponse
	err := c.control(ctx, protocol.OpRegisterConsumer, protocol.MembershipRequest{Group: group, MemberID: member}, &out)
	return out, err
}

func (c *Client) Fetch(ctx context.Context, req protocol.FetchRequest) ([]protocol.Delivery, error) {
	payload, _ := json.Marshal(req)
	resp, err := c.do(ctx, protocol.Frame{Operation: protocol.OpFetch, RequestID: c.nextID.Add(1), Payload: payload})
	if err != nil {
		return nil, err
	}
	return protocol.DecodeDeliveries(resp.Payload, req.MaxRecords, protocol.DefaultMaxFrame)
}

func (c *Client) Commit(ctx context.Context, req protocol.CommitRequest) error {
	return c.control(ctx, protocol.OpCommit, req, nil)
}

func (c *Client) control(ctx context.Context, op protocol.Operation, input, output any) error {
	payload, err := json.Marshal(input)
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, protocol.Frame{Operation: op, RequestID: c.nextID.Add(1), Payload: payload})
	if err != nil {
		return err
	}
	if output != nil {
		return json.Unmarshal(resp.Payload, output)
	}
	return nil
}

func (c *Client) do(ctx context.Context, request protocol.Frame) (protocol.Frame, error) {
	var conn net.Conn
	var err error
	if c.dial != nil {
		conn, err = c.dial(ctx)
	} else {
		dialer := net.Dialer{Timeout: c.timeout}
		conn, err = dialer.DialContext(ctx, "tcp", c.addr)
	}
	if err != nil {
		return protocol.Frame{}, err
	}
	defer conn.Close()
	deadline := time.Now().Add(c.timeout)
	if value, ok := ctx.Deadline(); ok && value.Before(deadline) {
		deadline = value
	}
	_ = conn.SetDeadline(deadline)
	if err := protocol.WriteFrame(conn, request); err != nil {
		return protocol.Frame{}, err
	}
	response, err := protocol.ReadFrame(conn, protocol.DefaultMaxFrame)
	if err != nil {
		return protocol.Frame{}, err
	}
	if response.RequestID != request.RequestID {
		return protocol.Frame{}, errors.New("broker response request ID mismatch")
	}
	if response.Operation == protocol.OpError {
		var apiErr protocol.ErrorResponse
		_ = json.Unmarshal(response.Payload, &apiErr)
		return protocol.Frame{}, fmt.Errorf("%s: %s", apiErr.Code, apiErr.Message)
	}
	return response, nil
}
