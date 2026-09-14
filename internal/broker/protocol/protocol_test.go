package protocol

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

func sampleTelemetry() model.Telemetry {
	return model.Telemetry{
		EventID: "event-1", SourceEventKey: "source-1", SourceTime: time.Unix(100, 200).UTC(),
		ObservedAt: time.Unix(300, 400).UTC(), MetricName: "DCGM_FI_DEV_GPU_TEMP", GPUID: "0",
		Device: "nvidia0", UUID: "GPU-1", ModelName: "A100", Hostname: "node-a",
		Container: "worker", Pod: "worker-0", Namespace: "compute", Value: 72.5,
		LabelsRaw: "gpu=0", DatasetID: "sample", CycleID: 4, AssignmentGeneration: 2,
		RowNumber: 9, StreamerID: "streamer-a",
	}
}

func TestFrameRoundTripAndValidation(t *testing.T) {
	want := Frame{Operation: OpPublish, Flags: 3, RequestID: 42, Payload: []byte("payload")}
	var buffer bytes.Buffer
	if err := WriteFrame(&buffer, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&buffer, DefaultMaxFrame)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}

	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{"magic", func(b []byte) { b[0] = 'X' }},
		{"version", func(b []byte) { b[4]++ }},
		{"checksum", func(b []byte) { b[len(b)-1] ^= 0xff }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var source bytes.Buffer
			_ = WriteFrame(&source, want)
			data := source.Bytes()
			test.mutate(data)
			if _, err := ReadFrame(bytes.NewReader(data), DefaultMaxFrame); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	var oversized bytes.Buffer
	_ = WriteFrame(&oversized, want)
	if _, err := ReadFrame(&oversized, 2); err == nil {
		t.Fatal("expected max frame error")
	}
	if err := WriteFrame(&bytes.Buffer{}, Frame{Payload: make([]byte, DefaultMaxFrame+1)}); err == nil {
		t.Fatal("expected oversized write error")
	}
}

func TestTelemetryAndBatchRoundTrips(t *testing.T) {
	want := sampleTelemetry()
	encoded, err := EncodeTelemetry(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeTelemetry(encoded, DefaultMaxFrame)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch\ngot: %#v\nwant: %#v", got, want)
	}
	batch, err := EncodeTelemetryBatch([]model.Telemetry{want, want})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeTelemetryBatch(batch, 2, DefaultMaxFrame)
	if err != nil || len(decoded) != 2 {
		t.Fatalf("batch decode: len=%d err=%v", len(decoded), err)
	}
	if _, err := DecodeTelemetryBatch(batch, 1, DefaultMaxFrame); err == nil {
		t.Fatal("expected record-count rejection")
	}
	if _, err := DecodeTelemetry(append(encoded, 1), DefaultMaxFrame); err == nil {
		t.Fatal("expected trailing byte rejection")
	}
	badVersion := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint16(badVersion, 99)
	if _, err := DecodeTelemetry(badVersion, DefaultMaxFrame); err == nil {
		t.Fatal("expected schema rejection")
	}
	if _, err := DecodeTelemetry(encoded[:5], DefaultMaxFrame); err == nil {
		t.Fatal("expected truncation rejection")
	}
	if _, err := DecodeTelemetry(encoded, 2); err == nil {
		t.Fatal("expected string size rejection")
	}
}

func TestDeliveryRoundTripAndValidation(t *testing.T) {
	want := []Delivery{{Partition: 2, Offset: 8, Event: sampleTelemetry()}}
	encoded, err := EncodeDeliveries(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDeliveries(encoded, 1, DefaultMaxFrame)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
	if _, err := DecodeDeliveries(encoded, 0, DefaultMaxFrame); err == nil {
		t.Fatal("expected delivery-count rejection")
	}
	if _, err := DecodeDeliveries(append(encoded, 0), 1, DefaultMaxFrame); err == nil {
		t.Fatal("expected trailing-byte rejection")
	}
	if _, err := DecodeDeliveries(encoded[:6], 1, DefaultMaxFrame); err == nil {
		t.Fatal("expected truncation rejection")
	}
}

func TestStringLengthValidation(t *testing.T) {
	var raw bytes.Buffer
	binary.Write(&raw, binary.BigEndian, uint32(10))
	raw.WriteString("short")
	if _, err := readString(bytes.NewReader(raw.Bytes()), 100); err == nil || !strings.Contains(err.Error(), "length") {
		t.Fatalf("expected invalid length, got %v", err)
	}
}
