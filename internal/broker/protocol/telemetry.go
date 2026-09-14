package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

const telemetrySchemaVersion uint16 = 1

func EncodeTelemetry(t model.Telemetry) ([]byte, error) {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, telemetrySchemaVersion)
	_ = binary.Write(&b, binary.BigEndian, t.SourceTime.UnixNano())
	_ = binary.Write(&b, binary.BigEndian, t.ObservedAt.UnixNano())
	_ = binary.Write(&b, binary.BigEndian, t.CycleID)
	_ = binary.Write(&b, binary.BigEndian, t.AssignmentGeneration)
	_ = binary.Write(&b, binary.BigEndian, t.RowNumber)
	_ = binary.Write(&b, binary.BigEndian, math.Float64bits(t.Value))
	for _, s := range []string{
		t.EventID, t.SourceEventKey, t.MetricName, t.GPUID, t.Device, t.UUID,
		t.ModelName, t.Hostname, t.Container, t.Pod, t.Namespace, t.LabelsRaw,
		t.DatasetID, t.StreamerID,
	} {
		if err := writeString(&b, s); err != nil {
			return nil, err
		}
	}
	return b.Bytes(), nil
}

func DecodeTelemetry(data []byte, maxString int) (model.Telemetry, error) {
	if maxString <= 0 {
		maxString = 1 << 20
	}
	r := bytes.NewReader(data)
	var version uint16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return model.Telemetry{}, err
	}
	if version != telemetrySchemaVersion {
		return model.Telemetry{}, fmt.Errorf("unsupported telemetry schema %d", version)
	}
	var sourceNS, observedNS int64
	var t model.Telemetry
	var valueBits uint64
	for _, v := range []any{&sourceNS, &observedNS, &t.CycleID, &t.AssignmentGeneration, &t.RowNumber, &valueBits} {
		if err := binary.Read(r, binary.BigEndian, v); err != nil {
			return model.Telemetry{}, err
		}
	}
	t.SourceTime = time.Unix(0, sourceNS).UTC()
	t.ObservedAt = time.Unix(0, observedNS).UTC()
	t.Value = math.Float64frombits(valueBits)
	fields := []*string{
		&t.EventID, &t.SourceEventKey, &t.MetricName, &t.GPUID, &t.Device, &t.UUID,
		&t.ModelName, &t.Hostname, &t.Container, &t.Pod, &t.Namespace, &t.LabelsRaw,
		&t.DatasetID, &t.StreamerID,
	}
	for _, dst := range fields {
		s, err := readString(r, maxString)
		if err != nil {
			return model.Telemetry{}, err
		}
		*dst = s
	}
	if r.Len() != 0 {
		return model.Telemetry{}, errors.New("unexpected bytes after telemetry record")
	}
	return t, nil
}

func EncodeTelemetryBatch(events []model.Telemetry) ([]byte, error) {
	var b bytes.Buffer
	if len(events) > math.MaxUint32 {
		return nil, errors.New("too many records")
	}
	_ = binary.Write(&b, binary.BigEndian, uint32(len(events)))
	for _, event := range events {
		encoded, err := EncodeTelemetry(event)
		if err != nil {
			return nil, err
		}
		_ = binary.Write(&b, binary.BigEndian, uint32(len(encoded)))
		_, _ = b.Write(encoded)
	}
	return b.Bytes(), nil
}

func DecodeTelemetryBatch(data []byte, maxRecords, maxRecordBytes int) ([]model.Telemetry, error) {
	r := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	if int(count) > maxRecords {
		return nil, fmt.Errorf("batch contains %d records; maximum is %d", count, maxRecords)
	}
	result := make([]model.Telemetry, 0, count)
	for range count {
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		if int(n) > maxRecordBytes || int(n) > r.Len() {
			return nil, errors.New("invalid telemetry record length")
		}
		buf := make([]byte, n)
		_, _ = r.Read(buf)
		t, err := DecodeTelemetry(buf, maxRecordBytes)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	if r.Len() != 0 {
		return nil, errors.New("unexpected bytes after telemetry batch")
	}
	return result, nil
}

type Delivery struct {
	Partition uint32
	Offset    uint64
	Event     model.Telemetry
}

func EncodeDeliveries(items []Delivery) ([]byte, error) {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, uint32(len(items)))
	for _, item := range items {
		data, err := EncodeTelemetry(item.Event)
		if err != nil {
			return nil, err
		}
		_ = binary.Write(&b, binary.BigEndian, item.Partition)
		_ = binary.Write(&b, binary.BigEndian, item.Offset)
		_ = binary.Write(&b, binary.BigEndian, uint32(len(data)))
		_, _ = b.Write(data)
	}
	return b.Bytes(), nil
}

func DecodeDeliveries(data []byte, maxRecords, maxRecordBytes int) ([]Delivery, error) {
	r := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	if int(count) > maxRecords {
		return nil, errors.New("too many deliveries")
	}
	items := make([]Delivery, 0, count)
	for range count {
		var item Delivery
		var n uint32
		for _, v := range []any{&item.Partition, &item.Offset, &n} {
			if err := binary.Read(r, binary.BigEndian, v); err != nil {
				return nil, err
			}
		}
		if int(n) > maxRecordBytes || int(n) > r.Len() {
			return nil, errors.New("invalid delivery length")
		}
		buf := make([]byte, n)
		_, _ = r.Read(buf)
		event, err := DecodeTelemetry(buf, maxRecordBytes)
		if err != nil {
			return nil, err
		}
		item.Event = event
		items = append(items, item)
	}
	if r.Len() != 0 {
		return nil, errors.New("unexpected bytes after deliveries")
	}
	return items, nil
}

func writeString(b *bytes.Buffer, value string) error {
	if len(value) > math.MaxUint32 {
		return errors.New("string too large")
	}
	_ = binary.Write(b, binary.BigEndian, uint32(len(value)))
	_, err := b.WriteString(value)
	return err
}

func readString(r *bytes.Reader, max int) (string, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return "", err
	}
	if int(n) > max || int(n) > r.Len() {
		return "", errors.New("invalid string length")
	}
	b := make([]byte, n)
	_, _ = r.Read(b)
	return string(b), nil
}
