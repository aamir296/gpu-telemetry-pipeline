package streamer

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

var requiredColumns = []string{"timestamp", "metric_name", "gpu_id", "device", "modelName", "Hostname", "value", "labels_raw"}

type Decoder struct {
	reader  *csv.Reader
	header  map[string]int
	row     uint64
	dataset string
}

func NewDecoder(r io.Reader, dataset string) (*Decoder, error) {
	reader := csv.NewReader(r)
	reader.ReuseRecord = false
	columns, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV header: %w", err)
	}
	header := make(map[string]int, len(columns))
	for i, column := range columns {
		header[column] = i
	}
	for _, name := range requiredColumns {
		if _, exists := header[name]; !exists {
			return nil, fmt.Errorf("missing required CSV column %q", name)
		}
	}
	return &Decoder{reader: reader, header: header, dataset: dataset}, nil
}

func (d *Decoder) Next() (model.Telemetry, string, error) {
	record, err := d.reader.Read()
	if err != nil {
		return model.Telemetry{}, "", err
	}
	d.row++
	get := func(name string) string {
		index, ok := d.header[name]
		if !ok || index >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[index])
	}
	sourceTime, err := time.Parse(time.RFC3339Nano, get("timestamp"))
	if err != nil {
		return model.Telemetry{}, "", fmt.Errorf("row %d timestamp: %w", d.row, err)
	}
	value, err := strconv.ParseFloat(get("value"), 64)
	if err != nil {
		return model.Telemetry{}, "", fmt.Errorf("row %d value: %w", d.row, err)
	}
	t := model.Telemetry{
		SourceTime: sourceTime.UTC(), MetricName: get("metric_name"), GPUID: get("gpu_id"),
		Device: get("device"), UUID: get("uuid"), ModelName: get("modelName"), Hostname: get("Hostname"),
		Container: get("container"), Pod: get("pod"), Namespace: get("namespace"), Value: value,
		LabelsRaw: get("labels_raw"), DatasetID: d.dataset, RowNumber: d.row,
	}
	if t.GPUKey() == "" {
		return model.Telemetry{}, "", fmt.Errorf("row %d lacks uuid or hostname/gpu_id identity", d.row)
	}
	if t.MetricName == "" {
		return model.Telemetry{}, "", errors.New("metric_name cannot be empty")
	}
	h := sha256.New()
	for _, value := range record {
		_, _ = io.WriteString(h, value)
		_, _ = h.Write([]byte{0})
	}
	return t, hex.EncodeToString(h.Sum(nil)), nil
}

func SourceEventKey(dataset string, cycleID, row uint64, rowHash string) string {
	raw := fmt.Sprintf("%s\x00%d\x00%d\x00%s", dataset, cycleID, row, rowHash)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
