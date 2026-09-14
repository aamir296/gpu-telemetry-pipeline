package model

import "time"

// Telemetry is one immutable GPU telemetry observation.
type Telemetry struct {
	EventID              string    `json:"event_id"`
	SourceEventKey       string    `json:"source_event_key"`
	SourceTime           time.Time `json:"source_timestamp"`
	ObservedAt           time.Time `json:"observed_at"`
	IngestedAt           time.Time `json:"ingested_at,omitempty"`
	MetricName           string    `json:"metric_name"`
	GPUID                string    `json:"gpu_id"`
	Device               string    `json:"device"`
	UUID                 string    `json:"uuid,omitempty"`
	ModelName            string    `json:"model_name"`
	Hostname             string    `json:"hostname"`
	Container            string    `json:"container,omitempty"`
	Pod                  string    `json:"pod,omitempty"`
	Namespace            string    `json:"namespace,omitempty"`
	Value                float64   `json:"value"`
	LabelsRaw            string    `json:"labels_raw"`
	DatasetID            string    `json:"dataset_id"`
	CycleID              uint64    `json:"cycle_id"`
	AssignmentGeneration uint64    `json:"assignment_generation"`
	RowNumber            uint64    `json:"row_number"`
	StreamerID           string    `json:"streamer_id"`
	QueuePartition       uint32    `json:"queue_partition"`
	QueueOffset          uint64    `json:"queue_offset"`
}

// GPUKey returns the globally useful identity for a row.
func (t Telemetry) GPUKey() string {
	if t.UUID != "" {
		return t.UUID
	}
	if t.Hostname != "" && t.GPUID != "" {
		return t.Hostname + "/" + t.GPUID
	}
	return ""
}

// GPU describes a GPU for which telemetry is available.
type GPU struct {
	ID        string    `json:"id"`
	UUID      string    `json:"uuid,omitempty"`
	Hostname  string    `json:"hostname"`
	GPUID     string    `json:"gpu_id"`
	Device    string    `json:"device"`
	ModelName string    `json:"model_name"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
}
