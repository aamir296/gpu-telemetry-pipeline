package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"slices"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/storage"
)

type gpuList struct {
	Items []model.GPU `json:"items"`
}

func main() {
	baseURL := getenv("API_URL", "http://127.0.0.1:18080")
	deadline := time.Now().Add(2 * time.Minute)
	client := &http.Client{Timeout: 5 * time.Second}
	var gpus gpuList
	for time.Now().Before(deadline) {
		if getJSON(client, baseURL+"/api/v1/gpus", &gpus) == nil && len(gpus.Items) > 0 {
			break
		}
		time.Sleep(time.Second)
	}
	if len(gpus.Items) == 0 {
		fail("no GPUs became queryable before timeout")
	}

	gpuID := gpus.Items[0].ID
	path := baseURL + "/api/v1/gpus/" + url.PathEscape(gpuID) + "/telemetry"
	var page storage.TelemetryPage
	cycles := map[uint64]struct{}{}
	for time.Now().Before(deadline) {
		if err := getJSON(client, path, &page); err == nil {
			cycles = map[uint64]struct{}{}
			for _, item := range page.Items {
				cycles[item.CycleID] = struct{}{}
			}
			if len(page.Items) >= 2 && len(cycles) >= 2 {
				break
			}
		}
		time.Sleep(time.Second)
	}
	if len(page.Items) < 2 || len(cycles) < 2 {
		fail("GPU %q did not retain rows from at least two cycles before timeout (rows=%d cycles=%d)", gpuID, len(page.Items), len(cycles))
	}
	event := page.Items[0]
	if event.EventID == "" || event.SourceEventKey == "" || event.ObservedAt.IsZero() || event.SourceTime.IsZero() {
		fail("response omitted required row identity or timestamps: %+v", event)
	}
	if event.MetricName == "" || event.GPUID == "" || event.Device == "" || event.ModelName == "" || event.Hostname == "" || event.LabelsRaw == "" || event.DatasetID == "" || event.CycleID == 0 || event.AssignmentGeneration == 0 || event.RowNumber == 0 || event.StreamerID == "" {
		fail("response omitted one or more persisted source/coordination fields: %+v", event)
	}
	if event.ObservedAt.Equal(event.SourceTime) {
		fail("observed_at was copied from source_timestamp instead of being assigned dynamically")
	}

	var exact storage.TelemetryPage
	if err := getJSON(client, path+"?event_id="+url.QueryEscape(event.EventID), &exact); err != nil {
		fail("exact-row query: %v", err)
	}
	if len(exact.Items) != 1 || exact.Items[0].EventID != event.EventID {
		fail("exact-row query did not return precisely %q", event.EventID)
	}

	start := event.ObservedAt.Format(time.RFC3339Nano)
	end := event.ObservedAt.Format(time.RFC3339Nano)
	var bounded storage.TelemetryPage
	if err := getJSON(client, path+"?start_time="+url.QueryEscape(start)+"&end_time="+url.QueryEscape(end), &bounded); err != nil {
		fail("inclusive time query: %v", err)
	}
	if !slices.ContainsFunc(bounded.Items, func(item model.Telemetry) bool { return item.EventID == event.EventID }) {
		fail("inclusive time bounds excluded their boundary event")
	}

	var first storage.TelemetryPage
	if err := getJSON(client, path+"?limit=1", &first); err != nil {
		fail("first page: %v", err)
	}
	if len(first.Items) != 1 || first.NextCursor == "" {
		fail("pagination did not produce one item and a cursor")
	}
	var second storage.TelemetryPage
	if err := getJSON(client, path+"?limit=1&cursor="+url.QueryEscape(first.NextCursor), &second); err != nil {
		fail("second page: %v", err)
	}
	if len(second.Items) == 0 || second.Items[0].EventID == first.Items[0].EventID {
		fail("cursor did not advance")
	}

	fmt.Printf("E2E PASS: %d GPUs, %d rows across %d cycles; previous-cycle history, exact row, dynamic timestamps, inclusive bounds, and pagination verified\n", len(gpus.Items), len(page.Items), len(cycles))
}

func getJSON(client *http.Client, endpoint string, output any) error {
	response, err := client.Get(endpoint)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned %s", endpoint, response.Status)
	}
	return json.NewDecoder(response.Body).Decode(output)
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func fail(format string, args ...any) {
	fmt.Fprintln(os.Stderr, "E2E FAIL:", fmt.Sprintf(format, args...))
	os.Exit(1)
}
