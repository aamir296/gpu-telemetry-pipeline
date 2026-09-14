package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/storage"
	"github.com/jackc/pgx/v5"
)

type fakeStore struct {
	gpus      []model.GPU
	page      storage.TelemetryPage
	err       error
	lastQuery storage.TelemetryQuery
}

func (f *fakeStore) ListGPUs(context.Context) ([]model.GPU, error) { return f.gpus, f.err }
func (f *fakeStore) QueryTelemetry(_ context.Context, query storage.TelemetryQuery) (storage.TelemetryPage, error) {
	f.lastQuery = query
	return f.page, f.err
}

func apiConfig() config.API {
	return config.API{QueryTimeout: time.Second, MaxResultRows: 2, MaxResultBytes: 1 << 20, AllResultsConcurrent: 1}
}

func TestListTelemetryFiltersAndHealth(t *testing.T) {
	observed := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	store := &fakeStore{
		gpus: []model.GPU{{ID: "GPU-1"}},
		page: storage.TelemetryPage{Items: []model.Telemetry{{EventID: "event-1", UUID: "GPU-1", ObservedAt: observed}}},
	}
	handler, documented := New(store, apiConfig())
	if spec, err := documented.OpenAPI().YAML(); err != nil || !strings.Contains(string(spec), "/api/v1/gpus/{id}/telemetry") {
		t.Fatalf("OpenAPI missing route: %v", err)
	}

	response := request(t, handler, "/api/v1/gpus")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "GPU-1") {
		t.Fatalf("list: %d %s", response.Code, response.Body.String())
	}
	response = request(t, handler, "/healthz")
	if response.Code != http.StatusOK {
		t.Fatalf("health: %d", response.Code)
	}
	response = request(t, handler, "/readyz")
	if response.Code != http.StatusOK {
		t.Fatalf("ready: %d", response.Code)
	}

	start := observed.Add(-time.Hour).Format(time.RFC3339Nano)
	end := observed.Add(time.Hour).Format(time.RFC3339Nano)
	response = request(t, handler, "/api/v1/gpus/GPU-1/telemetry?start_time="+start+"&end_time="+end+"&event_id=event-1&metric_name=temp&limit=1&cursor=abc")
	if response.Code != http.StatusOK {
		t.Fatalf("telemetry: %d %s", response.Code, response.Body.String())
	}
	if store.lastQuery.GPUKey != "GPU-1" || store.lastQuery.EventID != "event-1" || store.lastQuery.MetricName != "temp" || store.lastQuery.Limit != 1 || store.lastQuery.Cursor != "abc" || store.lastQuery.Start == nil || store.lastQuery.End == nil {
		t.Fatalf("query mapping failed: %+v", store.lastQuery)
	}
	var page storage.TelemetryPage
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil || len(page.Items) != 1 {
		t.Fatalf("body=%s err=%v", response.Body.String(), err)
	}
}

func TestTelemetryValidationAndSafetyLimits(t *testing.T) {
	store := &fakeStore{}
	handler, _ := New(store, apiConfig())
	for _, path := range []string{
		"/api/v1/gpus/GPU-1/telemetry?start_time=bad",
		"/api/v1/gpus/GPU-1/telemetry?end_time=bad",
		"/api/v1/gpus/GPU-1/telemetry?start_time=2026-01-02T00:00:00Z&end_time=2026-01-01T00:00:00Z",
		"/api/v1/gpus/GPU-1/telemetry?limit=1001",
	} {
		if response := request(t, handler, path); response.Code < 400 || response.Code >= 500 {
			t.Errorf("%s returned %d", path, response.Code)
		}
	}

	store.page.Items = []model.Telemetry{{EventID: "1"}, {EventID: "2"}, {EventID: "3"}}
	if response := request(t, handler, "/api/v1/gpus/GPU-1/telemetry"); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("row ceiling returned %d: %s", response.Code, response.Body.String())
	}

	cfg := apiConfig()
	cfg.MaxResultBytes = 16
	handler, _ = New(&fakeStore{page: storage.TelemetryPage{Items: []model.Telemetry{{EventID: strings.Repeat("x", 100)}}}}, cfg)
	if response := request(t, handler, "/api/v1/gpus/GPU-1/telemetry?limit=1"); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("byte ceiling returned %d", response.Code)
	}
}

func TestStorageErrorsAndNilStore(t *testing.T) {
	for _, test := range []struct {
		err  error
		want int
	}{
		{pgx.ErrNoRows, http.StatusNotFound},
		{context.DeadlineExceeded, http.StatusGatewayTimeout},
		{errors.New("invalid cursor encoding"), http.StatusBadRequest},
		{errors.New("database unavailable"), http.StatusInternalServerError},
	} {
		handler, _ := New(&fakeStore{err: test.err}, apiConfig())
		if response := request(t, handler, "/api/v1/gpus/GPU-1/telemetry?limit=1"); response.Code != test.want {
			t.Errorf("error %v returned %d, want %d", test.err, response.Code, test.want)
		}
	}
	handler, _ := New(nil, apiConfig())
	if response := request(t, handler, "/api/v1/gpus"); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil list returned %d", response.Code)
	}
	if response := request(t, handler, "/api/v1/gpus/GPU-1/telemetry"); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil query returned %d", response.Code)
	}
	if response := request(t, handler, "/readyz"); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil ready returned %d", response.Code)
	}
}

func request(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	return recorder
}
