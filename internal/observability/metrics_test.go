package observability

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsAndHealthHandlers(t *testing.T) {
	metrics := New("test")
	metrics.Observe("publish", nil, 3, time.Now().Add(-time.Millisecond))
	metrics.Observe("publish", errors.New("failed"), 0, time.Now())
	handler := metrics.Handler()

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `gpu_telemetry_records_total{operation="publish",service="test"} 3`) {
		t.Fatalf("metrics response: %d %s", response.Code, response.Body.String())
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial readiness=%d", response.Code)
	}
	metrics.SetReady(true)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusOK || response.Body.String() != `{"ready":true}` {
		t.Fatalf("ready response=%d %s", response.Code, response.Body.String())
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("health response=%d", response.Code)
	}

	var nilMetrics *Metrics
	nilMetrics.Observe("ignored", nil, 0, time.Now())
	nilMetrics.SetReady(true)
}
