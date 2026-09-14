package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/observability"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/storage"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/jackc/pgx/v5"
)

// Store is the read-only storage contract used by the HTTP service.
type Store interface {
	ListGPUs(context.Context) ([]model.GPU, error)
	QueryTelemetry(context.Context, storage.TelemetryQuery) (storage.TelemetryPage, error)
}

type Service struct {
	store      Store
	config     config.API
	allResults chan struct{}
}

// New builds the complete HTTP handler and its generated OpenAPI model.
func New(store Store, cfg config.API) (http.Handler, huma.API) {
	mux := http.NewServeMux()
	apiConfig := huma.DefaultConfig("GPU Telemetry API", "1.0.0")
	apiConfig.Info.Description = "Queries all persisted GPU telemetry, including original source timestamps and dynamically assigned observation timestamps."
	docAPI := humago.New(mux, apiConfig)
	service := &Service{store: store, config: cfg, allResults: make(chan struct{}, cfg.AllResultsConcurrent)}
	register(docAPI, service)
	metrics := observability.New("api")
	metrics.SetReady(true)
	mux.Handle("/metrics", metrics.Handler())
	return observeHTTP(mux, metrics), docAPI
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (writer *statusWriter) WriteHeader(status int) {
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func observeHTTP(next http.Handler, metrics *observability.Metrics) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		tracked := &statusWriter{ResponseWriter: writer, status: http.StatusOK}
		next.ServeHTTP(tracked, request)
		var err error
		if tracked.status >= http.StatusBadRequest {
			err = fmt.Errorf("HTTP %d", tracked.status)
		}
		metrics.Observe(apiOperation(request.URL.Path), err, 0, started)
	})
}

func apiOperation(path string) string {
	switch {
	case path == "/api/v1/gpus":
		return "list_gpus"
	case strings.HasPrefix(path, "/api/v1/gpus/") && strings.HasSuffix(path, "/telemetry"):
		return "query_telemetry"
	case path == "/metrics":
		return "metrics"
	case path == "/healthz" || path == "/readyz":
		return "health"
	default:
		return "unknown"
	}
}

type listGPUsOutput struct {
	Body struct {
		Items []model.GPU `json:"items"`
	}
}

type telemetryInput struct {
	ID         string `path:"id" doc:"Stable GPU key (UUID when available, otherwise hostname/GPU ID)"`
	StartTime  string `query:"start_time" doc:"Inclusive RFC3339 observation-time lower bound"`
	EndTime    string `query:"end_time" doc:"Inclusive RFC3339 observation-time upper bound"`
	EventID    string `query:"event_id" doc:"Return one exact event by its event ID"`
	MetricName string `query:"metric_name" doc:"Filter by metric name"`
	Limit      int    `query:"limit" minimum:"0" maximum:"1000" doc:"Page size; omit to return every match up to the server safety ceiling"`
	Cursor     string `query:"cursor" doc:"Opaque snapshot cursor returned by an earlier page"`
}

type telemetryOutput struct {
	Body storage.TelemetryPage
}

type healthOutput struct {
	Body struct {
		Status string `json:"status"`
	}
}

func register(api huma.API, service *Service) {
	huma.Register(api, huma.Operation{
		OperationID: "list-gpus",
		Method:      http.MethodGet,
		Path:        "/api/v1/gpus",
		Summary:     "List known GPUs",
		Tags:        []string{"Telemetry"},
		Errors:      []int{http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
	}, service.listGPUs)

	huma.Register(api, huma.Operation{
		OperationID: "get-gpu-telemetry",
		Method:      http.MethodGet,
		Path:        "/api/v1/gpus/{id}/telemetry",
		Summary:     "Query a GPU's telemetry",
		Description: "Returns all matched fields from every persisted cycle. Time bounds are inclusive and apply to the dynamically assigned observed_at timestamp. Use event_id for an exact row.",
		Tags:        []string{"Telemetry"},
		Errors:      []int{http.StatusBadRequest, http.StatusNotFound, http.StatusUnprocessableEntity, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
	}, service.telemetry)

	huma.Register(api, huma.Operation{OperationID: "liveness", Method: http.MethodGet, Path: "/healthz", Tags: []string{"Operations"}}, service.health)
	huma.Register(api, huma.Operation{OperationID: "readiness", Method: http.MethodGet, Path: "/readyz", Tags: []string{"Operations"}}, service.ready)
}

func (s *Service) listGPUs(ctx context.Context, _ *struct{}) (*listGPUsOutput, error) {
	if s.store == nil {
		return nil, huma.Error503ServiceUnavailable("storage is not configured")
	}
	queryCtx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	items, err := s.store.ListGPUs(queryCtx)
	if err != nil {
		return nil, storageError(err)
	}
	if items == nil {
		items = []model.GPU{}
	}
	response := &listGPUsOutput{}
	response.Body.Items = items
	return response, nil
}

func (s *Service) telemetry(ctx context.Context, input *telemetryInput) (*telemetryOutput, error) {
	if s.store == nil {
		return nil, huma.Error503ServiceUnavailable("storage is not configured")
	}
	start, err := parseTime("start_time", input.StartTime)
	if err != nil {
		return nil, err
	}
	end, err := parseTime("end_time", input.EndTime)
	if err != nil {
		return nil, err
	}
	if start != nil && end != nil && start.After(*end) {
		return nil, huma.Error400BadRequest("start_time must not be after end_time")
	}

	query := storage.TelemetryQuery{
		GPUKey:     input.ID,
		Start:      start,
		End:        end,
		EventID:    input.EventID,
		MetricName: input.MetricName,
		Limit:      input.Limit,
		Cursor:     input.Cursor,
	}
	unbounded := input.Limit == 0
	if unbounded {
		select {
		case s.allResults <- struct{}{}:
			defer func() { <-s.allResults }()
		default:
			return nil, huma.Error429TooManyRequests("too many concurrent unpaginated queries; retry with a limit")
		}
		query.Limit = s.config.MaxResultRows + 1
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.config.QueryTimeout)
	defer cancel()
	page, err := s.store.QueryTelemetry(queryCtx, query)
	if err != nil {
		return nil, storageError(err)
	}
	if unbounded {
		if len(page.Items) > s.config.MaxResultRows {
			return nil, huma.NewError(http.StatusUnprocessableEntity, fmt.Sprintf("result_set_too_large: result exceeds the %d-row safety ceiling; use limit and cursor", s.config.MaxResultRows))
		}
		page.NextCursor = ""
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return nil, huma.Error500InternalServerError("could not encode response")
	}
	if len(encoded) > s.config.MaxResultBytes {
		return nil, huma.NewError(http.StatusUnprocessableEntity, fmt.Sprintf("result_set_too_large: result exceeds the %d-byte safety ceiling; narrow the query or use pagination", s.config.MaxResultBytes))
	}
	return &telemetryOutput{Body: page}, nil
}

func (s *Service) health(context.Context, *struct{}) (*healthOutput, error) {
	response := &healthOutput{}
	response.Body.Status = "ok"
	return response, nil
}

func (s *Service) ready(ctx context.Context, _ *struct{}) (*healthOutput, error) {
	if s.store == nil {
		return nil, huma.Error503ServiceUnavailable("storage is not ready")
	}
	queryCtx, cancel := context.WithTimeout(ctx, min(s.config.QueryTimeout, time.Second))
	defer cancel()
	if _, err := s.store.ListGPUs(queryCtx); err != nil {
		return nil, huma.Error503ServiceUnavailable("storage is not ready")
	}
	response := &healthOutput{}
	response.Body.Status = "ready"
	return response, nil
}

func parseTime(field, value string) (*time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, huma.Error400BadRequest(field + " must be an RFC3339 timestamp")
	}
	parsed = parsed.UTC()
	return &parsed, nil
}

func storageError(err error) error {
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return huma.Error404NotFound("GPU not found")
	case errors.Is(err, context.DeadlineExceeded):
		return huma.Error504GatewayTimeout("storage query timed out")
	case strings.Contains(err.Error(), "cursor"):
		return huma.Error400BadRequest(err.Error())
	default:
		return huma.Error500InternalServerError("storage query failed")
	}
}
