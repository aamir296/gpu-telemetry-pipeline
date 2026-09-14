package observability

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is a low-cardinality, per-process Prometheus registry shared by all
// service types. Operation names are fixed call sites; IDs never become labels.
type Metrics struct {
	registry   *prometheus.Registry
	operations *prometheus.CounterVec
	records    *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	ready      prometheus.Gauge
	readyState atomic.Bool
}

func New(service string) *Metrics {
	labels := prometheus.Labels{"service": service}
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		operations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gpu_telemetry_operations_total", Help: "Completed service operations by outcome.", ConstLabels: labels,
		}, []string{"operation", "status"}),
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gpu_telemetry_records_total", Help: "Telemetry records handled by operation.", ConstLabels: labels,
		}, []string{"operation"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "gpu_telemetry_operation_duration_seconds", Help: "Service operation latency.", ConstLabels: labels,
			Buckets: prometheus.DefBuckets,
		}, []string{"operation"}),
		ready: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gpu_telemetry_ready", Help: "Whether the process has completed initialization.", ConstLabels: labels,
		}),
	}
	m.registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), m.operations, m.records, m.duration, m.ready)
	return m
}

func (m *Metrics) Observe(operation string, err error, records int, started time.Time) {
	if m == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	m.operations.WithLabelValues(operation, status).Inc()
	if records > 0 {
		m.records.WithLabelValues(operation).Add(float64(records))
	}
	m.duration.WithLabelValues(operation).Observe(time.Since(started).Seconds())
}

func (m *Metrics) SetReady(ready bool) {
	if m == nil {
		return
	}
	if ready {
		m.ready.Set(1)
	} else {
		m.ready.Set(0)
	}
	m.readyState.Store(ready)
}

func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if !m.readyState.Load() {
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte(`{"ready":false}`))
			return
		}
		_, _ = writer.Write([]byte(`{"ready":true}`))
	})
	return mux
}

func Serve(ctx context.Context, address string, handler http.Handler) error {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
