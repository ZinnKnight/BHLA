package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Recorder interface {
	Observe(method, code string, d time.Duration)
}

var _ Recorder = (*PrometheusRecorder)(nil)

type PrometheusRecorder struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func NewPrometheusRecorder() *PrometheusRecorder {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_server_requests_total",
		Help: "Total number of gRPC requests, by method and status code",
	}, []string{"method", "code"})

	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grpc_server_request_duration_seconds",
		Help:    "gRPC request latency, by method and status code",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "code"})

	reg.MustRegister(requests, duration)

	return &PrometheusRecorder{registry: reg, requests: requests, duration: duration}
}

func (p *PrometheusRecorder) Observe(method, code string, d time.Duration) {
	p.requests.WithLabelValues(method, code).Inc()
	p.duration.WithLabelValues(method, code).Observe(d.Seconds())
}

func (p *PrometheusRecorder) Handler() http.Handler {
	return promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{})
}
