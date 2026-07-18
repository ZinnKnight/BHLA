package metrics_port

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"BHLA/shared/metrics/metrics_port"
)

var _ metrics_port.MetricsRecord = (*PrometheusRecord)(nil)

type PrometheusRecord struct {
	registry *prometheus.Registry
	reqTotal *prometheus.CounterVec
}

func NewPrometheusRecord() *PrometheusRecord {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	reqTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_request_total",
		Help: "Total grpc requests, labeled by method and status code",
	}, []string{"method", "status_codes"})
	reg.MustRegister(reqTotal)

	return &PrometheusRecord{registry: reg, reqTotal: reqTotal}
}

func (p *PrometheusRecord) IncRequest(method, statusCode string) {
	p.reqTotal.WithLabelValues(method, statusCode).Inc()
}

func (p *PrometheusRecord) Registry() http.Handler {
	return promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{})
}
