// Package metrics holds the Prometheus collectors used by the proxy.
// Everything is registered on a caller-owned registry so tests and embedders
// never touch the global default registry.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics groups all collectors.
type Metrics struct {
	Requests       *prometheus.CounterVec   // upstream, code
	RequestSeconds *prometheus.HistogramVec // upstream
	Retries        prometheus.Counter
	ProbeSeconds   *prometheus.HistogramVec // upstream
	Healthy        *prometheus.GaugeVec     // upstream
	BreakerState   *prometheus.GaugeVec     // upstream (0 closed,1 open,2 half-open)
	LatencyEWMA    *prometheus.GaugeVec     // upstream
	Failovers      *prometheus.CounterVec   // kind (http|ws)
	WSSessions     prometheus.Gauge
	Resubscribes   prometheus.Counter
	TaskRestarts   *prometheus.CounterVec // task
}

// New creates and registers collectors on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rfp_upstream_requests_total",
			Help: "Proxied HTTP requests by upstream and outcome."}, []string{"upstream", "outcome"}),
		RequestSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "rfp_upstream_request_seconds",
			Help: "Upstream HTTP request latency.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16)}, []string{"upstream"}),
		Retries: prometheus.NewCounter(prometheus.CounterOpts{Name: "rfp_retries_total",
			Help: "HTTP attempts retried on another upstream."}),
		ProbeSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "rfp_probe_seconds",
			Help: "Health probe latency.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16)}, []string{"upstream"}),
		Healthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "rfp_upstream_healthy",
			Help: "1 if the last health probe succeeded."}, []string{"upstream"}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "rfp_breaker_state",
			Help: "Circuit breaker state: 0 closed, 1 open, 2 half-open."}, []string{"upstream"}),
		LatencyEWMA: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "rfp_upstream_latency_ewma_seconds",
			Help: "Smoothed probe latency used for ranking."}, []string{"upstream"}),
		Failovers: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rfp_failovers_total",
			Help: "Switches away from a failed upstream."}, []string{"kind"}),
		WSSessions: prometheus.NewGauge(prometheus.GaugeOpts{Name: "rfp_ws_sessions",
			Help: "Open client WebSocket sessions."}),
		Resubscribes: prometheus.NewCounter(prometheus.CounterOpts{Name: "rfp_ws_resubscribes_total",
			Help: "Subscriptions replayed on a new upstream after failover."}),
		TaskRestarts: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rfp_task_restarts_total",
			Help: "Supervised task restarts."}, []string{"task"}),
	}
	reg.MustRegister(m.Requests, m.RequestSeconds, m.Retries, m.ProbeSeconds, m.Healthy, m.BreakerState,
		m.LatencyEWMA, m.Failovers, m.WSSessions, m.Resubscribes, m.TaskRestarts)
	return m
}

// Nop returns metrics registered on a throwaway registry.
func Nop() *Metrics { return New(prometheus.NewRegistry()) }
