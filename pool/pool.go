// Package pool tracks a set of interchangeable upstream endpoints, probes
// their health and ranks them by smoothed latency.
package pool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/tommyshamamba/rpc-failover-proxy/breaker"
	"github.com/tommyshamamba/rpc-failover-proxy/metrics"
)

// Config describes one upstream.
type Config struct {
	Name    string
	HTTPURL string // used for HTTP proxying and health probes
	WSURL   string // optional; used for WebSocket sessions
}

// Endpoint is one upstream and its live health state.
type Endpoint struct {
	Config
	Breaker *breaker.Breaker
	order   int

	mu      sync.RWMutex
	healthy bool
	ewma    time.Duration // 0 = no sample yet
	lastErr error
}

// Healthy reports the last probe outcome.
func (e *Endpoint) Healthy() bool { e.mu.RLock(); defer e.mu.RUnlock(); return e.healthy }

// Latency returns the smoothed probe latency (0 if unknown).
func (e *Endpoint) Latency() time.Duration { e.mu.RLock(); defer e.mu.RUnlock(); return e.ewma }

// LastError returns the last probe error, if any.
func (e *Endpoint) LastError() error { e.mu.RLock(); defer e.mu.RUnlock(); return e.lastErr }

// Options tune probing and ranking.
type Options struct {
	ProbeInterval    time.Duration // default 2s
	ProbeTimeout     time.Duration // default 1s
	ProbeBody        []byte        // POSTed to HTTPURL; default a JSON-RPC web3_clientVersion call
	Alpha            float64       // EWMA weight of a new sample, default 0.3
	BreakerThreshold int           // default 3
	BreakerCooldown  time.Duration // default 5s
	Client           *http.Client
	Logger           *slog.Logger
	Metrics          *metrics.Metrics
}

func (o *Options) defaults() {
	if o.ProbeInterval <= 0 {
		o.ProbeInterval = 2 * time.Second
	}
	if o.ProbeTimeout <= 0 {
		o.ProbeTimeout = time.Second
	}
	if o.ProbeBody == nil {
		o.ProbeBody = []byte(`{"jsonrpc":"2.0","id":1,"method":"web3_clientVersion","params":[]}`)
	}
	if o.Alpha <= 0 || o.Alpha > 1 {
		o.Alpha = 0.3
	}
	if o.BreakerThreshold <= 0 {
		o.BreakerThreshold = 3
	}
	if o.BreakerCooldown <= 0 {
		o.BreakerCooldown = 5 * time.Second
	}
	if o.Client == nil {
		o.Client = &http.Client{}
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.Metrics == nil {
		o.Metrics = metrics.Nop()
	}
}

// Pool is safe for concurrent use.
type Pool struct {
	opts      Options
	endpoints []*Endpoint
}

// ErrNoUpstream is returned when no endpoint is usable.
var ErrNoUpstream = errors.New("no healthy upstream available")

// New builds a pool. Endpoints start healthy so traffic can flow before the
// first probe completes; the first probe corrects that within ProbeInterval.
func New(cfgs []Config, opts Options) (*Pool, error) {
	if len(cfgs) == 0 {
		return nil, errors.New("pool: at least one upstream is required")
	}
	opts.defaults()
	p := &Pool{opts: opts}
	seen := map[string]bool{}
	for i, c := range cfgs {
		if c.Name == "" || c.HTTPURL == "" {
			return nil, fmt.Errorf("pool: upstream %d needs a name and an HTTP URL", i)
		}
		if seen[c.Name] {
			return nil, fmt.Errorf("pool: duplicate upstream name %q", c.Name)
		}
		seen[c.Name] = true
		p.endpoints = append(p.endpoints, &Endpoint{Config: c, order: i, healthy: true,
			Breaker: breaker.New(opts.BreakerThreshold, opts.BreakerCooldown)})
		opts.Metrics.Healthy.WithLabelValues(c.Name).Set(1)
	}
	return p, nil
}

// Endpoints returns all endpoints in configuration order.
func (p *Pool) Endpoints() []*Endpoint { return append([]*Endpoint(nil), p.endpoints...) }

// Ranked returns usable endpoints, fastest first. Endpoints with no latency
// sample yet sort after measured ones; ties keep configuration order.
// If wantWS is true only endpoints with a WebSocket URL are returned.
func (p *Pool) Ranked(wantWS bool) []*Endpoint {
	out := make([]*Endpoint, 0, len(p.endpoints))
	for _, e := range p.endpoints {
		if wantWS && e.WSURL == "" {
			continue
		}
		if e.Healthy() && e.Breaker.Available() {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		li, lj := out[i].Latency(), out[j].Latency()
		if (li == 0) != (lj == 0) {
			return lj == 0
		}
		if li != lj {
			return li < lj
		}
		return out[i].order < out[j].order
	})
	return out
}

// Report records the outcome of real traffic against an endpoint.
func (p *Pool) Report(e *Endpoint, err error) {
	if err == nil {
		e.Breaker.Success()
	} else {
		e.Breaker.Failure()
	}
	p.opts.Metrics.BreakerState.WithLabelValues(e.Name).Set(float64(e.Breaker.State()))
}

// MarkDown marks an endpoint unhealthy immediately (for example when its
// WebSocket drops) without waiting for the next probe.
func (p *Pool) MarkDown(e *Endpoint, err error) {
	e.mu.Lock()
	e.healthy, e.lastErr = false, err
	e.mu.Unlock()
	p.Report(e, err)
	p.opts.Metrics.Healthy.WithLabelValues(e.Name).Set(0)
}

// ProbeOnce probes every endpoint concurrently and waits for all results.
func (p *Pool) ProbeOnce(ctx context.Context) {
	var wg sync.WaitGroup
	for _, e := range p.endpoints {
		wg.Add(1)
		go func(e *Endpoint) { defer wg.Done(); p.probe(ctx, e) }(e)
	}
	wg.Wait()
}

// Run probes on ProbeInterval until ctx is cancelled. It never returns an
// error other than ctx.Err(), so it is suitable for a supervisor.
func (p *Pool) Run(ctx context.Context) error {
	t := time.NewTicker(p.opts.ProbeInterval)
	defer t.Stop()
	for {
		p.ProbeOnce(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (p *Pool) probe(ctx context.Context, e *Endpoint) {
	ctx, cancel := context.WithTimeout(ctx, p.opts.ProbeTimeout)
	defer cancel()
	start := time.Now()
	err := p.doProbe(ctx, e)
	took := time.Since(start)
	if ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) && err != nil {
		return // shutting down; don't record a false failure
	}
	m := p.opts.Metrics
	e.mu.Lock()
	wasHealthy := e.healthy
	e.lastErr = err
	if err == nil {
		e.healthy = true
		if e.ewma == 0 {
			e.ewma = took
		} else {
			e.ewma = time.Duration(p.opts.Alpha*float64(took) + (1-p.opts.Alpha)*float64(e.ewma))
		}
	} else {
		e.healthy = false
	}
	ewma := e.ewma
	e.mu.Unlock()
	p.Report(e, err)
	m.ProbeSeconds.WithLabelValues(e.Name).Observe(took.Seconds())
	m.LatencyEWMA.WithLabelValues(e.Name).Set(ewma.Seconds())
	if err == nil {
		m.Healthy.WithLabelValues(e.Name).Set(1)
		if !wasHealthy {
			p.opts.Logger.Info("upstream recovered", "upstream", e.Name, "latency_ms", took.Milliseconds())
		}
	} else {
		m.Healthy.WithLabelValues(e.Name).Set(0)
		if wasHealthy {
			p.opts.Logger.Warn("upstream probe failed", "upstream", e.Name, "err", err)
		}
	}
}

func (p *Pool) doProbe(ctx context.Context, e *Endpoint) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.HTTPURL, bytes.NewReader(p.opts.ProbeBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.opts.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("probe status %d", resp.StatusCode)
	}
	return nil
}
