// Package proxy implements HTTP and WebSocket failover proxying over a pool.Pool.
package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/tommyshamamba/rpc-failover-proxy/metrics"
	"github.com/tommyshamamba/rpc-failover-proxy/pool"
)

// Options configure the proxy.
type Options struct {
	MaxAttempts    int           // HTTP upstreams tried per request, default 3
	RequestTimeout time.Duration // per-attempt timeout, default 10s
	MaxBodyBytes   int64         // default 10 MiB
	// WebSocket
	SubscribeMethods   []string      // default ["eth_subscribe"]
	UnsubscribeMethods []string      // default ["eth_unsubscribe"]
	PingInterval       time.Duration // default 5s; upstream declared dead after 2 missed pongs
	MaxOutage          time.Duration // how long a session waits for any upstream, default 30s
	Client             *http.Client
	Logger             *slog.Logger
	Metrics            *metrics.Metrics
}

func (o *Options) defaults() {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 3
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 10 * time.Second
	}
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = 10 << 20
	}
	if o.SubscribeMethods == nil {
		o.SubscribeMethods = []string{"eth_subscribe"}
	}
	if o.UnsubscribeMethods == nil {
		o.UnsubscribeMethods = []string{"eth_unsubscribe"}
	}
	if o.PingInterval <= 0 {
		o.PingInterval = 5 * time.Second
	}
	if o.MaxOutage <= 0 {
		o.MaxOutage = 30 * time.Second
	}
	if o.Client == nil {
		o.Client = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256, IdleConnTimeout: 90 * time.Second}}
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.Metrics == nil {
		o.Metrics = metrics.Nop()
	}
}

// Proxy serves HTTP requests and WebSocket upgrades.
type Proxy struct {
	pool *pool.Pool
	opts Options
}

// New builds a proxy.
func New(p *pool.Pool, opts Options) *Proxy {
	opts.defaults()
	return &Proxy{pool: p, opts: opts}
}

// ServeHTTP routes WebSocket upgrades to the session handler and everything
// else to the HTTP failover path.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isUpgrade(r) {
		p.serveWS(w, r)
		return
	}
	p.serveHTTP(w, r)
}

// retryable reports whether a status should move the request to another upstream.
func retryable(code int) bool {
	return code == http.StatusTooManyRequests || code == http.StatusBadGateway ||
		code == http.StatusServiceUnavailable || code == http.StatusGatewayTimeout
}

var errAllFailed = errors.New("all upstream attempts failed")

func (p *Proxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, p.opts.MaxBodyBytes))
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	attempts := 0
	var lastErr error
	for _, e := range p.pool.Ranked(false) {
		if attempts >= p.opts.MaxAttempts {
			break
		}
		if !e.Breaker.Allow() {
			continue
		}
		if attempts > 0 {
			p.opts.Metrics.Retries.Inc()
			p.opts.Metrics.Failovers.WithLabelValues("http").Inc()
		}
		attempts++
		done, err := p.attempt(w, r, e, body)
		if done {
			return
		}
		lastErr = err
		if r.Context().Err() != nil {
			return // client gave up
		}
	}
	if lastErr == nil {
		lastErr = pool.ErrNoUpstream
	}
	p.opts.Logger.Warn("request failed on every upstream", "attempts", attempts, "err", lastErr)
	writeRPCError(w, http.StatusBadGateway, fmt.Errorf("%w: %w", errAllFailed, lastErr))
}

// attempt forwards once. done=true means a response was written to the client.
func (p *Proxy) attempt(w http.ResponseWriter, r *http.Request, e *pool.Endpoint, body []byte) (bool, error) {
	ctx, cancel := context.WithTimeout(r.Context(), p.opts.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, r.Method, e.HTTPURL, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	for _, h := range []string{"Content-Type", "Accept", "Accept-Encoding"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	start := time.Now()
	resp, err := p.opts.Client.Do(req)
	p.opts.Metrics.RequestSeconds.WithLabelValues(e.Name).Observe(time.Since(start).Seconds())
	if err != nil {
		p.pool.Report(e, err)
		p.opts.Metrics.Requests.WithLabelValues(e.Name, "error").Inc()
		return false, fmt.Errorf("%s: %w", e.Name, err)
	}
	defer resp.Body.Close()
	if retryable(resp.StatusCode) {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		err := fmt.Errorf("%s: status %d", e.Name, resp.StatusCode)
		p.pool.Report(e, err)
		p.opts.Metrics.Requests.WithLabelValues(e.Name, "retryable_status").Inc()
		return false, err
	}
	// Buffer the response so a broken upstream body can still fail over
	// instead of handing the client a truncated reply.
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, p.opts.MaxBodyBytes+1))
	if err != nil || int64(len(respBody)) > p.opts.MaxBodyBytes {
		if err == nil {
			err = errors.New("response too large")
		}
		p.pool.Report(e, err)
		p.opts.Metrics.Requests.WithLabelValues(e.Name, "error").Inc()
		return false, fmt.Errorf("%s: %w", e.Name, err)
	}
	p.pool.Report(e, nil)
	p.opts.Metrics.Requests.WithLabelValues(e.Name, "ok").Inc()
	for _, h := range []string{"Content-Type", "Content-Encoding"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.Header().Set("X-Upstream", e.Name)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)
	return true, nil
}

func writeRPCError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	msg, _ := jsonString(err.Error())
	_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":%s}}`, msg)
}
