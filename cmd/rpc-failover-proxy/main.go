// Command rpc-failover-proxy runs the HTTP/WebSocket failover proxy.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/tommyshamamba/rpc-failover-proxy/metrics"
	"github.com/tommyshamamba/rpc-failover-proxy/pool"
	"github.com/tommyshamamba/rpc-failover-proxy/proxy"
	"github.com/tommyshamamba/rpc-failover-proxy/supervisor"
)

// parseUpstreams reads "name=http_url[|ws_url]" entries separated by commas.
func parseUpstreams(spec string) ([]pool.Config, error) {
	var out []pool.Config
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, urls, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("upstream %q: want name=http_url[|ws_url]", item)
		}
		httpURL, wsURL, _ := strings.Cut(urls, "|")
		out = append(out, pool.Config{Name: name, HTTPURL: httpURL, WSURL: wsURL})
	}
	return out, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", env("RFP_LISTEN", ":8545"), "proxy listen address")
	adminListen := flag.String("admin-listen", env("RFP_ADMIN_LISTEN", ":9090"), "metrics and health listen address")
	upstreams := flag.String("upstreams", env("RFP_UPSTREAMS", ""), "comma-separated name=http_url[|ws_url]")
	probeInterval := flag.Duration("probe-interval", 2*time.Second, "health probe interval")
	probeBody := flag.String("probe-body", env("RFP_PROBE_BODY", ""), "JSON body POSTed as health probe (default JSON-RPC web3_clientVersion)")
	ping := flag.Duration("ws-ping", 5*time.Second, "WebSocket ping interval; 2 missed pongs = dead upstream")
	shutdownGrace := flag.Duration("shutdown-grace", 15*time.Second, "graceful shutdown timeout")
	logLevel := flag.String("log-level", env("RFP_LOG_LEVEL", "info"), "debug|info|warn|error")
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(log)

	cfgs, err := parseUpstreams(*upstreams)
	if err != nil {
		return err
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)
	popts := pool.Options{ProbeInterval: *probeInterval, Logger: log, Metrics: m}
	if *probeBody != "" {
		popts.ProbeBody = []byte(*probeBody)
	}
	pl, err := pool.New(cfgs, popts)
	if err != nil {
		return err
	}
	px := proxy.New(pl, proxy.Options{PingInterval: *ping, Logger: log, Metrics: m})

	root, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// sessions is cancelled after HTTP shutdown begins so hijacked WebSocket
	// sessions (which http.Server.Shutdown does not track) close cleanly.
	sessions, endSessions := context.WithCancel(context.Background())
	defer endSessions()

	sup := &supervisor.Supervisor{Logger: log, OnRestart: func(task string, _ error) { m.TaskRestarts.WithLabelValues(task).Inc() }}
	sup.Go(sessions, "health-prober", pl.Run)

	admin := http.NewServeMux()
	admin.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	admin.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	admin.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if len(pl.Ranked(false)) == 0 {
			http.Error(w, "no healthy upstream", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})

	srv := &http.Server{Addr: *listen, Handler: px, ReadHeaderTimeout: 10 * time.Second,
		BaseContext: func(net.Listener) context.Context { return sessions }}
	adm := &http.Server{Addr: *adminListen, Handler: admin, ReadHeaderTimeout: 5 * time.Second}

	errc := make(chan error, 2)
	go func() { errc <- srv.ListenAndServe() }()
	go func() { errc <- adm.ListenAndServe() }()
	log.Info("proxy started", "listen", *listen, "admin", *adminListen, "upstreams", len(cfgs))

	select {
	case <-root.Done():
		log.Info("shutdown signal received")
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), *shutdownGrace)
	defer cancel()
	err = srv.Shutdown(ctx) // stop accepting, drain in-flight HTTP
	endSessions()           // close WebSocket sessions and stop the prober
	sup.Wait()
	_ = adm.Shutdown(ctx)
	log.Info("shutdown complete")
	return err
}
