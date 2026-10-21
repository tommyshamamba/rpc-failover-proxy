// Command fake-upstream runs a controllable JSON-RPC HTTP/WebSocket upstream for demos.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/tommyshamamba/rpc-failover-proxy/internal/fakeupstream"
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	name := flag.String("name", "fake", "upstream name reported in responses")
	tick := flag.Duration("tick", time.Second, "subscription notification interval")
	delay := flag.Duration("delay", 0, "artificial HTTP latency")
	flag.Parse()
	s := fakeupstream.New(*name, *tick)
	s.SetDelay(*delay)
	slog.Info("fake upstream listening", "addr", *addr, "name", *name)
	srv := &http.Server{Addr: *addr, Handler: s, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
