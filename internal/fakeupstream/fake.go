// Package fakeupstream is a controllable JSON-RPC over HTTP + WebSocket server
// used by tests, chaos tests, benchmarks and the docker-compose demo.
package fakeupstream

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Server answers any JSON-RPC method with {"upstream": Name}. On WebSocket,
// "eth_subscribe" (or any method ending in "_subscribe") starts a stream of
// notifications every Tick.
type Server struct {
	Name string
	Tick time.Duration

	failing atomic.Bool
	frozen  atomic.Bool
	delay   atomic.Int64 // ns
	subSeq  atomic.Int64

	mu    sync.Mutex
	conns map[*websocket.Conn]struct{}
}

// New returns a server emitting notifications every tick.
func New(name string, tick time.Duration) *Server {
	return &Server{Name: name, Tick: tick, conns: map[*websocket.Conn]struct{}{}}
}

// SetFailing makes HTTP return 503, refuses WebSocket upgrades and drops open sockets.
func (s *Server) SetFailing(v bool) {
	s.failing.Store(v)
	if v {
		s.DropConns()
	}
}

// Freeze stops all WebSocket traffic (no reads, writes or pongs) while
// keeping TCP connections open: the "silent hang" failure mode.
func (s *Server) Freeze(v bool) { s.frozen.Store(v) }

// SetDelay adds latency to every HTTP response.
func (s *Server) SetDelay(d time.Duration) { s.delay.Store(int64(d)) }

// DropConns closes every open WebSocket abruptly.
func (s *Server) DropConns() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		_ = c.UnderlyingConn().Close()
	}
}

type req struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func (s *Server) result(id json.RawMessage) []byte {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]string{"upstream": s.Name}})
	return b
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// ServeHTTP implements http.Handler. /admin/fail?on=1|0 and /admin/freeze?on=1|0
// toggle failure modes for demos.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/admin/fail":
		s.SetFailing(r.URL.Query().Get("on") == "1")
		_, _ = fmt.Fprintf(w, "%s failing=%v\n", s.Name, s.failing.Load())
		return
	case "/admin/freeze":
		s.Freeze(r.URL.Query().Get("on") == "1")
		_, _ = fmt.Fprintf(w, "%s frozen=%v\n", s.Name, s.frozen.Load())
		return
	}
	if s.failing.Load() {
		http.Error(w, "upstream failing", http.StatusServiceUnavailable)
		return
	}
	if websocket.IsWebSocketUpgrade(r) {
		s.serveWS(w, r)
		return
	}
	if d := time.Duration(s.delay.Load()); d > 0 {
		time.Sleep(d)
	}
	var q req
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(s.result(q.ID))
}

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		_ = c.Close()
	}()
	var wmu sync.Mutex
	write := func(b []byte) error {
		if s.frozen.Load() {
			return nil
		}
		wmu.Lock()
		defer wmu.Unlock()
		return c.WriteMessage(websocket.TextMessage, b)
	}
	done := make(chan struct{})
	defer close(done)
	c.SetPingHandler(func(data string) error {
		if s.frozen.Load() {
			return nil // a hung upstream answers nothing, not even pings
		}
		wmu.Lock()
		defer wmu.Unlock()
		return c.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})
	for {
		_, msg, err := c.ReadMessage()
		if err != nil {
			return
		}
		if s.frozen.Load() {
			continue // hung: swallow requests
		}
		var q req
		if json.Unmarshal(msg, &q) != nil {
			continue
		}
		if len(q.Method) > 10 && q.Method[len(q.Method)-10:] == "_subscribe" {
			sub := fmt.Sprintf("0x%s%d", s.Name, s.subSeq.Add(1))
			b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": sub})
			if write(b) != nil {
				return
			}
			go s.stream(sub, write, done)
			continue
		}
		if len(q.Method) > 12 && q.Method[len(q.Method)-12:] == "_unsubscribe" {
			b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": true})
			_ = write(b)
			continue
		}
		if write(s.result(q.ID)) != nil {
			return
		}
	}
}

func (s *Server) stream(sub string, write func([]byte) error, done <-chan struct{}) {
	t := time.NewTicker(s.Tick)
	defer t.Stop()
	for seq := 1; ; seq++ {
		select {
		case <-done:
			return
		case <-t.C:
		}
		b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "eth_subscription",
			"params": map[string]any{"subscription": sub, "result": map[string]any{"upstream": s.Name, "seq": seq}}})
		if write(b) != nil {
			return
		}
	}
}
