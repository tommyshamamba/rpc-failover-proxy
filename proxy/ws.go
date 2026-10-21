package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/tommyshamamba/rpc-failover-proxy/pool"
)

func isUpgrade(r *http.Request) bool { return websocket.IsWebSocketUpgrade(r) }

var upgrader = websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096,
	CheckOrigin: func(*http.Request) bool { return true }}

func jsonString(s string) (string, error) { b, err := json.Marshal(s); return string(b), err }

// frame is the subset of JSON-RPC we inspect. Unknown fields are not
// re-encoded: frames are forwarded byte-for-byte unless a subscription ID must
// be rewritten.
type frame struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  json.RawMessage `json:"error,omitempty"`
}

type subscription struct {
	clientID   string // subscription ID the client knows (first upstream's ID)
	request    []byte // original subscribe frame, replayed on failover
	upstreamID string // ID on the current upstream
}

// session holds all per-client state. Only the session goroutine touches it,
// so no locks are needed.
type session struct {
	p      *Proxy
	client *websocket.Conn

	subs     map[string]*subscription // by clientID
	byUp     map[string]*subscription // by current upstreamID
	pending  map[string][]byte        // client subscribe requests awaiting a result, by request id
	inflight map[string]bool          // other client request ids awaiting a response
	resubs   map[string]*subscription // proxy-issued resubscribe ids
	seq      int
}

type upMsg struct {
	data []byte
	err  error
}

func (p *Proxy) serveWS(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	p.opts.Metrics.WSSessions.Inc()
	defer p.opts.Metrics.WSSessions.Dec()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	s := &session{p: p, client: c, subs: map[string]*subscription{}, byUp: map[string]*subscription{},
		pending: map[string][]byte{}, inflight: map[string]bool{}, resubs: map[string]*subscription{}}

	fromClient := make(chan []byte, 256)
	go func() { // client reader: lives for the whole session
		defer cancel()
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			select {
			case fromClient <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	err = s.run(ctx, fromClient)
	code, reason := websocket.CloseNormalClosure, "bye"
	if err != nil && ctx.Err() == nil {
		code, reason = websocket.CloseTryAgainLater, truncate(err.Error(), 100)
	}
	_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(time.Second))
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// run connects to the best upstream, pumps traffic, and fails over until the
// client leaves or no upstream comes back within MaxOutage.
func (s *session) run(ctx context.Context, fromClient <-chan []byte) error {
	first := true
	var avoid *pool.Endpoint
	for {
		up, e, err := s.dial(ctx, avoid)
		if err != nil {
			return err
		}
		if !first {
			s.p.opts.Metrics.Failovers.WithLabelValues("ws").Inc()
			s.p.opts.Logger.Info("ws failover", "upstream", e.Name, "subscriptions", len(s.subs))
		}
		first = false
		err = s.pump(ctx, up, e, fromClient)
		_ = up.Close()
		if ctx.Err() != nil {
			return nil
		}
		s.p.pool.MarkDown(e, err)
		s.p.opts.Logger.Warn("ws upstream lost", "upstream", e.Name, "err", err)
		s.failInflight()
		avoid = e
	}
}

// dial tries ranked upstreams, retrying with a short backoff until MaxOutage.
// avoid (the upstream that just failed this session) is only used when nothing
// else is available: its health probe may still pass while its WebSocket is broken.
func (s *session) dial(ctx context.Context, avoid *pool.Endpoint) (*websocket.Conn, *pool.Endpoint, error) {
	deadline := time.Now().Add(s.p.opts.MaxOutage)
	delay := 50 * time.Millisecond
	for {
		ranked := s.p.pool.Ranked(true)
		if len(ranked) > 1 && avoid != nil {
			for i, e := range ranked {
				if e == avoid {
					ranked = append(append([]*pool.Endpoint{}, ranked[:i]...), ranked[i+1:]...)
					break
				}
			}
		}
		for _, e := range ranked {
			if !e.Breaker.Allow() {
				continue
			}
			dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			c, resp, err := websocket.DefaultDialer.DialContext(dctx, e.WSURL, nil)
			cancel()
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if err != nil {
				s.p.pool.Report(e, err)
				continue
			}
			// Success is reported only once the upstream actually talks (see pump):
			// a socket that accepts but never answers must still trip the breaker.
			return c, e, nil
		}
		if time.Now().After(deadline) {
			return nil, nil, pool.ErrNoUpstream
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(delay):
		}
		if delay < time.Second {
			delay *= 2
		}
	}
}

func (s *session) pump(ctx context.Context, up *websocket.Conn, e *pool.Endpoint, fromClient <-chan []byte) error {
	ping := s.p.opts.PingInterval
	_ = up.SetReadDeadline(time.Now().Add(2 * ping))
	up.SetPongHandler(func(string) error { return up.SetReadDeadline(time.Now().Add(2 * ping)) })
	fromUp := make(chan upMsg, 256)
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for {
			_, msg, err := up.ReadMessage()
			if err == nil {
				_ = up.SetReadDeadline(time.Now().Add(2 * ping)) // any traffic proves liveness
			}
			select {
			case fromUp <- upMsg{msg, err}:
			case <-pctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	// Replay subscriptions on this upstream.
	for _, sub := range s.subs {
		s.seq++
		id := fmt.Sprintf(`"rfp-resub-%d"`, s.seq)
		s.resubs[id] = sub
		if sub.upstreamID != "" {
			delete(s.byUp, sub.upstreamID)
			sub.upstreamID = ""
		}
		if err := up.WriteMessage(websocket.TextMessage, withID(sub.request, id)); err != nil {
			return err
		}
	}
	t := time.NewTicker(ping)
	defer t.Stop()
	proven := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := up.WriteControl(websocket.PingMessage, nil, time.Now().Add(ping)); err != nil {
				return err
			}
		case m := <-fromUp:
			if m.err != nil {
				return m.err
			}
			if !proven {
				proven = true
				s.p.pool.Report(e, nil)
			}
			if err := s.fromUpstream(m.data); err != nil {
				return err
			}
		case msg := <-fromClient:
			out, err := s.fromClient(msg)
			if err != nil {
				continue // malformed client frame: forward nothing
			}
			if out != nil {
				if err := up.WriteMessage(websocket.TextMessage, out); err != nil {
					return err
				}
			}
		}
	}
}

func contains(list []string, m string) bool {
	for _, x := range list {
		if x == m {
			return true
		}
	}
	return false
}

// fromClient inspects a client frame and returns what to send upstream.
func (s *session) fromClient(msg []byte) ([]byte, error) {
	var f frame
	if err := json.Unmarshal(msg, &f); err != nil {
		return msg, nil // not JSON-RPC: pass through untouched
	}
	id := string(f.ID)
	switch {
	case contains(s.p.opts.SubscribeMethods, f.Method) && id != "":
		s.pending[id] = msg
	case contains(s.p.opts.UnsubscribeMethods, f.Method):
		var params []json.RawMessage
		if json.Unmarshal(f.Params, &params) == nil && len(params) > 0 {
			var cid string
			if json.Unmarshal(params[0], &cid) == nil {
				if sub, ok := s.subs[cid]; ok {
					delete(s.subs, cid)
					delete(s.byUp, sub.upstreamID)
					if sub.upstreamID != "" && sub.upstreamID != cid {
						params[0], _ = json.Marshal(sub.upstreamID)
						return rewriteParams(msg, params)
					}
				}
			}
		}
		if id != "" {
			s.inflight[id] = true
		}
	default:
		if id != "" {
			s.inflight[id] = true
		}
	}
	return msg, nil
}

// fromUpstream handles an upstream frame and forwards it to the client if needed.
func (s *session) fromUpstream(msg []byte) error {
	var f frame
	if err := json.Unmarshal(msg, &f); err != nil {
		return s.toClient(msg)
	}
	id := string(f.ID)
	if sub, ok := s.resubs[id]; ok { // our own replayed subscribe
		delete(s.resubs, id)
		var upID string
		if f.Error != nil || json.Unmarshal(f.Result, &upID) != nil {
			s.p.opts.Logger.Warn("resubscribe rejected", "client_sub", sub.clientID, "resp", truncate(string(msg), 200))
			return errors.New("upstream rejected resubscribe")
		}
		sub.upstreamID = upID
		s.byUp[upID] = sub
		s.p.opts.Metrics.Resubscribes.Inc()
		return nil
	}
	if req, ok := s.pending[id]; ok {
		delete(s.pending, id)
		var upID string
		if f.Error == nil && json.Unmarshal(f.Result, &upID) == nil {
			clientID := upID
			if _, taken := s.subs[clientID]; taken {
				// A new upstream reused an ID the client already holds from an
				// earlier upstream: hand out a proxy-owned ID instead.
				s.seq++
				clientID = fmt.Sprintf("0xrfp%x", s.seq)
				var m map[string]json.RawMessage
				if json.Unmarshal(msg, &m) == nil {
					m["result"], _ = json.Marshal(clientID)
					msg, _ = json.Marshal(m)
				}
			}
			sub := &subscription{clientID: clientID, request: req, upstreamID: upID}
			s.subs[clientID] = sub
			s.byUp[upID] = sub
		}
		return s.toClient(msg)
	}
	if id != "" {
		delete(s.inflight, id)
		return s.toClient(msg)
	}
	out, err := s.translate(msg)
	if err != nil || out == nil {
		return err
	}
	return s.toClient(out)
}

// translate rewrites a notification's subscription ID to the one the client
// knows. It returns nil for notifications of subscriptions the session no
// longer tracks.
func (s *session) translate(msg []byte) ([]byte, error) {
	var f frame
	if err := json.Unmarshal(msg, &f); err != nil {
		return msg, nil
	}
	if f.Method != "" && len(f.Params) > 0 { // notification
		var params map[string]json.RawMessage
		if json.Unmarshal(f.Params, &params) == nil {
			var upID string
			if json.Unmarshal(params["subscription"], &upID) == nil {
				sub, ok := s.byUp[upID]
				if !ok {
					return nil, nil // stale subscription from before an unsubscribe/failover
				}
				if sub.clientID != upID {
					params["subscription"], _ = json.Marshal(sub.clientID)
					var out map[string]json.RawMessage
					_ = json.Unmarshal(msg, &out)
					out["params"], _ = json.Marshal(params)
					msg, _ = json.Marshal(out)
				}
			}
		}
	}
	return msg, nil
}

func (s *session) toClient(msg []byte) error {
	_ = s.client.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := s.client.WriteMessage(websocket.TextMessage, msg); err != nil {
		return fmt.Errorf("client write: %w", err)
	}
	return nil
}

// failInflight answers requests lost with the old upstream so clients don't
// hang; they decide whether a retry is safe. Pending subscribes are kept and
// re-sent as if new.
func (s *session) failInflight() {
	for id := range s.inflight {
		msg := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":-32603,"message":"upstream failover; request outcome unknown"}}`, id)
		_ = s.client.WriteMessage(websocket.TextMessage, []byte(msg))
	}
	s.inflight = map[string]bool{}
	// Subscribe requests whose response was lost are answered with an error
	// too: replaying them could create a duplicate subscription the client
	// never learns about.
	for id := range s.pending {
		msg := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":-32603,"message":"upstream failover before subscription confirmed; retry"}}`, id)
		_ = s.client.WriteMessage(websocket.TextMessage, []byte(msg))
	}
	s.pending = map[string][]byte{}
	s.resubs = map[string]*subscription{}
}

func withID(msg []byte, id string) []byte {
	var m map[string]json.RawMessage
	if json.Unmarshal(msg, &m) != nil {
		return msg
	}
	m["id"] = json.RawMessage(id)
	out, _ := json.Marshal(m)
	return out
}

func rewriteParams(msg []byte, params []json.RawMessage) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(msg, &m); err != nil {
		return nil, err
	}
	b, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	m["params"] = b
	return json.Marshal(m)
}
