package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/tommyshamamba/rpc-failover-proxy/internal/fakeupstream"
	"github.com/tommyshamamba/rpc-failover-proxy/pool"
)

type fixture struct {
	fakes   []*fakeupstream.Server
	servers []*httptest.Server
	pool    *pool.Pool
	proxy   *httptest.Server
	cancel  context.CancelFunc
}

func newFixture(t testing.TB, n int, tick time.Duration, popts pool.Options, opts Options) *fixture {
	t.Helper()
	f := &fixture{}
	var cfgs []pool.Config
	for i := 0; i < n; i++ {
		name := string(rune('a' + i))
		fk := fakeupstream.New(name, tick)
		srv := httptest.NewServer(fk)
		f.fakes = append(f.fakes, fk)
		f.servers = append(f.servers, srv)
		cfgs = append(cfgs, pool.Config{Name: name, HTTPURL: srv.URL, WSURL: "ws" + strings.TrimPrefix(srv.URL, "http")})
	}
	p, err := pool.New(cfgs, popts)
	if err != nil {
		t.Fatal(err)
	}
	f.pool = p
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	go func() { _ = p.Run(ctx) }()
	f.proxy = httptest.NewServer(New(p, opts))
	t.Cleanup(func() {
		cancel()
		f.proxy.Close()
		for i, s := range f.servers {
			f.fakes[i].Freeze(false)
			f.fakes[i].DropConns()
			s.Close()
		}
	})
	return f
}

func post(t testing.TB, url string) (string, int) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		ID     int               `json:"id"`
		Result map[string]string `json:"result"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return body.Result["upstream"], resp.StatusCode
}

func TestHTTPFailoverOnStatus(t *testing.T) {
	f := newFixture(t, 2, time.Second, pool.Options{ProbeInterval: time.Hour}, Options{})
	up, code := post(t, f.proxy.URL)
	if code != 200 || (up != "a" && up != "b") {
		t.Fatalf("got %s/%d", up, code)
	}
	f.fakes[up[0]-'a'].SetFailing(true)
	other := map[string]string{"a": "b", "b": "a"}[up]
	for i := 0; i < 5; i++ {
		if got, code := post(t, f.proxy.URL); got != other || code != 200 {
			t.Fatalf("want failover to %s, got %s/%d", other, got, code)
		}
	}
}

func TestHTTPFailoverOnConnectionRefused(t *testing.T) {
	f := newFixture(t, 2, time.Second, pool.Options{ProbeInterval: time.Hour}, Options{})
	up, _ := post(t, f.proxy.URL)
	idx := int(up[0] - 'a')
	f.servers[idx].CloseClientConnections()
	f.servers[idx].Listener.Close()
	other := map[string]string{"a": "b", "b": "a"}[up]
	if got, code := post(t, f.proxy.URL); got != other || code != 200 {
		t.Fatalf("want %s/200, got %s/%d", other, got, code)
	}
}

func TestHTTPAllDown(t *testing.T) {
	f := newFixture(t, 2, time.Second, pool.Options{ProbeInterval: time.Hour}, Options{})
	f.fakes[0].SetFailing(true)
	f.fakes[1].SetFailing(true)
	if _, code := post(t, f.proxy.URL); code != http.StatusBadGateway {
		t.Fatalf("want 502, got %d", code)
	}
}

type note struct {
	Method string `json:"method"`
	ID     any    `json:"id"`
	Result any    `json:"result"`
	Params struct {
		Subscription string `json:"subscription"`
		Result       struct {
			Upstream string `json:"upstream"`
		} `json:"result"`
	} `json:"params"`
}

func dialSub(t testing.TB, f *fixture) (*websocket.Conn, string) {
	t.Helper()
	c, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(f.proxy.URL, "http"), nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`)); err != nil {
		t.Fatal(err)
	}
	for {
		var n note
		if err := c.ReadJSON(&n); err != nil {
			t.Fatal(err)
		}
		if n.Method == "" {
			return c, n.Result.(string)
		}
	}
}

// next reads until a notification arrives and returns its upstream.
func next(t testing.TB, c *websocket.Conn, subID string) string {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		var n note
		if err := c.ReadJSON(&n); err != nil {
			t.Fatal(err)
		}
		if n.Method == "eth_subscription" {
			if n.Params.Subscription != subID {
				t.Fatalf("subscription id changed: %q != %q", n.Params.Subscription, subID)
			}
			return n.Params.Result.Upstream
		}
	}
}

func TestWSSubscribeAndUnsubscribe(t *testing.T) {
	f := newFixture(t, 1, 10*time.Millisecond, pool.Options{ProbeInterval: time.Hour}, Options{})
	c, sub := dialSub(t, f)
	if up := next(t, c, sub); up != "a" {
		t.Fatalf("got %s", up)
	}
	_ = c.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":2,"method":"eth_unsubscribe","params":["`+sub+`"]}`))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		var n note
		if err := c.ReadJSON(&n); err != nil {
			t.Fatal(err)
		}
		if n.Method == "" && n.Result == true {
			return
		}
	}
}

// failover measures the time from fault injection to the first notification
// delivered from a different upstream under the client's original subscription ID.
func failover(t *testing.T, inject func(*fakeupstream.Server)) time.Duration {
	f := newFixture(t, 3, 10*time.Millisecond, pool.Options{ProbeInterval: 50 * time.Millisecond, ProbeTimeout: 50 * time.Millisecond},
		Options{PingInterval: 50 * time.Millisecond})
	c, sub := dialSub(t, f)
	first := next(t, c, sub)
	victim := f.fakes[first[0]-'a']
	start := time.Now()
	inject(victim)
	for {
		if up := next(t, c, sub); up != first {
			return time.Since(start)
		}
	}
}

func TestChaosKillUpstream(t *testing.T) {
	var worst time.Duration
	for i := 0; i < 5; i++ {
		d := failover(t, func(s *fakeupstream.Server) { s.SetFailing(true) })
		if d > worst {
			worst = d
		}
	}
	t.Logf("kill: worst failover over 5 runs = %v", worst)
	if worst > 500*time.Millisecond {
		t.Fatalf("failover took %v, budget 500ms", worst)
	}
}

func TestChaosSilentHang(t *testing.T) {
	var worst time.Duration
	for i := 0; i < 5; i++ {
		d := failover(t, func(s *fakeupstream.Server) { s.Freeze(true) })
		if d > worst {
			worst = d
		}
	}
	t.Logf("hang (ping 50ms, 2 missed pongs): worst failover over 5 runs = %v", worst)
	if worst > 750*time.Millisecond {
		t.Fatalf("failover took %v, budget 750ms", worst)
	}
}

func TestChaosRollingKills(t *testing.T) {
	f := newFixture(t, 3, 5*time.Millisecond, pool.Options{ProbeInterval: 20 * time.Millisecond, ProbeTimeout: 50 * time.Millisecond, BreakerCooldown: 50 * time.Millisecond},
		Options{PingInterval: 50 * time.Millisecond})
	c, sub := dialSub(t, f)
	var switches atomic.Int32
	cur := next(t, c, sub)
	for round := 0; round < 6; round++ {
		victim := f.fakes[cur[0]-'a']
		victim.SetFailing(true)
		for {
			up := next(t, c, sub)
			if up != cur {
				cur = up
				switches.Add(1)
				break
			}
		}
		victim.SetFailing(false) // comes back; may be chosen again later
	}
	if switches.Load() != 6 {
		t.Fatalf("switches=%d", switches.Load())
	}
}
