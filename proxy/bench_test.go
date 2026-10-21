package proxy

import (
	"bytes"
	"io"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/tommyshamamba/rpc-failover-proxy/pool"
)

var benchBody = []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)

func benchPost(b *testing.B, c *http.Client, url string) {
	resp, err := c.Post(url, "application/json", bytes.NewReader(benchBody))
	if err != nil {
		b.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		b.Fatalf("status %d", resp.StatusCode)
	}
}

func client() *http.Client {
	return &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 512}}
}

// BenchmarkHTTPDirect is the baseline: client -> fake upstream.
func BenchmarkHTTPDirect(b *testing.B) {
	f := newFixture(b, 1, time.Hour, pool.Options{ProbeInterval: time.Hour}, Options{})
	c := client()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchPost(b, c, f.servers[0].URL)
		}
	})
}

// BenchmarkHTTPProxy measures client -> proxy -> fake upstream.
func BenchmarkHTTPProxy(b *testing.B) {
	f := newFixture(b, 3, time.Hour, pool.Options{ProbeInterval: time.Hour}, Options{})
	c := client()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchPost(b, c, f.proxy.URL)
		}
	})
}

// BenchmarkHTTPProxyLatency reports sequential round-trip percentiles through the proxy.
func BenchmarkHTTPProxyLatency(b *testing.B) {
	f := newFixture(b, 3, time.Hour, pool.Options{ProbeInterval: time.Hour}, Options{})
	c := client()
	lat := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := time.Now()
		benchPost(b, c, f.proxy.URL)
		lat = append(lat, time.Since(s))
	}
	b.StopTimer()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	b.ReportMetric(float64(lat[len(lat)/2].Microseconds()), "p50_us")
	b.ReportMetric(float64(lat[len(lat)*99/100].Microseconds()), "p99_us")
}

// BenchmarkNotificationRewrite measures the per-message cost of translating a
// subscription ID after failover (the WebSocket hot path).
func BenchmarkNotificationRewrite(b *testing.B) {
	p := New(mustPool(b), Options{})
	s := &session{p: p, subs: map[string]*subscription{}, byUp: map[string]*subscription{},
		pending: map[string][]byte{}, inflight: map[string]bool{}, resubs: map[string]*subscription{}}
	sub := &subscription{clientID: "0xclient", upstreamID: "0xup"}
	s.subs["0xclient"], s.byUp["0xup"] = sub, sub
	msg := []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xup","result":{"number":"0x1b4","hash":"0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae"}}}`)
	s.client = nil
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.translate(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func mustPool(b *testing.B) *pool.Pool {
	p, err := pool.New([]pool.Config{{Name: "a", HTTPURL: "http://127.0.0.1:1"}}, pool.Options{})
	if err != nil {
		b.Fatal(err)
	}
	return p
}
