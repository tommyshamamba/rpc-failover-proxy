package pool

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func server(delay time.Duration, status int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		w.WriteHeader(status)
	}))
}

func names(es []*Endpoint) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.Name)
	}
	return out
}

func TestRankingByLatencyAndHealth(t *testing.T) {
	slow, fast, bad := server(30*time.Millisecond, 200), server(0, 200), server(0, 500)
	defer slow.Close()
	defer fast.Close()
	defer bad.Close()
	p, err := New([]Config{{Name: "slow", HTTPURL: slow.URL}, {Name: "fast", HTTPURL: fast.URL}, {Name: "bad", HTTPURL: bad.URL}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got := names(p.Ranked(false)); len(got) != 3 || got[0] != "slow" {
		t.Fatalf("before probing, config order expected, got %v", got)
	}
	p.ProbeOnce(context.Background())
	got := names(p.Ranked(false))
	if len(got) != 2 || got[0] != "fast" || got[1] != "slow" {
		t.Fatalf("want [fast slow], got %v", got)
	}
}

func TestBreakerExcludesEndpoint(t *testing.T) {
	s := server(0, 200)
	defer s.Close()
	p, _ := New([]Config{{Name: "a", HTTPURL: s.URL}, {Name: "b", HTTPURL: s.URL}}, Options{BreakerThreshold: 2, BreakerCooldown: time.Hour})
	a := p.Endpoints()[0]
	p.Report(a, errors.New("x"))
	p.Report(a, errors.New("x"))
	if got := names(p.Ranked(false)); len(got) != 1 || got[0] != "b" {
		t.Fatalf("open breaker should hide a, got %v", got)
	}
}

func TestWSFilterAndValidation(t *testing.T) {
	p, _ := New([]Config{{Name: "h", HTTPURL: "http://x"}, {Name: "w", HTTPURL: "http://y", WSURL: "ws://y"}}, Options{})
	if got := names(p.Ranked(true)); len(got) != 1 || got[0] != "w" {
		t.Fatalf("got %v", got)
	}
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("empty pool must fail")
	}
	if _, err := New([]Config{{Name: "a", HTTPURL: "x"}, {Name: "a", HTTPURL: "y"}}, Options{}); err == nil {
		t.Fatal("duplicate names must fail")
	}
}

func TestMarkDownAndRecovery(t *testing.T) {
	s := server(0, 200)
	defer s.Close()
	p, _ := New([]Config{{Name: "a", HTTPURL: s.URL}}, Options{BreakerThreshold: 5})
	p.MarkDown(p.Endpoints()[0], errors.New("ws dropped"))
	if len(p.Ranked(false)) != 0 {
		t.Fatal("marked down endpoint must be hidden")
	}
	p.ProbeOnce(context.Background())
	if len(p.Ranked(false)) != 1 {
		t.Fatal("successful probe must restore it")
	}
}
