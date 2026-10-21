package breaker

import (
	"sync"
	"testing"
	"time"
)

func TestLifecycle(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(3, time.Second)
	b.SetClock(func() time.Time { return now })
	for i := 0; i < 2; i++ {
		b.Failure()
	}
	if b.State() != Closed || !b.Allow() {
		t.Fatal("should stay closed below threshold")
	}
	b.Failure()
	if b.State() != Open || b.Allow() {
		t.Fatal("should open at threshold")
	}
	now = now.Add(time.Second)
	if !b.Allow() || b.State() != HalfOpen {
		t.Fatal("should admit one trial after cooldown")
	}
	if b.Allow() {
		t.Fatal("only one half-open trial")
	}
	b.Failure()
	if b.State() != Open {
		t.Fatal("failed trial re-opens")
	}
	now = now.Add(time.Second)
	b.Allow()
	b.Success()
	if b.State() != Closed {
		t.Fatal("successful trial closes")
	}
}

func TestSuccessResetsCount(t *testing.T) {
	b := New(2, time.Minute)
	b.Failure()
	b.Success()
	b.Failure()
	if b.State() != Closed {
		t.Fatal("non-consecutive failures must not open")
	}
}

func TestConcurrent(t *testing.T) {
	b := New(5, time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				if b.Allow() {
					if (i+j)%3 == 0 {
						b.Failure()
					} else {
						b.Success()
					}
				}
				_ = b.Available()
			}
		}(i)
	}
	wg.Wait()
}
