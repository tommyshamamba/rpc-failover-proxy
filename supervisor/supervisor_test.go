package supervisor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDelayCapped(t *testing.T) {
	b := Backoff{Initial: 10 * time.Millisecond, Max: 50 * time.Millisecond, Factor: 2}
	want := []time.Duration{10, 20, 40, 50, 50}
	for i, w := range want {
		if got := b.Delay(i); got != w*time.Millisecond {
			t.Fatalf("Delay(%d)=%v want %v", i, got, w*time.Millisecond)
		}
	}
}

func TestRestartsOnErrorAndPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var runs, restarts atomic.Int32
	s := &Supervisor{Backoff: Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond, Jitter: 0},
		OnRestart: func(string, error) { restarts.Add(1) }}
	s.Go(ctx, "flaky", func(ctx context.Context) error {
		n := runs.Add(1)
		switch {
		case n == 1:
			return errors.New("boom")
		case n == 2:
			panic("kaboom")
		case n >= 3:
			<-ctx.Done()
		}
		return ctx.Err()
	})
	deadline := time.After(2 * time.Second)
	for runs.Load() < 3 {
		select {
		case <-deadline:
			t.Fatal("task was not restarted")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	s.Wait()
	if restarts.Load() != 2 {
		t.Fatalf("restarts=%d want 2", restarts.Load())
	}
}

func TestStopsDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Supervisor{Backoff: Backoff{Initial: time.Hour}}
	s.Go(ctx, "fails", func(context.Context) error { return errors.New("x") })
	time.Sleep(10 * time.Millisecond)
	done := make(chan struct{})
	go func() { cancel(); s.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("supervisor did not stop while sleeping in backoff")
	}
}
