// Package supervisor runs long-lived goroutines and restarts them with
// capped, jittered exponential backoff when they return an error or panic.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"
)

// Task is a long-lived unit of work. It should return only when ctx is done
// or when it hits an error it cannot recover from itself.
type Task func(ctx context.Context) error

// Backoff configures restart delays.
type Backoff struct {
	Initial time.Duration // default 100ms
	Max     time.Duration // default 30s
	Factor  float64       // default 2
	Jitter  float64       // fraction 0..1 of the delay randomised, default 0.2
	// ResetAfter: a run that lasted at least this long resets the delay. Default 10s.
	ResetAfter time.Duration
}

func (b *Backoff) defaults() {
	if b.Initial <= 0 {
		b.Initial = 100 * time.Millisecond
	}
	if b.Max <= 0 {
		b.Max = 30 * time.Second
	}
	if b.Factor < 1 {
		b.Factor = 2
	}
	if b.Jitter < 0 || b.Jitter > 1 {
		b.Jitter = 0.2
	}
	if b.ResetAfter <= 0 {
		b.ResetAfter = 10 * time.Second
	}
}

// Delay returns the delay before restart number n (n starts at 0), without jitter.
func (b Backoff) Delay(n int) time.Duration {
	b.defaults()
	d := float64(b.Initial)
	for i := 0; i < n; i++ {
		d *= b.Factor
		if d >= float64(b.Max) {
			return b.Max
		}
	}
	return time.Duration(d)
}

func (b Backoff) jittered(n int) time.Duration {
	d := b.Delay(n)
	if b.Jitter == 0 {
		return d
	}
	span := float64(d) * b.Jitter
	return time.Duration(float64(d) - span/2 + rand.Float64()*span)
}

// Supervisor owns a set of named tasks.
type Supervisor struct {
	Backoff   Backoff
	Logger    *slog.Logger
	OnRestart func(task string, err error) // optional, e.g. metrics

	wg sync.WaitGroup
}

// ErrPanic wraps a recovered panic.
var ErrPanic = errors.New("task panicked")

// Go starts task under supervision. It is restarted until ctx is cancelled.
func (s *Supervisor) Go(ctx context.Context, name string, task Task) {
	s.Backoff.defaults()
	log := s.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		attempt := 0
		for {
			start := time.Now()
			err := runSafe(ctx, task)
			if ctx.Err() != nil {
				return
			}
			if time.Since(start) >= s.Backoff.ResetAfter {
				attempt = 0
			}
			if err == nil {
				err = errors.New("task returned without error before shutdown")
			}
			delay := s.Backoff.jittered(attempt)
			attempt++
			log.Warn("task stopped; restarting", "task", name, "err", err, "attempt", attempt, "delay_ms", delay.Milliseconds())
			if s.OnRestart != nil {
				s.OnRestart(name, err)
			}
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}()
}

// Wait blocks until every task has exited (after ctx cancellation).
func (s *Supervisor) Wait() { s.wg.Wait() }

func runSafe(ctx context.Context, task Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrPanic, r, debug.Stack())
		}
	}()
	return task(ctx)
}
