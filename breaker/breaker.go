// Package breaker implements a small consecutive-failure circuit breaker.
//
// States: Closed (traffic flows), Open (traffic rejected until Cooldown has
// elapsed) and HalfOpen (a single trial request is allowed; success closes the
// breaker, failure re-opens it).
package breaker

import (
	"sync"
	"time"
)

// State is the breaker state.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	default:
		return "half_open"
	}
}

// Breaker is safe for concurrent use.
type Breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	state    State
	failures int
	openedAt time.Time
	trial    bool // a half-open trial is in flight
}

// New returns a breaker that opens after threshold consecutive failures and
// allows a trial after cooldown.
func New(threshold int, cooldown time.Duration) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now}
}

// SetClock replaces the time source (tests).
func (b *Breaker) SetClock(now func() time.Time) { b.mu.Lock(); b.now = now; b.mu.Unlock() }

// Allow reports whether a request may proceed. In HalfOpen only one caller
// at a time is admitted.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true
	case Open:
		if b.now().Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.state = HalfOpen
		b.trial = true
		return true
	default:
		if b.trial {
			return false
		}
		b.trial = true
		return true
	}
}

// Available is like Allow but has no side effects; used for ranking.
func (b *Breaker) Available() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true
	case Open:
		return b.now().Sub(b.openedAt) >= b.cooldown
	default:
		return !b.trial
	}
}

// Success records a successful request.
func (b *Breaker) Success() {
	b.mu.Lock()
	b.state, b.failures, b.trial = Closed, 0, false
	b.mu.Unlock()
}

// Failure records a failed request.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.trial = false
	if b.state == HalfOpen {
		b.state, b.openedAt = Open, b.now()
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.state, b.openedAt = Open, b.now()
	}
}

// State returns the current state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
