// Package clock abstracts time so that time-driven logic (health windows,
// wave promotion) can be tested deterministically with a fake clock instead
// of sleeps.
package clock

import (
	"sync"
	"time"
)

// Clock is the minimal surface the rollout engine needs from time.
type Clock interface {
	Now() time.Time
	// AfterFunc schedules f to run after d. f runs at most once.
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is a cancellable scheduled callback.
type Timer interface {
	// Stop cancels the timer. It reports whether the call prevented the
	// callback from firing.
	Stop() bool
}

// ---------------------------------------------------------------------------
// Real clock
// ---------------------------------------------------------------------------

// Real is the production Clock backed by package time.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

func (Real) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

// ---------------------------------------------------------------------------
// Fake clock
// ---------------------------------------------------------------------------

// Fake is a manually advanced Clock for tests. Timers fire synchronously
// inside Advance, in deadline order, with the clock set exactly to each
// timer's deadline — so test assertions observe a fully settled state and
// never race against a background scheduler.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	seq    int
}

type fakeTimer struct {
	clk      *Fake
	deadline time.Time
	seq      int // tie-break: FIFO among equal deadlines
	fn       func()
	stopped  bool
	fired    bool
}

// NewFake returns a Fake clock frozen at start.
func NewFake(start time.Time) *Fake { return &Fake{now: start} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) AfterFunc(d time.Duration, fn func()) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{clk: f, deadline: f.now.Add(d), seq: f.seq, fn: fn}
	f.seq++
	f.timers = append(f.timers, t)
	return t
}

func (t *fakeTimer) Stop() bool {
	t.clk.mu.Lock()
	defer t.clk.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

// Advance moves the clock forward by d, firing due timers in deadline order.
// Callbacks run with the clock's mutex released, so they may schedule new
// timers; timers scheduled inside a callback fire within the same Advance if
// they fall inside the window.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	for {
		t := f.popDue(target)
		if t == nil {
			break
		}
		if t.deadline.After(f.now) {
			f.now = t.deadline
		}
		t.fired = true
		fn := t.fn
		f.mu.Unlock()
		fn()
		f.mu.Lock()
	}
	f.now = target
	f.mu.Unlock()
}

// popDue removes and returns the earliest un-stopped timer with
// deadline <= target, or nil.
func (f *Fake) popDue(target time.Time) *fakeTimer {
	best := -1
	for i, t := range f.timers {
		if t.stopped || t.fired || t.deadline.After(target) {
			continue
		}
		if best == -1 || t.deadline.Before(f.timers[best].deadline) ||
			(t.deadline.Equal(f.timers[best].deadline) && t.seq < f.timers[best].seq) {
			best = i
		}
	}
	if best == -1 {
		return nil
	}
	t := f.timers[best]
	f.timers = append(f.timers[:best], f.timers[best+1:]...)
	return t
}
