package clock

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestFakeAdvanceFiresInDeadlineOrder(t *testing.T) {
	f := NewFake(t0)
	var order []string
	f.AfterFunc(3*time.Second, func() { order = append(order, "c") })
	f.AfterFunc(1*time.Second, func() { order = append(order, "a") })
	f.AfterFunc(2*time.Second, func() { order = append(order, "b") })

	f.Advance(2 * time.Second)
	if got := len(order); got != 2 {
		t.Fatalf("after 2s: fired %d timers, want 2 (%v)", got, order)
	}
	if order[0] != "a" || order[1] != "b" {
		t.Fatalf("fire order = %v, want [a b]", order)
	}
	f.Advance(1 * time.Second)
	if len(order) != 3 || order[2] != "c" {
		t.Fatalf("after 3s total: order = %v, want [a b c]", order)
	}
}

func TestFakeStopPreventsFiring(t *testing.T) {
	f := NewFake(t0)
	fired := false
	tm := f.AfterFunc(time.Second, func() { fired = true })
	if !tm.Stop() {
		t.Fatal("Stop() = false, want true on pending timer")
	}
	f.Advance(2 * time.Second)
	if fired {
		t.Fatal("stopped timer fired")
	}
	if tm.Stop() {
		t.Fatal("second Stop() = true, want false")
	}
}

func TestFakeCallbackMaySetNowAndReschedule(t *testing.T) {
	f := NewFake(t0)
	var at []time.Time
	// Callback schedules a follow-up inside the same Advance window.
	f.AfterFunc(1*time.Second, func() {
		at = append(at, f.Now())
		f.AfterFunc(1*time.Second, func() { at = append(at, f.Now()) })
	})
	f.Advance(5 * time.Second)
	if len(at) != 2 {
		t.Fatalf("fired %d callbacks, want 2", len(at))
	}
	if !at[0].Equal(t0.Add(1 * time.Second)) {
		t.Errorf("first fire at %v, want %v", at[0], t0.Add(time.Second))
	}
	if !at[1].Equal(t0.Add(2 * time.Second)) {
		t.Errorf("second fire at %v, want %v", at[1], t0.Add(2*time.Second))
	}
	if !f.Now().Equal(t0.Add(5 * time.Second)) {
		t.Errorf("final Now() = %v, want %v", f.Now(), t0.Add(5*time.Second))
	}
}
