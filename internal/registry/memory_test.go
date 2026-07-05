package registry

import (
	"errors"
	"testing"
	"time"
)

var now = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestUpsertIsIdempotentAndRefreshes(t *testing.T) {
	s := NewInMemory()
	if err := s.UpsertDevice("d1", map[string]string{"site": "a"}, "ap-1", now); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertDevice("d1", map[string]string{"site": "b"}, "ap-1", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	d, ok := s.Device("d1")
	if !ok {
		t.Fatal("device not found after upsert")
	}
	if d.Labels["site"] != "b" || !d.LastSeen.Equal(now.Add(time.Minute)) {
		t.Errorf("re-register did not refresh: %+v", d)
	}
	if got := len(s.ListDevices()); got != 1 {
		t.Errorf("ListDevices len = %d, want 1", got)
	}
}

func TestRecordTelemetryUnknownDevice(t *testing.T) {
	s := NewInMemory()
	err := s.RecordTelemetry("ghost", HealthHealthy, "v1", now)
	var unknown ErrUnknownDevice
	if !errors.As(err, &unknown) {
		t.Fatalf("err = %v, want ErrUnknownDevice", err)
	}
}

func TestDesiredResolutionOrder(t *testing.T) {
	s := NewInMemory()
	mustUpsert(t, s, "d1")
	if err := s.PutConfig(Config{Version: "v1", Blob: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutConfig(Config{Version: "v2", Blob: []byte("two")}); err != nil {
		t.Fatal(err)
	}

	// No baseline, no assignment: nothing desired.
	if _, ok := s.Desired("d1"); ok {
		t.Fatal("Desired should be empty before baseline/assignment")
	}
	// Baseline applies to unassigned devices.
	if err := s.SetBaseline("v1"); err != nil {
		t.Fatal(err)
	}
	cfg, ok := s.Desired("d1")
	if !ok || cfg.Version != "v1" {
		t.Fatalf("Desired = %v %v, want v1", cfg.Version, ok)
	}
	// Explicit assignment overrides baseline.
	if err := s.SetDesired([]string{"d1"}, "v2"); err != nil {
		t.Fatal(err)
	}
	cfg, _ = s.Desired("d1")
	if cfg.Version != "v2" {
		t.Fatalf("Desired = %v, want v2 after assignment", cfg.Version)
	}
}

func TestSetDesiredValidation(t *testing.T) {
	s := NewInMemory()
	mustUpsert(t, s, "d1")
	var unknownCfg ErrUnknownConfig
	if err := s.SetDesired([]string{"d1"}, "nope"); !errors.As(err, &unknownCfg) {
		t.Fatalf("err = %v, want ErrUnknownConfig", err)
	}
	if err := s.PutConfig(Config{Version: "v1"}); err != nil {
		t.Fatal(err)
	}
	var unknownDev ErrUnknownDevice
	if err := s.SetDesired([]string{"d1", "ghost"}, "v1"); !errors.As(err, &unknownDev) {
		t.Fatalf("err = %v, want ErrUnknownDevice", err)
	}
	// All-or-nothing: d1 must not have been assigned by the failed call.
	if _, ok := s.Desired("d1"); ok {
		t.Fatal("failed SetDesired must not partially apply")
	}
}

func TestSelectorAndSelect(t *testing.T) {
	s := NewInMemory()
	if err := s.UpsertDevice("b", map[string]string{"ring": "canary"}, "m", now); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertDevice("a", map[string]string{"ring": "canary", "site": "x"}, "m", now); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertDevice("c", map[string]string{"ring": "prod"}, "m", now); err != nil {
		t.Fatal(err)
	}

	if got := Select(s, Selector{All: true}); len(got) != 3 || got[0] != "a" {
		t.Errorf("Select(all) = %v, want [a b c]", got)
	}
	got := Select(s, Selector{MatchLabels: map[string]string{"ring": "canary"}})
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("Select(ring=canary) = %v, want [a b]", got)
	}
	// Empty selector (not all, no labels) selects nothing — refuse to
	// accidentally target the whole fleet.
	if got := Select(s, Selector{}); len(got) != 0 {
		t.Errorf("Select(empty) = %v, want []", got)
	}
}

func TestStoreCopiesAreIsolated(t *testing.T) {
	s := NewInMemory()
	labels := map[string]string{"k": "v"}
	if err := s.UpsertDevice("d1", labels, "m", now); err != nil {
		t.Fatal(err)
	}
	labels["k"] = "mutated"
	d, _ := s.Device("d1")
	if d.Labels["k"] != "v" {
		t.Error("store shares label map with caller")
	}
	d.Labels["k"] = "mutated-out"
	d2, _ := s.Device("d1")
	if d2.Labels["k"] != "v" {
		t.Error("returned device shares label map with store")
	}
}

func mustUpsert(t *testing.T, s Store, id string) {
	t.Helper()
	if err := s.UpsertDevice(id, nil, "model", now); err != nil {
		t.Fatal(err)
	}
}
