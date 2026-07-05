package rollout

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ericsson-kuma/fleet-ctl/internal/clock"
	"github.com/ericsson-kuma/fleet-ctl/internal/registry"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

const window = 10 * time.Second

// fixture: n devices dev-00..dev-NN, baseline config v1 assigned fleet-wide.
func fixture(t *testing.T, n int) (*Engine, *registry.InMemory, *clock.Fake) {
	t.Helper()
	store := registry.NewInMemory()
	clk := clock.NewFake(t0)
	for i := 0; i < n; i++ {
		if err := store.UpsertDevice(devID(i), map[string]string{"ring": "prod"}, "ap-1", t0); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.PutConfig(registry.Config{Version: "v1", Blob: []byte("baseline")}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetBaseline("v1"); err != nil {
		t.Fatal(err)
	}
	return New(clk, store), store, clk
}

func devID(i int) string { return fmt.Sprintf("dev-%02d", i) }

func params(threshold float64, percents ...int) Params {
	return Params{
		Config:           registry.Config{Version: "v2", Blob: []byte("new")},
		Target:           registry.Selector{All: true},
		WavePercents:     percents,
		FailureThreshold: threshold,
		HealthWindow:     window,
	}
}

func ackWave(e *Engine, ids []string, version string, ok bool) {
	for _, id := range ids {
		e.HandleApplyResult(id, version, ok, map[bool]string{true: "", false: "apply failed"}[ok])
	}
}

func desiredVersion(t *testing.T, s registry.Store, id string) string {
	t.Helper()
	cfg, ok := s.Desired(id)
	if !ok {
		return "<none>"
	}
	return cfg.Version
}

func kinds(events []Event) []string {
	var out []string
	for _, ev := range events {
		out = append(out, ev.Kind)
	}
	return out
}

func countKind(events []Event, kind string) int {
	n := 0
	for _, ev := range events {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}

// --- happy path -------------------------------------------------------------

func TestHappyPathPromotesThroughAllWaves(t *testing.T) {
	e, store, clk := fixture(t, 20)
	v, err := e.Create(params(0.2, 5, 25, 100))
	if err != nil {
		t.Fatal(err)
	}

	// ceil wave math: 5% of 20 = 1, 25% = 5 (delta 4), 100% = 20 (delta 15).
	wantDeltas := []int{1, 4, 15}
	for i, w := range v.Waves {
		if len(w.TargetIDs) != wantDeltas[i] {
			t.Fatalf("wave %d delta = %d, want %d", i, len(w.TargetIDs), wantDeltas[i])
		}
	}

	// Canary assigned immediately; the rest of the fleet is untouched.
	if got := desiredVersion(t, store, "dev-00"); got != "v2" {
		t.Fatalf("canary desired = %s, want v2", got)
	}
	if got := desiredVersion(t, store, "dev-19"); got != "v1" {
		t.Fatalf("uncovered device desired = %s, want baseline v1", got)
	}

	for waveIdx, w := range v.Waves {
		cur, _ := e.Get(v.ID)
		if cur.CurrentWave != waveIdx || cur.State != StateInProgress {
			t.Fatalf("before wave %d acks: state=%v wave=%d", waveIdx, cur.State, cur.CurrentWave)
		}
		ackWave(e, w.TargetIDs, "v2", true)
		clk.Advance(window)
	}

	final, _ := e.Get(v.ID)
	if final.State != StateSucceeded {
		t.Fatalf("state = %v, want SUCCEEDED", final.State)
	}
	if got := store.Baseline(); got != "v2" {
		t.Errorf("baseline = %s, want v2 after success", got)
	}
	for i := 0; i < 20; i++ {
		if got := desiredVersion(t, store, devID(i)); got != "v2" {
			t.Errorf("%s desired = %s, want v2", devID(i), got)
		}
	}
	replay, _, _, err := e.Watch(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := countKind(replay, EventWavePromoted); got != 3 {
		t.Errorf("WAVE_PROMOTED count = %d, want 3 (events: %v)", got, kinds(replay))
	}
	if replay[len(replay)-1].Kind != EventSucceeded {
		t.Errorf("last event = %s, want SUCCEEDED", replay[len(replay)-1].Kind)
	}
}

// --- guardrail trips --------------------------------------------------------

func TestGuardrailHaltsAndRollsBackOnExplicitFailures(t *testing.T) {
	e, store, _ := fixture(t, 20)
	v, err := e.Create(params(0.2, 25, 100)) // wave 0: 5 devices
	if err != nil {
		t.Fatal(err)
	}
	wave0 := v.Waves[0].TargetIDs
	if len(wave0) != 5 {
		t.Fatalf("wave 0 size = %d, want 5", len(wave0))
	}

	e.HandleApplyResult(wave0[0], "v2", true, "")
	e.HandleApplyResult(wave0[1], "v2", false, "boom") // 1/5 = 0.2, at budget: no halt
	mid, _ := e.Get(v.ID)
	if mid.State != StateInProgress {
		t.Fatalf("state after failure at threshold = %v, want IN_PROGRESS", mid.State)
	}
	e.HandleApplyResult(wave0[2], "v2", false, "boom") // 2/5 = 0.4 > 0.2: halt now
	final, _ := e.Get(v.ID)
	if final.State != StateRolledBack {
		t.Fatalf("state = %v, want ROLLED_BACK", final.State)
	}
	if final.Waves[0].State != WaveFailed {
		t.Errorf("wave 0 state = %v, want FAILED", final.Waves[0].State)
	}

	// Touched devices reverted to previous baseline; blast radius stops at wave 0.
	for _, id := range wave0 {
		if got := desiredVersion(t, store, id); got != "v1" {
			t.Errorf("%s desired = %s, want v1 after rollback", id, got)
		}
	}
	if got := store.Baseline(); got != "v1" {
		t.Errorf("baseline = %s, want v1 (failed rollout must not become baseline)", got)
	}
	replay, _, _, _ := e.Watch(v.ID)
	seq := kinds(replay)
	if countKind(replay, EventHalted) != 1 || countKind(replay, EventRolledBack) != 1 {
		t.Errorf("want exactly one HALTED and one ROLLED_BACK, got %v", seq)
	}
	// Wave 1 must never have started.
	if countKind(replay, EventWaveStarted) != 1 {
		t.Errorf("wave 1 started despite halt: %v", seq)
	}
}

// --- boundary: rate == threshold is tolerated, just above is not ------------

func TestThresholdBoundary(t *testing.T) {
	// 10 devices, single wave, threshold 0.2: 2 failures (rate exactly 0.2)
	// must NOT halt; a 3rd failure (0.3) must.
	t.Run("exactly_at_threshold_promotes", func(t *testing.T) {
		e, store, clk := fixture(t, 10)
		v, err := e.Create(params(0.2, 100))
		if err != nil {
			t.Fatal(err)
		}
		all := v.Waves[0].TargetIDs
		ackWave(e, all[:8], "v2", true)
		ackWave(e, all[8:], "v2", false) // 2/10 == threshold
		mid, _ := e.Get(v.ID)
		if mid.State != StateInProgress {
			t.Fatalf("state = %v, want IN_PROGRESS at exact threshold", mid.State)
		}
		clk.Advance(window)
		final, _ := e.Get(v.ID)
		if final.State != StateSucceeded {
			t.Fatalf("state = %v, want SUCCEEDED (budget is 'at most')", final.State)
		}
		if got := store.Baseline(); got != "v2" {
			t.Errorf("baseline = %s, want v2", got)
		}
	})
	t.Run("just_above_threshold_halts", func(t *testing.T) {
		e, _, _ := fixture(t, 10)
		v, err := e.Create(params(0.2, 100))
		if err != nil {
			t.Fatal(err)
		}
		all := v.Waves[0].TargetIDs
		ackWave(e, all[:3], "v2", false) // 3/10 > 0.2
		final, _ := e.Get(v.ID)
		if final.State != StateRolledBack {
			t.Fatalf("state = %v, want ROLLED_BACK just above threshold", final.State)
		}
	})
}

// --- silence counts against the budget at window end -------------------------

func TestUnresponsiveDevicesFailTheWaveAtWindowEnd(t *testing.T) {
	e, store, clk := fixture(t, 5)
	v, err := e.Create(params(0.2, 100))
	if err != nil {
		t.Fatal(err)
	}
	all := v.Waves[0].TargetIDs
	ackWave(e, all[:3], "v2", true) // 3 ok, 2 silent -> 2/5 = 0.4 > 0.2 at window end
	mid, _ := e.Get(v.ID)
	if mid.State != StateInProgress {
		t.Fatalf("state = %v, want IN_PROGRESS before window end", mid.State)
	}
	clk.Advance(window)
	final, _ := e.Get(v.ID)
	if final.State != StateRolledBack {
		t.Fatalf("state = %v, want ROLLED_BACK (silence is not health)", final.State)
	}
	replay, _, _, _ := e.Watch(v.ID)
	halted := replay[len(replay)-2]
	if halted.Kind != EventHalted || !strings.Contains(halted.Message, "unresponsive") {
		t.Errorf("HALTED event should attribute silence, got %+v", halted)
	}
	for _, id := range all {
		if got := desiredVersion(t, store, id); got != "v1" {
			t.Errorf("%s desired = %s, want v1", id, got)
		}
	}
}

// --- rollback idempotency ----------------------------------------------------

func TestRollbackIsIdempotentAgainstStragglers(t *testing.T) {
	e, store, clk := fixture(t, 20)
	v, err := e.Create(params(0.2, 25, 100))
	if err != nil {
		t.Fatal(err)
	}
	wave0 := v.Waves[0].TargetIDs
	ackWave(e, wave0[:2], "v2", false) // 2/5 = 0.4 > 0.2: halt + rollback
	rolled, _ := e.Get(v.ID)
	if rolled.State != StateRolledBack {
		t.Fatalf("precondition: state = %v, want ROLLED_BACK", rolled.State)
	}
	replayBefore, _, _, _ := e.Watch(v.ID)

	// Straggler acks (ok and fail), a duplicate ack, an unrelated version,
	// and the stale wave timer all arrive after rollback.
	e.HandleApplyResult(wave0[2], "v2", false, "late boom")
	e.HandleApplyResult(wave0[3], "v2", true, "")
	e.HandleApplyResult(wave0[0], "v2", false, "duplicate")
	e.HandleApplyResult(wave0[0], "v9", true, "")
	clk.Advance(2 * window) // stale health-window timer fires into a dead rollout

	replayAfter, _, _, _ := e.Watch(v.ID)
	if len(replayAfter) != len(replayBefore) {
		t.Errorf("events grew after terminal state: %v -> %v", kinds(replayBefore), kinds(replayAfter))
	}
	final, _ := e.Get(v.ID)
	if final.State != StateRolledBack {
		t.Errorf("state = %v, want ROLLED_BACK to stay terminal", final.State)
	}
	for _, id := range wave0 {
		if got := desiredVersion(t, store, id); got != "v1" {
			t.Errorf("%s desired = %s, want v1 (rollback must not be re-applied or undone)", id, got)
		}
	}
}

// --- selector scoping ---------------------------------------------------------

func TestSelectorScopesRolloutAndRollback(t *testing.T) {
	e, store, _ := fixture(t, 4) // dev-00..03 labeled ring=prod
	if err := store.UpsertDevice("edge-1", map[string]string{"ring": "edge"}, "ap-2", t0); err != nil {
		t.Fatal(err)
	}
	p := params(0.0, 100)
	p.Target = registry.Selector{MatchLabels: map[string]string{"ring": "prod"}}
	v, err := e.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.TargetIDs) != 4 {
		t.Fatalf("targets = %v, want the 4 prod devices", v.TargetIDs)
	}
	if got := desiredVersion(t, store, "edge-1"); got != "v1" {
		t.Errorf("edge-1 desired = %s, want v1 (out of scope)", got)
	}
	e.HandleApplyResult("dev-00", "v2", false, "boom") // threshold 0 -> any failure halts
	final, _ := e.Get(v.ID)
	if final.State != StateRolledBack {
		t.Fatalf("state = %v, want ROLLED_BACK", final.State)
	}
	if got := desiredVersion(t, store, "edge-1"); got != "v1" {
		t.Errorf("edge-1 desired = %s after rollback, want untouched v1", got)
	}
}

// --- watch: replay + live ------------------------------------------------------

func TestWatchReplaysThenStreamsLive(t *testing.T) {
	e, _, clk := fixture(t, 5)
	v, err := e.Create(params(0.2, 100))
	if err != nil {
		t.Fatal(err)
	}
	replay, live, cancel, err := e.Watch(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if got := kinds(replay); len(got) != 2 || got[0] != EventCreated || got[1] != EventWaveStarted {
		t.Fatalf("replay = %v, want [CREATED WAVE_STARTED]", got)
	}
	ackWave(e, v.Waves[0].TargetIDs, "v2", true)
	clk.Advance(window)
	var liveKinds []string
	for ev := range live { // channel closes at terminal state
		liveKinds = append(liveKinds, ev.Kind)
	}
	want := []string{EventDeviceOK, EventDeviceOK, EventDeviceOK, EventDeviceOK, EventDeviceOK, EventWavePromoted, EventSucceeded}
	if len(liveKinds) != len(want) {
		t.Fatalf("live events = %v, want %v", liveKinds, want)
	}
	for i := range want {
		if liveKinds[i] != want[i] {
			t.Fatalf("live[%d] = %s, want %s (all: %v)", i, liveKinds[i], want[i], liveKinds)
		}
	}
	// Watching a finished rollout: full replay, closed channel.
	replay2, live2, _, err := e.Watch(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay2) != len(replay)+len(want) {
		t.Errorf("terminal replay length = %d, want %d", len(replay2), len(replay)+len(want))
	}
	if _, open := <-live2; open {
		t.Error("live channel of terminal rollout should be closed")
	}
}

// --- validation -----------------------------------------------------------------

func TestCreateValidation(t *testing.T) {
	e, _, _ := fixture(t, 5)
	base := params(0.2, 5, 100)
	cases := []struct {
		name   string
		mutate func(*Params)
	}{
		{"empty version", func(p *Params) { p.Config.Version = "" }},
		{"no waves", func(p *Params) { p.WavePercents = nil }},
		{"not ascending", func(p *Params) { p.WavePercents = []int{25, 25, 100} }},
		{"over 100", func(p *Params) { p.WavePercents = []int{5, 120} }},
		{"missing final 100", func(p *Params) { p.WavePercents = []int{5, 25} }},
		{"negative threshold", func(p *Params) { p.FailureThreshold = -0.1 }},
		{"threshold over 1", func(p *Params) { p.FailureThreshold = 1.5 }},
		{"zero window", func(p *Params) { p.HealthWindow = 0 }},
		{"no matching devices", func(p *Params) { p.Target = registry.Selector{MatchLabels: map[string]string{"ring": "nope"}} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			tc.mutate(&p)
			if _, err := e.Create(p); err == nil {
				t.Errorf("Create accepted invalid params: %s", tc.name)
			}
		})
	}
}
