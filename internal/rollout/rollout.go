// Package rollout implements the staged config rollout state machine — the
// control loop that takes a config version from canary to full fleet with an
// SRE-style guardrail.
//
// Lifecycle of one rollout:
//
//	Create ──► wave 0 (canary) ──► wave 1 ──► … ──► last wave ──► SUCCEEDED
//	              │                  │                 │
//	              └── guardrail ─────┴─────────────────┘
//	                      │
//	                  HALTED ──► ROLLED_BACK (touched devices reverted)
//
// Each wave targets a cumulative percentage of the selected fleet (e.g.
// 5% → 25% → 100%); the devices *new* in a wave get the config assigned and
// must ack the apply over telemetry. Guardrail semantics, chosen to be exact
// and testable:
//
//   - A wave halts immediately when explicit failures alone push the wave
//     failure rate strictly above FailureThreshold (rate == threshold is
//     tolerated — the threshold is a budget, "at most this much may fail").
//   - A wave is promoted only when its health window elapses. Devices that
//     never acked by then count as failures (silence is not health), and the
//     same strictly-greater test runs once more before promotion.
//   - Halting rolls back every device touched by any started wave to the
//     version that was fleet baseline when the rollout began; devices in
//     never-started waves were never touched — that is the blast radius the
//     staging exists to bound.
//
// All time is injected (internal/clock), so every path above is exercised in
// deterministic unit tests with a fake clock.
package rollout

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ericsson-kuma/fleet-ctl/internal/clock"
	"github.com/ericsson-kuma/fleet-ctl/internal/registry"
)

// State is the rollout state machine's top-level state.
type State int

const (
	StatePending State = iota
	StateInProgress
	StateSucceeded
	StateHalted
	StateRolledBack
)

func (s State) String() string {
	switch s {
	case StatePending:
		return "PENDING"
	case StateInProgress:
		return "IN_PROGRESS"
	case StateSucceeded:
		return "SUCCEEDED"
	case StateHalted:
		return "HALTED"
	case StateRolledBack:
		return "ROLLED_BACK"
	default:
		return "UNKNOWN"
	}
}

// Terminal reports whether no further transitions can occur.
func (s State) Terminal() bool { return s == StateSucceeded || s == StateRolledBack }

// WaveState tracks one wave's progress.
type WaveState int

const (
	WavePending WaveState = iota
	WaveRunning
	WavePromoted
	WaveFailed
)

func (w WaveState) String() string {
	switch w {
	case WavePending:
		return "PENDING"
	case WaveRunning:
		return "RUNNING"
	case WavePromoted:
		return "PROMOTED"
	case WaveFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// Event kinds emitted by the state machine.
const (
	EventCreated      = "CREATED"
	EventWaveStarted  = "WAVE_STARTED"
	EventDeviceOK     = "DEVICE_OK"
	EventDeviceFail   = "DEVICE_FAIL"
	EventWavePromoted = "WAVE_PROMOTED"
	EventHalted       = "HALTED"
	EventRolledBack   = "ROLLED_BACK"
	EventSucceeded    = "SUCCEEDED"
	EventError        = "ERROR"
)

// Event is one entry in a rollout's append-only event log.
type Event struct {
	RolloutID string
	At        time.Time
	Kind      string
	Message   string
	State     State
	Wave      int
}

// Params defines a rollout request.
type Params struct {
	Config registry.Config
	Target registry.Selector
	// WavePercents are cumulative fleet percentages, strictly ascending,
	// each in (0,100], ending at 100. Example: [5, 25, 100].
	WavePercents []int
	// FailureThreshold is the tolerated per-wave failure rate in [0,1].
	// The guardrail halts when a wave's rate strictly exceeds it.
	FailureThreshold float64
	// HealthWindow is how long a wave must soak before promotion.
	HealthWindow time.Duration
}

type wave struct {
	index     int
	percent   int
	targetIDs []string        // devices newly covered by this wave
	results   map[string]bool // deviceID -> ok (first ack wins)
	state     WaveState
}

type rollout struct {
	id     string
	params Params
	state  State
	// previousVersion is the fleet baseline captured at creation; rollback
	// reverts touched devices to it ("" = no baseline: assignments cleared).
	previousVersion string
	targetIDs       []string
	waves           []*wave
	currentWave     int
	waveTimer       clock.Timer
	events          []Event
	subs            []*subscriber
}

type subscriber struct {
	ch     chan Event
	closed bool
}

// WaveView is a read-only snapshot of one wave.
type WaveView struct {
	Index     int
	Percent   int
	TargetIDs []string
	OK        int
	Failed    int
	State     WaveState
}

// View is a read-only snapshot of a rollout.
type View struct {
	ID               string
	ConfigVersion    string
	Description      string
	Target           registry.Selector
	State            State
	CurrentWave      int
	Waves            []WaveView
	PreviousVersion  string
	TargetIDs        []string
	WavePercents     []int
	FailureThreshold float64
	HealthWindow     time.Duration
}

// Engine owns all rollouts. It implements telemetry.ApplySink.
type Engine struct {
	mu       sync.Mutex
	clk      clock.Clock
	store    registry.Store
	rollouts map[string]*rollout
	seq      int
}

// New returns an engine using clk for all timing decisions.
func New(clk clock.Clock, store registry.Store) *Engine {
	return &Engine{clk: clk, store: store, rollouts: make(map[string]*rollout)}
}

// Create validates params, snapshots the target device set, registers the
// config version, and starts wave 0. It fails if no device matches the
// selector — rolling out to nobody is almost certainly an operator error.
func (e *Engine) Create(p Params) (View, error) {
	if err := validate(p); err != nil {
		return View{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	targets := registry.Select(e.store, p.Target)
	if len(targets) == 0 {
		return View{}, errors.New("rollout: selector matches no devices")
	}
	if err := e.store.PutConfig(p.Config); err != nil {
		return View{}, fmt.Errorf("rollout: store config: %w", err)
	}

	e.seq++
	r := &rollout{
		id:              fmt.Sprintf("ro-%d", e.seq),
		params:          p,
		state:           StateInProgress,
		previousVersion: e.store.Baseline(),
		targetIDs:       targets,
	}
	// Cumulative percent -> cumulative device count (ceil, so a small canary
	// percent still covers at least one device). Wave i owns the delta.
	prev := 0
	for i, pct := range p.WavePercents {
		count := (pct*len(targets) + 99) / 100
		if count > len(targets) {
			count = len(targets)
		}
		if count < prev {
			count = prev
		}
		r.waves = append(r.waves, &wave{
			index:     i,
			percent:   pct,
			targetIDs: targets[prev:count],
			results:   make(map[string]bool),
		})
		prev = count
	}
	e.rollouts[r.id] = r

	e.emitLocked(r, EventCreated, fmt.Sprintf("rollout of %s to %d device(s) in %d wave(s), failure budget %.0f%%/wave, health window %s",
		p.Config.Version, len(targets), len(r.waves), p.FailureThreshold*100, p.HealthWindow))
	e.startWaveLocked(r)
	return e.viewLocked(r), nil
}

func validate(p Params) error {
	switch {
	case p.Config.Version == "":
		return errors.New("rollout: config version must be set")
	case len(p.WavePercents) == 0:
		return errors.New("rollout: at least one wave required")
	case p.FailureThreshold < 0 || p.FailureThreshold > 1:
		return errors.New("rollout: failure threshold must be in [0,1]")
	case p.HealthWindow <= 0:
		return errors.New("rollout: health window must be positive")
	}
	last := 0
	for _, pct := range p.WavePercents {
		if pct <= last || pct > 100 {
			return fmt.Errorf("rollout: wave percents must be strictly ascending in (0,100], got %v", p.WavePercents)
		}
		last = pct
	}
	if last != 100 {
		return errors.New("rollout: final wave must be 100%")
	}
	return nil
}

// Get returns a snapshot of a rollout.
func (e *Engine) Get(id string) (View, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.rollouts[id]
	if !ok {
		return View{}, false
	}
	return e.viewLocked(r), true
}

// Watch returns the event log so far plus a live channel. The channel is
// closed when the rollout reaches a terminal state (or cancel is called).
func (e *Engine) Watch(id string) (replay []Event, live <-chan Event, cancel func(), err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.rollouts[id]
	if !ok {
		return nil, nil, nil, fmt.Errorf("rollout: unknown rollout %q", id)
	}
	replay = append([]Event(nil), r.events...)
	sub := &subscriber{ch: make(chan Event, 256)}
	if r.state.Terminal() {
		close(sub.ch)
		sub.closed = true
		return replay, sub.ch, func() {}, nil
	}
	r.subs = append(r.subs, sub)
	cancel = func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		if !sub.closed {
			sub.closed = true
			close(sub.ch)
		}
		for i, s := range r.subs {
			if s == sub {
				r.subs = append(r.subs[:i], r.subs[i+1:]...)
				break
			}
		}
	}
	return replay, sub.ch, cancel, nil
}

// HandleApplyResult ingests one device's config-apply verdict (implements
// telemetry.ApplySink). Acks are attributed to the in-progress rollout whose
// version matches and whose *current* wave targets the device; anything else
// (late acks, re-acks after promotion or rollback, unrelated versions) is
// deliberately ignored — the state machine only moves forward on fresh
// evidence about the wave in flight.
func (e *Engine) HandleApplyResult(deviceID, version string, ok bool, errMsg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range e.rollouts {
		if r.state != StateInProgress || r.params.Config.Version != version {
			continue
		}
		w := r.waves[r.currentWave]
		if !contains(w.targetIDs, deviceID) {
			continue
		}
		if _, seen := w.results[deviceID]; seen {
			continue // first ack wins; duplicates are no-ops
		}
		w.results[deviceID] = ok
		if ok {
			e.emitLocked(r, EventDeviceOK, fmt.Sprintf("%s applied %s (%d/%d ok)", deviceID, version, w.okCount(), len(w.targetIDs)))
		} else {
			e.emitLocked(r, EventDeviceFail, fmt.Sprintf("%s failed to apply %s: %s", deviceID, version, errMsg))
			// Immediate guardrail: explicit failures alone already blow the
			// budget — no point waiting out the window.
			if rate(w.failCount(), len(w.targetIDs)) > r.params.FailureThreshold {
				e.haltAndRollbackLocked(r, fmt.Sprintf("wave %d failure rate %d/%d exceeds threshold %.0f%%",
					w.index, w.failCount(), len(w.targetIDs), r.params.FailureThreshold*100))
			}
		}
		return
	}
}

// startWaveLocked assigns the config to the current wave's devices and arms
// the health-window timer.
func (e *Engine) startWaveLocked(r *rollout) {
	w := r.waves[r.currentWave]
	w.state = WaveRunning
	if len(w.targetIDs) > 0 {
		if err := e.store.SetDesired(w.targetIDs, r.params.Config.Version); err != nil {
			e.emitLocked(r, EventError, fmt.Sprintf("internal: assign wave %d: %v", w.index, err))
		}
	}
	e.emitLocked(r, EventWaveStarted, fmt.Sprintf("wave %d (%d%%): %d device(s) assigned %s, soaking %s",
		w.index, w.percent, len(w.targetIDs), r.params.Config.Version, r.params.HealthWindow))
	idx := w.index
	r.waveTimer = e.clk.AfterFunc(r.params.HealthWindow, func() { e.onWindowExpired(r.id, idx) })
}

// onWindowExpired is the health-window verdict for one wave: count silent
// devices as failures, re-check the budget, then promote or roll back.
func (e *Engine) onWindowExpired(rolloutID string, waveIdx int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.rollouts[rolloutID]
	if !ok || r.state != StateInProgress || r.currentWave != waveIdx {
		return // stale timer: the rollout moved on or halted first
	}
	w := r.waves[waveIdx]
	missing := len(w.targetIDs) - len(w.results)
	if rate(w.failCount()+missing, len(w.targetIDs)) > r.params.FailureThreshold {
		e.haltAndRollbackLocked(r, fmt.Sprintf("wave %d unhealthy at window end: %d failed, %d unresponsive of %d (threshold %.0f%%)",
			w.index, w.failCount(), missing, len(w.targetIDs), r.params.FailureThreshold*100))
		return
	}
	w.state = WavePromoted
	e.emitLocked(r, EventWavePromoted, fmt.Sprintf("wave %d healthy for %s (%d ok, %d failed, %d silent of %d) — promoting",
		w.index, r.params.HealthWindow, w.okCount(), w.failCount(), missing, len(w.targetIDs)))
	if waveIdx == len(r.waves)-1 {
		r.state = StateSucceeded
		if err := e.store.SetBaseline(r.params.Config.Version); err != nil {
			e.emitLocked(r, EventError, fmt.Sprintf("internal: set baseline: %v", err))
		}
		e.emitLocked(r, EventSucceeded, fmt.Sprintf("%s is now fleet baseline (%d device(s))", r.params.Config.Version, len(r.targetIDs)))
		e.closeSubsLocked(r)
		return
	}
	r.currentWave++
	e.startWaveLocked(r)
}

// haltAndRollbackLocked stops the rollout and reverts every touched device.
// It is idempotent: only an in-progress rollout can halt, and both transitions
// happen atomically under the engine lock, so late acks or stale timers after
// the fact are no-ops.
func (e *Engine) haltAndRollbackLocked(r *rollout, reason string) {
	if r.state != StateInProgress {
		return
	}
	if r.waveTimer != nil {
		r.waveTimer.Stop()
		r.waveTimer = nil
	}
	r.waves[r.currentWave].state = WaveFailed
	r.state = StateHalted
	e.emitLocked(r, EventHalted, reason)

	// Blast radius = devices in waves that actually started.
	var touched []string
	for i := 0; i <= r.currentWave; i++ {
		touched = append(touched, r.waves[i].targetIDs...)
	}
	if r.previousVersion != "" {
		if err := e.store.SetDesired(touched, r.previousVersion); err != nil {
			e.emitLocked(r, EventError, fmt.Sprintf("internal: rollback: %v", err))
		}
	} else if err := e.store.ClearDesired(touched); err != nil {
		e.emitLocked(r, EventError, fmt.Sprintf("internal: rollback (clear): %v", err))
	}
	r.state = StateRolledBack
	target := r.previousVersion
	if target == "" {
		target = "unassigned (no prior baseline)"
	}
	e.emitLocked(r, EventRolledBack, fmt.Sprintf("%d touched device(s) reverted to %s; %d device(s) never exposed",
		len(touched), target, len(r.targetIDs)-len(touched)))
	e.closeSubsLocked(r)
}

func (e *Engine) emitLocked(r *rollout, kind, msg string) {
	ev := Event{RolloutID: r.id, At: e.clk.Now(), Kind: kind, Message: msg, State: r.state, Wave: r.currentWave}
	r.events = append(r.events, ev)
	for _, s := range r.subs {
		if s.closed {
			continue
		}
		select {
		case s.ch <- ev:
		default: // slow watcher: drop rather than stall the state machine
		}
	}
}

func (e *Engine) closeSubsLocked(r *rollout) {
	for _, s := range r.subs {
		if !s.closed {
			s.closed = true
			close(s.ch)
		}
	}
	r.subs = nil
}

func (e *Engine) viewLocked(r *rollout) View {
	v := View{
		ID:               r.id,
		ConfigVersion:    r.params.Config.Version,
		Description:      r.params.Config.Description,
		Target:           r.params.Target,
		State:            r.state,
		CurrentWave:      r.currentWave,
		PreviousVersion:  r.previousVersion,
		TargetIDs:        append([]string(nil), r.targetIDs...),
		WavePercents:     append([]int(nil), r.params.WavePercents...),
		FailureThreshold: r.params.FailureThreshold,
		HealthWindow:     r.params.HealthWindow,
	}
	for _, w := range r.waves {
		v.Waves = append(v.Waves, WaveView{
			Index:     w.index,
			Percent:   w.percent,
			TargetIDs: append([]string(nil), w.targetIDs...),
			OK:        w.okCount(),
			Failed:    w.failCount(),
			State:     w.state,
		})
	}
	return v
}

func (w *wave) okCount() int {
	n := 0
	for _, ok := range w.results {
		if ok {
			n++
		}
	}
	return n
}

func (w *wave) failCount() int {
	n := 0
	for _, ok := range w.results {
		if !ok {
			n++
		}
	}
	return n
}

func rate(n, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) / float64(total)
}

func contains(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}
