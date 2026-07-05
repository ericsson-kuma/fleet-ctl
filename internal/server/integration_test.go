package server_test

// End-to-end integration tests: a real gRPC server on an in-process bufconn
// transport, real clients, simulated devices — and a fake clock, so wave
// promotion is driven deterministically while the transport stays genuinely
// asynchronous (acks are synced by polling the admin API, never by sleeping
// for "long enough").

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ericsson-kuma/fleet-ctl/api/fleetpb"
	"github.com/ericsson-kuma/fleet-ctl/internal/clock"
	"github.com/ericsson-kuma/fleet-ctl/internal/registry"
	"github.com/ericsson-kuma/fleet-ctl/internal/rollout"
	"github.com/ericsson-kuma/fleet-ctl/internal/server"
	"github.com/ericsson-kuma/fleet-ctl/internal/telemetry"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type harness struct {
	dev   fleetpb.DeviceServiceClient
	admin fleetpb.AdminServiceClient
	store *registry.InMemory
	clk   *clock.Fake
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	store := registry.NewInMemory()
	clk := clock.NewFake(t0)
	eng := rollout.New(clk, store)
	ing := telemetry.NewIngestor(store, eng)

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	fleetpb.RegisterDeviceServiceServer(gs, server.NewDeviceServer(store, ing, clk))
	fleetpb.RegisterAdminServiceServer(gs, server.NewAdminServer(store, eng))
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
	})
	return &harness{
		dev:   fleetpb.NewDeviceServiceClient(conn),
		admin: fleetpb.NewAdminServiceClient(conn),
		store: store,
		clk:   clk,
	}
}

// device is a minimal in-test fleet member with an open telemetry stream.
type device struct {
	id     string
	stream grpc.ClientStreamingClient[fleetpb.TelemetryReport, fleetpb.TelemetryStreamSummary]
	acked  map[string]bool // versions already acked
}

func (h *harness) spawnFleet(t *testing.T, ctx context.Context, n int) []*device {
	t.Helper()
	// Seed a fleet baseline the way an operator would have before day 2.
	if err := h.store.PutConfig(registry.Config{Version: "v1", Blob: []byte("baseline")}); err != nil {
		t.Fatal(err)
	}
	if err := h.store.SetBaseline("v1"); err != nil {
		t.Fatal(err)
	}
	var devs []*device
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("dev-%02d", i)
		_, err := h.dev.Register(ctx, &fleetpb.RegisterRequest{
			Id: id, Labels: map[string]string{"ring": "prod"}, Model: "sim-ap",
		})
		if err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		stream, err := h.dev.TelemetryStream(ctx)
		if err != nil {
			t.Fatalf("stream %s: %v", id, err)
		}
		devs = append(devs, &device{id: id, stream: stream, acked: map[string]bool{"v1": true}})
	}
	return devs
}

// converge makes every device whose assignment is `version` (and which hasn't
// acked it yet) report an apply verdict; ok(deviceID) decides the verdict.
func converge(t *testing.T, ctx context.Context, h *harness, devs []*device, version string, ok func(string) bool) int {
	t.Helper()
	sent := 0
	for _, d := range devs {
		if d.acked[version] {
			continue
		}
		a, err := h.dev.GetAssignment(ctx, &fleetpb.GetAssignmentRequest{DeviceId: d.id})
		if err != nil {
			t.Fatal(err)
		}
		if a.GetDesired().GetVersion() != version {
			continue // not (yet) targeted by this wave
		}
		verdict := ok(d.id)
		rep := &fleetpb.TelemetryReport{
			DeviceId: d.id,
			Health:   fleetpb.Health_HEALTH_HEALTHY,
			ApplyResult: &fleetpb.ConfigApplyResult{
				Version: version, Success: verdict,
			},
		}
		if verdict {
			rep.AppliedVersion = version
		} else {
			rep.Health = fleetpb.Health_HEALTH_DEGRADED
			rep.ApplyResult.Error = "simulated apply failure"
		}
		if err := d.stream.Send(rep); err != nil {
			t.Fatalf("send %s: %v", d.id, err)
		}
		d.acked[version] = true
		sent++
	}
	return sent
}

// waitFor polls cond (transport is async) with a real-time deadline. The
// rollout clock itself never depends on this — only ack delivery does.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (h *harness) rolloutState(t *testing.T, ctx context.Context, id string) *fleetpb.Rollout {
	t.Helper()
	r, err := h.admin.GetRollout(ctx, &fleetpb.GetRolloutRequest{RolloutId: id})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestIntegrationGoodConfigReachesWholeFleet(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	devs := h.spawnFleet(t, ctx, 8)

	resp, err := h.admin.CreateRollout(ctx, &fleetpb.CreateRolloutRequest{
		Config:           &fleetpb.Config{Version: "v2", Blob: []byte(`{"feature":"on"}`), Description: "good config"},
		Target:           &fleetpb.TargetSelector{All: true},
		WavePercents:     []int32{25, 100},
		FailureThreshold: 0.25,
		HealthWindowMs:   10_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	id := resp.GetRolloutId()

	allOK := func(string) bool { return true }
	// Wave 0: 25% of 8 = 2 devices; wave 1: the remaining 6.
	for wave, wantAcks := range []int{2, 6} {
		waitFor(t, fmt.Sprintf("wave %d assignments", wave), func() bool {
			return converge(t, ctx, h, devs, "v2", allOK) > 0 || allAcked(devs, "v2", cumulative(wave, 2, 6))
		})
		converge(t, ctx, h, devs, "v2", allOK) // catch stragglers in the same wave
		waitFor(t, fmt.Sprintf("wave %d acks ingested", wave), func() bool {
			r := h.rolloutState(t, ctx, id)
			return int(r.GetWaves()[wave].GetAppliedOk()) == wantAcks
		})
		h.clk.Advance(10 * time.Second) // health window elapses -> promote
	}

	final := h.rolloutState(t, ctx, id)
	if final.GetState() != fleetpb.RolloutState_ROLLOUT_STATE_SUCCEEDED {
		t.Fatalf("state = %v, want SUCCEEDED", final.GetState())
	}
	for _, d := range devs {
		a, err := h.dev.GetAssignment(ctx, &fleetpb.GetAssignmentRequest{DeviceId: d.id})
		if err != nil {
			t.Fatal(err)
		}
		if a.GetDesired().GetVersion() != "v2" {
			t.Errorf("%s desired = %q, want v2", d.id, a.GetDesired().GetVersion())
		}
	}
	// The stream summary comes back when a device closes its side.
	sum, err := devs[0].stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if sum.GetReportsReceived() < 1 {
		t.Errorf("stream summary reports = %d, want >= 1", sum.GetReportsReceived())
	}
}

func TestIntegrationBadConfigRollsBackAtCanary(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	devs := h.spawnFleet(t, ctx, 8)

	resp, err := h.admin.CreateRollout(ctx, &fleetpb.CreateRolloutRequest{
		Config:           &fleetpb.Config{Version: "v3", Blob: []byte(`{"mtu":90000}`), Description: "bad config"},
		Target:           &fleetpb.TargetSelector{All: true},
		WavePercents:     []int32{25, 100},
		FailureThreshold: 0.2,
		HealthWindowMs:   10_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	id := resp.GetRolloutId()

	// Watch concurrently: replay + live events until the stream ends at the
	// terminal state.
	events := make(chan []string, 1)
	go func() {
		var kinds []string
		w, err := h.admin.WatchRollout(ctx, &fleetpb.WatchRolloutRequest{RolloutId: id})
		if err == nil {
			for {
				ev, err := w.Recv()
				if errors.Is(err, io.EOF) || err != nil {
					break
				}
				kinds = append(kinds, ev.GetKind())
			}
		}
		events <- kinds
	}()

	// Canary wave = dev-00, dev-01. dev-01 rejects: 1/2 = 50% > 20% budget.
	badDevice := func(id string) bool { return id != "dev-01" }
	waitFor(t, "canary assignments and acks", func() bool {
		converge(t, ctx, h, devs, "v3", badDevice)
		return h.rolloutState(t, ctx, id).GetState() == fleetpb.RolloutState_ROLLOUT_STATE_ROLLED_BACK
	})

	final := h.rolloutState(t, ctx, id)
	w0 := final.GetWaves()[0]
	if w0.GetState() != fleetpb.WaveState_WAVE_STATE_FAILED || w0.GetAppliedFail() != 1 {
		t.Errorf("wave 0 = %v (fail=%d), want FAILED with 1 failure", w0.GetState(), w0.GetAppliedFail())
	}
	// Rollback: canary devices revert to v1; the other 6 were never touched.
	for _, d := range devs {
		a, err := h.dev.GetAssignment(ctx, &fleetpb.GetAssignmentRequest{DeviceId: d.id})
		if err != nil {
			t.Fatal(err)
		}
		if got := a.GetDesired().GetVersion(); got != "v1" {
			t.Errorf("%s desired = %q, want v1 after rollback", d.id, got)
		}
	}

	kinds := <-events
	var haveHalt, haveRollback bool
	waveStarts := 0
	for _, k := range kinds {
		switch k {
		case "HALTED":
			haveHalt = true
		case "ROLLED_BACK":
			haveRollback = true
		case "WAVE_STARTED":
			waveStarts++
		}
	}
	if !haveHalt || !haveRollback {
		t.Errorf("watch events missing guardrail transitions: %v", kinds)
	}
	if waveStarts != 1 {
		t.Errorf("wave starts = %d, want 1 (wave 1 must never start on a halted rollout)", waveStarts)
	}
}

// allAcked reports whether the first `count` devices have acked version.
func allAcked(devs []*device, version string, count int) bool {
	for i := 0; i < count && i < len(devs); i++ {
		if !devs[i].acked[version] {
			return false
		}
	}
	return true
}

// cumulative returns how many devices are covered once `wave` has started,
// given per-wave delta sizes.
func cumulative(wave int, deltas ...int) int {
	n := 0
	for i := 0; i <= wave && i < len(deltas); i++ {
		n += deltas[i]
	}
	return n
}
