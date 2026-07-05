package telemetry

import (
	"testing"
	"time"

	"github.com/ericsson-kuma/fleet-ctl/internal/registry"
)

type sinkCall struct {
	device, version, errMsg string
	ok                      bool
}

type fakeSink struct{ calls []sinkCall }

func (f *fakeSink) HandleApplyResult(deviceID, version string, ok bool, errMsg string) {
	f.calls = append(f.calls, sinkCall{deviceID, version, errMsg, ok})
}

var now = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestIngestUpdatesStore(t *testing.T) {
	s := registry.NewInMemory()
	if err := s.UpsertDevice("d1", nil, "m", now); err != nil {
		t.Fatal(err)
	}
	ing := NewIngestor(s, nil)

	err := ing.Ingest(Report{DeviceID: "d1", At: now.Add(time.Second), Health: registry.HealthDegraded, AppliedVersion: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	d, _ := s.Device("d1")
	if d.Health != registry.HealthDegraded || d.AppliedVersion != "v1" || !d.LastSeen.Equal(now.Add(time.Second)) {
		t.Errorf("store not updated: %+v", d)
	}
}

func TestIngestForwardsApplyAcksOnly(t *testing.T) {
	s := registry.NewInMemory()
	if err := s.UpsertDevice("d1", nil, "m", now); err != nil {
		t.Fatal(err)
	}
	sink := &fakeSink{}
	ing := NewIngestor(s, sink)

	// Plain heartbeat: no sink call.
	if err := ing.Ingest(Report{DeviceID: "d1", At: now, Health: registry.HealthHealthy}); err != nil {
		t.Fatal(err)
	}
	if len(sink.calls) != 0 {
		t.Fatalf("heartbeat reached sink: %+v", sink.calls)
	}
	// Ack: forwarded verbatim.
	err := ing.Ingest(Report{
		DeviceID: "d1", At: now, Health: registry.HealthDegraded,
		Apply: &ApplyResult{Version: "v2", OK: false, Err: "checksum mismatch"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := sinkCall{"d1", "v2", "checksum mismatch", false}
	if len(sink.calls) != 1 || sink.calls[0] != want {
		t.Fatalf("sink calls = %+v, want [%+v]", sink.calls, want)
	}
}

func TestIngestRejectsUnknownDevice(t *testing.T) {
	s := registry.NewInMemory()
	sink := &fakeSink{}
	ing := NewIngestor(s, sink)
	err := ing.Ingest(Report{DeviceID: "ghost", At: now, Apply: &ApplyResult{Version: "v2", OK: true}})
	if err == nil {
		t.Fatal("want error for unregistered device")
	}
	if len(sink.calls) != 0 {
		t.Fatal("rejected report must not reach the sink")
	}
}
