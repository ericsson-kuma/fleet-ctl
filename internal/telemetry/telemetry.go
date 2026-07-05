// Package telemetry ingests device reports: it validates them, updates the
// registry, and forwards config-apply acks to whoever enforces rollout
// guardrails (the rollout engine, decoupled behind ApplySink).
package telemetry

import (
	"fmt"
	"time"

	"github.com/ericsson-kuma/fleet-ctl/internal/registry"
)

// ApplyResult is a device's verdict on one config-apply attempt.
type ApplyResult struct {
	Version string
	OK      bool
	Err     string
}

// Report is one telemetry sample from a device.
type Report struct {
	DeviceID       string
	At             time.Time
	Health         registry.Health
	AppliedVersion string
	Apply          *ApplyResult // nil for plain heartbeats
	Metrics        map[string]float64
}

// ApplySink consumes config-apply acks. Implemented by rollout.Engine;
// declared here so telemetry does not depend on the rollout package.
type ApplySink interface {
	HandleApplyResult(deviceID, version string, ok bool, errMsg string)
}

// Ingestor is the single entry point for device telemetry.
type Ingestor struct {
	store registry.Store
	sink  ApplySink // may be nil (e.g. in tools that only mirror state)
}

// NewIngestor wires an ingestor to the store and an optional apply sink.
func NewIngestor(store registry.Store, sink ApplySink) *Ingestor {
	return &Ingestor{store: store, sink: sink}
}

// Ingest validates and applies one report. Reports for unregistered devices
// are rejected — devices must Register before streaming.
func (i *Ingestor) Ingest(r Report) error {
	if r.DeviceID == "" {
		return fmt.Errorf("telemetry: empty device id")
	}
	if err := i.store.RecordTelemetry(r.DeviceID, r.Health, r.AppliedVersion, r.At); err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	if r.Apply != nil && i.sink != nil {
		i.sink.HandleApplyResult(r.DeviceID, r.Apply.Version, r.Apply.OK, r.Apply.Err)
	}
	return nil
}
