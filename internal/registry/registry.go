// Package registry is the control plane's source of truth: the device
// inventory, the catalog of config versions, and the desired-state assignment
// (which config version each device should converge to).
//
// Store is an interface so the in-memory implementation can later be swapped
// for a persistent one (bbolt — see BACKLOG.md) without touching callers.
package registry

import (
	"fmt"
	"sort"
	"time"
)

// Health mirrors the wire enum without importing generated code, keeping
// domain logic independent of the transport layer.
type Health int

const (
	HealthUnknown Health = iota
	HealthHealthy
	HealthDegraded
	HealthOffline
)

func (h Health) String() string {
	switch h {
	case HealthHealthy:
		return "healthy"
	case HealthDegraded:
		return "degraded"
	case HealthOffline:
		return "offline"
	default:
		return "unknown"
	}
}

// Device is the inventory record for one fleet member.
type Device struct {
	ID             string
	Labels         map[string]string
	Model          string
	Health         Health
	AppliedVersion string // last version the device reported as running
	LastSeen       time.Time
}

// Config is a versioned opaque blob. The control plane never parses Blob.
type Config struct {
	Version     string
	Blob        []byte
	Description string
}

// Store is the persistence boundary of the control plane.
type Store interface {
	// UpsertDevice registers or refreshes a device (idempotent).
	UpsertDevice(id string, labels map[string]string, model string, now time.Time) error
	// RecordTelemetry updates health/applied-version/last-seen for a known device.
	RecordTelemetry(id string, h Health, appliedVersion string, now time.Time) error
	Device(id string) (Device, bool)
	// ListDevices returns all devices sorted by ID.
	ListDevices() []Device

	PutConfig(cfg Config) error
	Config(version string) (Config, bool)

	// SetDesired points devices at a config version (must exist in the catalog).
	SetDesired(deviceIDs []string, version string) error
	// Desired resolves the config a device should run: its assignment if any,
	// else the fleet baseline, else ok=false.
	Desired(deviceID string) (Config, bool)
	// SetBaseline sets the fleet-wide default version for devices without an
	// explicit assignment (e.g. new registrations).
	SetBaseline(version string) error
	Baseline() string
}

// ErrUnknownDevice is returned for operations on unregistered device IDs.
type ErrUnknownDevice struct{ ID string }

func (e ErrUnknownDevice) Error() string { return fmt.Sprintf("unknown device %q", e.ID) }

// ErrUnknownConfig is returned when a referenced config version is not in the catalog.
type ErrUnknownConfig struct{ Version string }

func (e ErrUnknownConfig) Error() string { return fmt.Sprintf("unknown config version %q", e.Version) }

// Selector picks devices by labels. All=true matches everything; otherwise a
// device must carry every key=value in MatchLabels.
type Selector struct {
	All         bool
	MatchLabels map[string]string
}

// Matches reports whether the selector selects d.
func (s Selector) Matches(d Device) bool {
	if s.All {
		return true
	}
	if len(s.MatchLabels) == 0 {
		return false
	}
	for k, v := range s.MatchLabels {
		if d.Labels[k] != v {
			return false
		}
	}
	return true
}

// Select returns the IDs of matching devices, sorted for determinism.
func Select(st Store, sel Selector) []string {
	var ids []string
	for _, d := range st.ListDevices() {
		if sel.Matches(d) {
			ids = append(ids, d.ID)
		}
	}
	sort.Strings(ids)
	return ids
}
