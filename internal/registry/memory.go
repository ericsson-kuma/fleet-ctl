package registry

import (
	"maps"
	"sort"
	"sync"
	"time"
)

// InMemory is a mutex-guarded Store. All returned values are copies; callers
// never share memory with the store.
type InMemory struct {
	mu       sync.RWMutex
	devices  map[string]*Device
	configs  map[string]Config
	desired  map[string]string // deviceID -> version
	baseline string
}

// NewInMemory returns an empty in-memory store.
func NewInMemory() *InMemory {
	return &InMemory{
		devices: make(map[string]*Device),
		configs: make(map[string]Config),
		desired: make(map[string]string),
	}
}

var _ Store = (*InMemory)(nil)

func (s *InMemory) UpsertDevice(id string, labels map[string]string, model string, now time.Time) error {
	if id == "" {
		return ErrUnknownDevice{ID: id}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.devices[id]
	if !ok {
		d = &Device{ID: id, Health: HealthUnknown}
		s.devices[id] = d
	}
	d.Labels = maps.Clone(labels)
	d.Model = model
	d.LastSeen = now
	return nil
}

func (s *InMemory) RecordTelemetry(id string, h Health, appliedVersion string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.devices[id]
	if !ok {
		return ErrUnknownDevice{ID: id}
	}
	d.Health = h
	if appliedVersion != "" {
		d.AppliedVersion = appliedVersion
	}
	d.LastSeen = now
	return nil
}

func (s *InMemory) Device(id string) (Device, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.devices[id]
	if !ok {
		return Device{}, false
	}
	return copyDevice(d), true
}

func (s *InMemory) ListDevices() []Device {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Device, 0, len(s.devices))
	for _, d := range s.devices {
		out = append(out, copyDevice(d))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *InMemory) PutConfig(cfg Config) error {
	if cfg.Version == "" {
		return ErrUnknownConfig{Version: cfg.Version}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg.Blob = append([]byte(nil), cfg.Blob...)
	s.configs[cfg.Version] = cfg
	return nil
}

func (s *InMemory) Config(version string) (Config, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := s.configs[version]
	if ok {
		cfg.Blob = append([]byte(nil), cfg.Blob...)
	}
	return cfg, ok
}

func (s *InMemory) SetDesired(deviceIDs []string, version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.configs[version]; !ok {
		return ErrUnknownConfig{Version: version}
	}
	for _, id := range deviceIDs {
		if _, ok := s.devices[id]; !ok {
			return ErrUnknownDevice{ID: id}
		}
	}
	for _, id := range deviceIDs {
		s.desired[id] = version
	}
	return nil
}

func (s *InMemory) Desired(deviceID string) (Config, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	version, ok := s.desired[deviceID]
	if !ok {
		version = s.baseline
	}
	if version == "" {
		return Config{}, false
	}
	cfg, ok := s.configs[version]
	if !ok {
		return Config{}, false
	}
	cfg.Blob = append([]byte(nil), cfg.Blob...)
	return cfg, true
}

func (s *InMemory) SetBaseline(version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.configs[version]; !ok {
		return ErrUnknownConfig{Version: version}
	}
	s.baseline = version
	return nil
}

func (s *InMemory) Baseline() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.baseline
}

func copyDevice(d *Device) Device {
	c := *d
	c.Labels = maps.Clone(d.Labels)
	return c
}
