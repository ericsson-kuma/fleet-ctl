// Package server bridges the wire (fleetpb) to the domain packages. It owns
// all proto<->domain conversion so registry/telemetry/rollout stay free of
// generated types.
package server

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ericsson-kuma/fleet-ctl/api/fleetpb"
	"github.com/ericsson-kuma/fleet-ctl/internal/clock"
	"github.com/ericsson-kuma/fleet-ctl/internal/registry"
	"github.com/ericsson-kuma/fleet-ctl/internal/telemetry"
)

// DeviceServer implements fleetpb.DeviceServiceServer.
type DeviceServer struct {
	fleetpb.UnimplementedDeviceServiceServer
	store registry.Store
	ing   *telemetry.Ingestor
	clk   clock.Clock
}

// NewDeviceServer wires the device-facing API to the store and ingest pipeline.
func NewDeviceServer(store registry.Store, ing *telemetry.Ingestor, clk clock.Clock) *DeviceServer {
	return &DeviceServer{store: store, ing: ing, clk: clk}
}

// Register upserts the device and hands back its current desired config.
func (s *DeviceServer) Register(_ context.Context, req *fleetpb.RegisterRequest) (*fleetpb.RegisterResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "device id required")
	}
	if err := s.store.UpsertDevice(req.GetId(), req.GetLabels(), req.GetModel(), s.clk.Now()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &fleetpb.RegisterResponse{}
	if cfg, ok := s.store.Desired(req.GetId()); ok {
		resp.Desired = configToPB(cfg)
	}
	return resp, nil
}

// TelemetryStream consumes a device's report stream until it closes, feeding
// each report through the ingest pipeline (which routes apply acks to the
// rollout guardrail).
func (s *DeviceServer) TelemetryStream(stream fleetpb.DeviceService_TelemetryStreamServer) error {
	var received int32
	for {
		rep, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&fleetpb.TelemetryStreamSummary{ReportsReceived: received})
		}
		if err != nil {
			return err
		}
		if err := s.ing.Ingest(reportFromPB(rep, s.clk.Now())); err != nil {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
		received++
	}
}

// GetAssignment returns the config the device should converge to. An empty
// assignment means the control plane has no opinion yet.
func (s *DeviceServer) GetAssignment(_ context.Context, req *fleetpb.GetAssignmentRequest) (*fleetpb.Assignment, error) {
	if req.GetDeviceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "device id required")
	}
	resp := &fleetpb.Assignment{}
	if cfg, ok := s.store.Desired(req.GetDeviceId()); ok {
		resp.Desired = configToPB(cfg)
	}
	return resp, nil
}

// --- conversions -----------------------------------------------------------

func configToPB(c registry.Config) *fleetpb.Config {
	return &fleetpb.Config{Version: c.Version, Blob: c.Blob, Description: c.Description}
}

func reportFromPB(r *fleetpb.TelemetryReport, now time.Time) telemetry.Report {
	at := now
	if ms := r.GetTimestampUnixMs(); ms != 0 {
		at = time.UnixMilli(ms)
	}
	rep := telemetry.Report{
		DeviceID:       r.GetDeviceId(),
		At:             at,
		Health:         healthFromPB(r.GetHealth()),
		AppliedVersion: r.GetAppliedVersion(),
		Metrics:        r.GetMetrics(),
	}
	if ar := r.GetApplyResult(); ar != nil {
		rep.Apply = &telemetry.ApplyResult{Version: ar.GetVersion(), OK: ar.GetSuccess(), Err: ar.GetError()}
	}
	return rep
}

func healthFromPB(h fleetpb.Health) registry.Health {
	switch h {
	case fleetpb.Health_HEALTH_HEALTHY:
		return registry.HealthHealthy
	case fleetpb.Health_HEALTH_DEGRADED:
		return registry.HealthDegraded
	case fleetpb.Health_HEALTH_OFFLINE:
		return registry.HealthOffline
	default:
		return registry.HealthUnknown
	}
}

func healthToPB(h registry.Health) fleetpb.Health {
	switch h {
	case registry.HealthHealthy:
		return fleetpb.Health_HEALTH_HEALTHY
	case registry.HealthDegraded:
		return fleetpb.Health_HEALTH_DEGRADED
	case registry.HealthOffline:
		return fleetpb.Health_HEALTH_OFFLINE
	default:
		return fleetpb.Health_HEALTH_UNSPECIFIED
	}
}
