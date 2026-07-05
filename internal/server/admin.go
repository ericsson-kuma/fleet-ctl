package server

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ericsson-kuma/fleet-ctl/api/fleetpb"
	"github.com/ericsson-kuma/fleet-ctl/internal/registry"
	"github.com/ericsson-kuma/fleet-ctl/internal/rollout"
)

// AdminServer implements fleetpb.AdminServiceServer.
type AdminServer struct {
	fleetpb.UnimplementedAdminServiceServer
	store registry.Store
	eng   *rollout.Engine
}

// NewAdminServer wires the operator-facing API to the store and rollout engine.
func NewAdminServer(store registry.Store, eng *rollout.Engine) *AdminServer {
	return &AdminServer{store: store, eng: eng}
}

func (s *AdminServer) ListDevices(context.Context, *fleetpb.ListDevicesRequest) (*fleetpb.ListDevicesResponse, error) {
	resp := &fleetpb.ListDevicesResponse{}
	for _, d := range s.store.ListDevices() {
		resp.Devices = append(resp.Devices, &fleetpb.Device{
			Id:             d.ID,
			Labels:         d.Labels,
			Model:          d.Model,
			Health:         healthToPB(d.Health),
			AppliedVersion: d.AppliedVersion,
			LastSeenUnixMs: d.LastSeen.UnixMilli(),
		})
	}
	return resp, nil
}

func (s *AdminServer) CreateRollout(_ context.Context, req *fleetpb.CreateRolloutRequest) (*fleetpb.CreateRolloutResponse, error) {
	if req.GetConfig() == nil {
		return nil, status.Error(codes.InvalidArgument, "config required")
	}
	percents := make([]int, 0, len(req.GetWavePercents()))
	for _, p := range req.GetWavePercents() {
		percents = append(percents, int(p))
	}
	view, err := s.eng.Create(rollout.Params{
		Config: registry.Config{
			Version:     req.GetConfig().GetVersion(),
			Blob:        req.GetConfig().GetBlob(),
			Description: req.GetConfig().GetDescription(),
		},
		Target:           selectorFromPB(req.GetTarget()),
		WavePercents:     percents,
		FailureThreshold: req.GetFailureThreshold(),
		HealthWindow:     time.Duration(req.GetHealthWindowMs()) * time.Millisecond,
	})
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &fleetpb.CreateRolloutResponse{RolloutId: view.ID}, nil
}

func (s *AdminServer) GetRollout(_ context.Context, req *fleetpb.GetRolloutRequest) (*fleetpb.Rollout, error) {
	view, ok := s.eng.Get(req.GetRolloutId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown rollout %q", req.GetRolloutId())
	}
	return rolloutToPB(view), nil
}

// WatchRollout replays the event log, then streams live events until the
// rollout reaches a terminal state or the client goes away.
func (s *AdminServer) WatchRollout(req *fleetpb.WatchRolloutRequest, stream fleetpb.AdminService_WatchRolloutServer) error {
	replay, live, cancel, err := s.eng.Watch(req.GetRolloutId())
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	defer cancel()
	for _, ev := range replay {
		if err := stream.Send(eventToPB(ev)); err != nil {
			return err
		}
	}
	for {
		select {
		case ev, ok := <-live:
			if !ok {
				return nil // terminal state reached
			}
			if err := stream.Send(eventToPB(ev)); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// --- conversions -----------------------------------------------------------

func selectorFromPB(t *fleetpb.TargetSelector) registry.Selector {
	if t == nil {
		return registry.Selector{}
	}
	return registry.Selector{All: t.GetAll(), MatchLabels: t.GetMatchLabels()}
}

func selectorToPB(sel registry.Selector) *fleetpb.TargetSelector {
	return &fleetpb.TargetSelector{All: sel.All, MatchLabels: sel.MatchLabels}
}

func rolloutToPB(v rollout.View) *fleetpb.Rollout {
	r := &fleetpb.Rollout{
		Id:              v.ID,
		Config:          &fleetpb.Config{Version: v.ConfigVersion, Description: v.Description},
		Target:          selectorToPB(v.Target),
		State:           stateToPB(v.State),
		CurrentWave:     int32(v.CurrentWave),
		PreviousVersion: v.PreviousVersion,
		TargetDeviceIds: v.TargetIDs,
	}
	for _, w := range v.Waves {
		r.Waves = append(r.Waves, &fleetpb.Wave{
			Index:       int32(w.Index),
			Percent:     int32(w.Percent),
			TargetCount: int32(len(w.TargetIDs)),
			AppliedOk:   int32(w.OK),
			AppliedFail: int32(w.Failed),
			State:       waveStateToPB(w.State),
		})
	}
	return r
}

func eventToPB(ev rollout.Event) *fleetpb.RolloutEvent {
	return &fleetpb.RolloutEvent{
		RolloutId:       ev.RolloutID,
		TimestampUnixMs: ev.At.UnixMilli(),
		Kind:            ev.Kind,
		Message:         ev.Message,
		State:           stateToPB(ev.State),
		Wave:            int32(ev.Wave),
	}
}

func stateToPB(s rollout.State) fleetpb.RolloutState {
	switch s {
	case rollout.StatePending:
		return fleetpb.RolloutState_ROLLOUT_STATE_PENDING
	case rollout.StateInProgress:
		return fleetpb.RolloutState_ROLLOUT_STATE_IN_PROGRESS
	case rollout.StateSucceeded:
		return fleetpb.RolloutState_ROLLOUT_STATE_SUCCEEDED
	case rollout.StateHalted:
		return fleetpb.RolloutState_ROLLOUT_STATE_HALTED
	case rollout.StateRolledBack:
		return fleetpb.RolloutState_ROLLOUT_STATE_ROLLED_BACK
	default:
		return fleetpb.RolloutState_ROLLOUT_STATE_UNSPECIFIED
	}
}

func waveStateToPB(s rollout.WaveState) fleetpb.WaveState {
	switch s {
	case rollout.WavePending:
		return fleetpb.WaveState_WAVE_STATE_PENDING
	case rollout.WaveRunning:
		return fleetpb.WaveState_WAVE_STATE_RUNNING
	case rollout.WavePromoted:
		return fleetpb.WaveState_WAVE_STATE_PROMOTED
	case rollout.WaveFailed:
		return fleetpb.WaveState_WAVE_STATE_FAILED
	default:
		return fleetpb.WaveState_WAVE_STATE_UNSPECIFIED
	}
}
