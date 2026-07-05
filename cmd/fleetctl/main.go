// fleetctl is the operator CLI for the fleet control plane.
//
//	fleetctl devices  -addr HOST:PORT
//	fleetctl rollout  -addr HOST:PORT -version v2 [-blob s | -blob-file f]
//	                  -target all|k=v[,k=v] -waves 5,25,100
//	                  -threshold 0.2 -window 30s [-watch]
//	fleetctl status   -addr HOST:PORT -id ro-1
//	fleetctl watch    -addr HOST:PORT -id ro-1
//
// Exit code 3 means a watched rollout ended in ROLLED_BACK — scriptable, like
// any good deploy tool.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ericsson-kuma/fleet-ctl/api/fleetpb"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "devices":
		err = cmdDevices(os.Args[2:])
	case "rollout":
		err = cmdRollout(os.Args[2:])
	case "status":
		err = cmdStatus(os.Args[2:])
	case "watch":
		err = cmdWatch(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "fleetctl: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		var exit exitError
		if errors.As(err, &exit) {
			os.Exit(exit.code)
		}
		fmt.Fprintf(os.Stderr, "fleetctl: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `fleetctl — operator CLI for the fleet control plane

commands:
  devices   list registered devices
  rollout   create a staged rollout (optionally -watch it)
  status    show one rollout's waves and state
  watch     stream a rollout's event timeline

run 'fleetctl <command> -h' for flags
`)
}

type exitError struct{ code int }

func (e exitError) Error() string { return fmt.Sprintf("exit %d", e.code) }

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func cmdDevices(args []string) error {
	fs := flag.NewFlagSet("devices", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7443", "fleetd address")
	_ = fs.Parse(args)
	conn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := fleetpb.NewAdminServiceClient(conn).ListDevices(timeoutCtx(), &fleetpb.ListDevicesRequest{})
	if err != nil {
		return err
	}
	fmt.Printf("%-10s %-8s %-9s %-10s %s\n", "DEVICE", "MODEL", "HEALTH", "APPLIED", "LABELS")
	for _, d := range resp.GetDevices() {
		fmt.Printf("%-10s %-8s %-9s %-10s %s\n",
			d.GetId(), d.GetModel(), healthString(d.GetHealth()),
			orDash(d.GetAppliedVersion()), labelString(d.GetLabels()))
	}
	fmt.Printf("%d device(s)\n", len(resp.GetDevices()))
	return nil
}

func cmdRollout(args []string) error {
	fs := flag.NewFlagSet("rollout", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7443", "fleetd address")
	version := fs.String("version", "", "config version (required)")
	blob := fs.String("blob", "", "inline config payload")
	blobFile := fs.String("blob-file", "", "read config payload from file")
	desc := fs.String("desc", "", "human description")
	target := fs.String("target", "all", `target selector: "all" or k=v[,k=v]`)
	waves := fs.String("waves", "5,25,100", "cumulative wave percents")
	threshold := fs.Float64("threshold", 0.2, "per-wave failure budget in [0,1]")
	window := fs.Duration("window", 30*time.Second, "health window per wave")
	watch := fs.Bool("watch", false, "stream the rollout timeline until it finishes")
	_ = fs.Parse(args)
	if *version == "" {
		return errors.New("-version is required")
	}
	payload := []byte(*blob)
	if *blobFile != "" {
		b, err := os.ReadFile(*blobFile)
		if err != nil {
			return err
		}
		payload = b
	}
	var percents []int32
	for _, part := range strings.Split(*waves, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return fmt.Errorf("bad -waves %q: %w", *waves, err)
		}
		percents = append(percents, int32(n))
	}
	sel, err := parseSelector(*target)
	if err != nil {
		return err
	}

	conn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	admin := fleetpb.NewAdminServiceClient(conn)
	resp, err := admin.CreateRollout(timeoutCtx(), &fleetpb.CreateRolloutRequest{
		Config:           &fleetpb.Config{Version: *version, Blob: payload, Description: *desc},
		Target:           sel,
		WavePercents:     percents,
		FailureThreshold: *threshold,
		HealthWindowMs:   window.Milliseconds(),
	})
	if err != nil {
		return err
	}
	fmt.Printf("rollout %s created (config %s, waves %s, budget %.0f%%/wave, window %s)\n",
		resp.GetRolloutId(), *version, *waves, *threshold*100, *window)
	if !*watch {
		return nil
	}
	return watchRollout(admin, resp.GetRolloutId())
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7443", "fleetd address")
	id := fs.String("id", "", "rollout id (required)")
	_ = fs.Parse(args)
	if *id == "" {
		return errors.New("-id is required")
	}
	conn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	r, err := fleetpb.NewAdminServiceClient(conn).GetRollout(timeoutCtx(), &fleetpb.GetRolloutRequest{RolloutId: *id})
	if err != nil {
		return err
	}
	fmt.Printf("rollout %s  config=%s  state=%s  targets=%d  previous=%s\n",
		r.GetId(), r.GetConfig().GetVersion(), stateString(r.GetState()),
		len(r.GetTargetDeviceIds()), orDash(r.GetPreviousVersion()))
	fmt.Printf("%-6s %-8s %-8s %-4s %-5s %s\n", "WAVE", "PERCENT", "DEVICES", "OK", "FAIL", "STATE")
	for _, w := range r.GetWaves() {
		fmt.Printf("%-6d %-8s %-8d %-4d %-5d %s\n",
			w.GetIndex(), fmt.Sprintf("%d%%", w.GetPercent()), w.GetTargetCount(),
			w.GetAppliedOk(), w.GetAppliedFail(), waveString(w.GetState()))
	}
	return nil
}

func cmdWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7443", "fleetd address")
	id := fs.String("id", "", "rollout id (required)")
	_ = fs.Parse(args)
	if *id == "" {
		return errors.New("-id is required")
	}
	conn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	return watchRollout(fleetpb.NewAdminServiceClient(conn), *id)
}

// watchRollout prints the event timeline until the rollout is terminal.
func watchRollout(admin fleetpb.AdminServiceClient, id string) error {
	stream, err := admin.WatchRollout(context.Background(), &fleetpb.WatchRolloutRequest{RolloutId: id})
	if err != nil {
		return err
	}
	last := fleetpb.RolloutState_ROLLOUT_STATE_UNSPECIFIED
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if last == fleetpb.RolloutState_ROLLOUT_STATE_ROLLED_BACK {
				return exitError{code: 3}
			}
			return nil
		}
		if err != nil {
			return err
		}
		last = ev.GetState()
		ts := time.UnixMilli(ev.GetTimestampUnixMs()).Format("15:04:05.000")
		fmt.Printf("%s  [%s] %-13s %s\n", ts, ev.GetRolloutId(), ev.GetKind(), ev.GetMessage())
	}
}

// --- helpers ----------------------------------------------------------------

func timeoutCtx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = cancel // rpc finishes well within; contexts are per-call
	return ctx
}

func parseSelector(s string) (*fleetpb.TargetSelector, error) {
	if s == "all" {
		return &fleetpb.TargetSelector{All: true}, nil
	}
	labels := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok || k == "" {
			return nil, fmt.Errorf(`bad -target %q (want "all" or k=v[,k=v])`, s)
		}
		labels[k] = v
	}
	return &fleetpb.TargetSelector{MatchLabels: labels}, nil
}

func healthString(h fleetpb.Health) string {
	switch h {
	case fleetpb.Health_HEALTH_HEALTHY:
		return "healthy"
	case fleetpb.Health_HEALTH_DEGRADED:
		return "degraded"
	case fleetpb.Health_HEALTH_OFFLINE:
		return "offline"
	default:
		return "unknown"
	}
}

func stateString(s fleetpb.RolloutState) string {
	return strings.TrimPrefix(s.String(), "ROLLOUT_STATE_")
}

func waveString(s fleetpb.WaveState) string {
	return strings.TrimPrefix(s.String(), "WAVE_STATE_")
}

func labelString(m map[string]string) string {
	if len(m) == 0 {
		return "-"
	}
	var parts []string
	for k, v := range m {
		parts = append(parts, k+"="+v)
	}
	// small maps; ordering jitter is fine for humans, sort for stable demos
	for i := 0; i < len(parts); i++ {
		for j := i + 1; j < len(parts); j++ {
			if parts[j] < parts[i] {
				parts[i], parts[j] = parts[j], parts[i]
			}
		}
	}
	return strings.Join(parts, ",")
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
