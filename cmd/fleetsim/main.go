// fleetsim simulates a fleet of devices against a running fleetd: each device
// registers, streams telemetry heartbeats, polls its config assignment, and
// applies new versions — deterministically failing a designated bad version at
// a chosen rate, which is what gives the rollout guardrail something to catch.
//
//	fleetsim -addr 127.0.0.1:7443 -n 20 -fail-rate 0.25 -fail-version v3
//
// Failures are spread deterministically across the fleet (Bresenham-style),
// so a given -n / -fail-rate always produces the same failing devices and the
// demo timeline is reproducible.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ericsson-kuma/fleet-ctl/api/fleetpb"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7443", "fleetd address")
	n := flag.Int("n", 20, "number of simulated devices")
	failRate := flag.Float64("fail-rate", 0.25, "fraction of devices that reject -fail-version")
	failVersion := flag.String("fail-version", "v3", "config version the failing devices reject")
	heartbeat := flag.Duration("heartbeat", 1*time.Second, "telemetry heartbeat interval")
	poll := flag.Duration("poll", 300*time.Millisecond, "assignment poll interval")
	labels := flag.String("labels", "ring=prod", "labels for every device (k=v[,k=v])")
	model := flag.String("model", "sim-ap", "device model string")
	flag.Parse()

	lbls := map[string]string{}
	for _, pair := range strings.Split(*labels, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(pair), "="); ok && k != "" {
			lbls[k] = v
		}
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("fleetsim: %v", err)
	}
	defer conn.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	failers := 0
	var wg sync.WaitGroup
	for i := 0; i < *n; i++ {
		d := &device{
			id:          fmt.Sprintf("dev-%02d", i),
			labels:      lbls,
			model:       *model,
			rejects:     failsAt(i, *failRate),
			failVersion: *failVersion,
			applied:     "factory",
			client:      fleetpb.NewDeviceServiceClient(conn),
			heartbeat:   *heartbeat,
			poll:        *poll,
		}
		if d.rejects {
			failers++
		}
		wg.Add(1)
		go func() { defer wg.Done(); d.run(ctx) }()
	}
	log.Printf("fleetsim: %d device(s) up; %d will reject %s (fail-rate %.2f)", *n, failers, *failVersion, *failRate)
	wg.Wait()
	log.Print("fleetsim: all devices stopped")
}

// failsAt spreads failures evenly and deterministically: device i fails iff
// the integer count of expected failures increases at i.
func failsAt(i int, rate float64) bool {
	return int(float64(i+1)*rate) > int(float64(i)*rate)
}

type device struct {
	id          string
	labels      map[string]string
	model       string
	rejects     bool
	failVersion string
	applied     string
	lastAttempt string
	health      fleetpb.Health
	client      fleetpb.DeviceServiceClient
	stream      grpc.ClientStreamingClient[fleetpb.TelemetryReport, fleetpb.TelemetryStreamSummary]
	heartbeat   time.Duration
	poll        time.Duration
}

func (d *device) run(ctx context.Context) {
	d.health = fleetpb.Health_HEALTH_HEALTHY
	resp, err := d.client.Register(ctx, &fleetpb.RegisterRequest{Id: d.id, Labels: d.labels, Model: d.model})
	if err != nil {
		log.Printf("%s: register: %v", d.id, err)
		return
	}
	// Stream outlives ctx cancellation just long enough to close cleanly.
	stream, err := d.client.TelemetryStream(context.Background())
	if err != nil {
		log.Printf("%s: stream: %v", d.id, err)
		return
	}
	d.stream = stream
	d.maybeApply(resp.GetDesired()) // converge to baseline at boot, if any

	pollT := time.NewTicker(d.poll)
	hbT := time.NewTicker(d.heartbeat)
	defer pollT.Stop()
	defer hbT.Stop()
	for {
		select {
		case <-ctx.Done():
			if _, err := d.stream.CloseAndRecv(); err != nil && ctx.Err() == nil {
				log.Printf("%s: close: %v", d.id, err)
			}
			return
		case <-pollT.C:
			a, err := d.client.GetAssignment(ctx, &fleetpb.GetAssignmentRequest{DeviceId: d.id})
			if err != nil {
				continue // control plane briefly away; keep trying
			}
			d.maybeApply(a.GetDesired())
		case <-hbT.C:
			d.send(&fleetpb.TelemetryReport{
				DeviceId:       d.id,
				Health:         d.health,
				AppliedVersion: d.applied,
				Metrics:        map[string]float64{"load1": rand.Float64() * 2},
			})
		}
	}
}

// maybeApply converges toward the desired config: apply at most once per
// desired version change (retry/backoff is a real device's job — BACKLOG).
func (d *device) maybeApply(cfg *fleetpb.Config) {
	if cfg == nil || cfg.GetVersion() == "" || cfg.GetVersion() == d.applied || cfg.GetVersion() == d.lastAttempt {
		return
	}
	version := cfg.GetVersion()
	d.lastAttempt = version
	time.Sleep(30*time.Millisecond + rand.N(90*time.Millisecond)) // apply takes a moment

	if d.rejects && version == d.failVersion {
		log.Printf("%s: apply %s FAILED (simulated device fault)", d.id, version)
		d.send(&fleetpb.TelemetryReport{
			DeviceId:       d.id,
			Health:         fleetpb.Health_HEALTH_DEGRADED,
			AppliedVersion: d.applied, // still running the old version
			ApplyResult: &fleetpb.ConfigApplyResult{
				Version: version, Success: false, Error: "simulated apply failure: config rejected by device",
			},
		})
		return // rejection is transient: device keeps running the old config
	}
	d.applied = version
	d.health = fleetpb.Health_HEALTH_HEALTHY
	log.Printf("%s: applied %s", d.id, version)
	d.send(&fleetpb.TelemetryReport{
		DeviceId:       d.id,
		Health:         d.health,
		AppliedVersion: d.applied,
		ApplyResult:    &fleetpb.ConfigApplyResult{Version: version, Success: true},
	})
}

func (d *device) send(rep *fleetpb.TelemetryReport) {
	rep.TimestampUnixMs = time.Now().UnixMilli()
	if err := d.stream.Send(rep); err != nil {
		log.Printf("%s: telemetry send: %v", d.id, err)
	}
}
