// fleetd is the fleet control-plane server: it hosts the device-facing
// DeviceService (registration, telemetry, config assignment) and the
// operator-facing AdminService (inventory, staged rollouts).
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/ericsson-kuma/fleet-ctl/api/fleetpb"
	"github.com/ericsson-kuma/fleet-ctl/internal/clock"
	"github.com/ericsson-kuma/fleet-ctl/internal/registry"
	"github.com/ericsson-kuma/fleet-ctl/internal/rollout"
	"github.com/ericsson-kuma/fleet-ctl/internal/server"
	"github.com/ericsson-kuma/fleet-ctl/internal/telemetry"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7443", "address to serve gRPC on")
	flag.Parse()

	store := registry.NewInMemory()
	clk := clock.Real{}
	eng := rollout.New(clk, store)
	ing := telemetry.NewIngestor(store, eng)

	gs := grpc.NewServer()
	fleetpb.RegisterDeviceServiceServer(gs, server.NewDeviceServer(store, ing, clk))
	fleetpb.RegisterAdminServiceServer(gs, server.NewAdminServer(store, eng))
	reflection.Register(gs)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("fleetd: listen %s: %v", *listen, err)
	}
	log.Printf("fleetd: serving gRPC on %s", lis.Addr())

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		s := <-sig
		log.Printf("fleetd: %v received, draining", s)
		gs.GracefulStop()
	}()

	if err := gs.Serve(lis); err != nil {
		log.Fatalf("fleetd: serve: %v", err)
	}
	log.Print("fleetd: bye")
}
