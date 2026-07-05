#!/usr/bin/env bash
# Regenerate Go code from api/fleet.proto.
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc on PATH.
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
set -euo pipefail
cd "$(dirname "$0")/.."

protoc \
  --go_out=. --go_opt=module=github.com/ericsson-kuma/fleet-ctl \
  --go-grpc_out=. --go-grpc_opt=module=github.com/ericsson-kuma/fleet-ctl \
  api/fleet.proto

echo "generated: $(ls api/fleetpb/)"
