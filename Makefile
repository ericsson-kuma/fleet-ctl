GO   ?= go
BIN  := bin

.PHONY: all build test race vet proto demo clean

all: vet test build

build:
	$(GO) build -o $(BIN)/ ./cmd/...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

# Regenerate api/fleetpb from api/fleet.proto (generated code is committed).
proto:
	./scripts/genproto.sh

# End-to-end demo: fleetd + 20 simulated devices, a good rollout that reaches
# the whole fleet, then a bad one the guardrail halts and rolls back.
demo: build
	./scripts/demo.sh

clean:
	rm -rf $(BIN)
