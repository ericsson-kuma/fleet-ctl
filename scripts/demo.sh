#!/usr/bin/env bash
# Demo: staged rollout with guardrails, end to end on one machine.
#
#   phase 1  good config v2: canary 5% -> 25% -> 100%, health-gated promotion
#   phase 2  bad config v3: 25% of devices reject it -> guardrail halts the
#            wave and automatically rolls touched devices back to v2
#
# Run via `make demo`. Server/simulator logs land in a temp dir (printed).
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=bin
PORT="${FLEET_PORT:-7443}"
ADDR="127.0.0.1:${PORT}"
LOGDIR="$(mktemp -d -t fleet-demo-XXXX)"

FLEETD_PID=""
SIM_PID=""
cleanup() {
  [ -n "$SIM_PID" ] && kill "$SIM_PID" 2>/dev/null || true
  [ -n "$FLEETD_PID" ] && kill "$FLEETD_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

say() { printf '\n=== %s ===\n' "$*"; }

say "starting fleetd on ${ADDR} + 20 simulated devices (logs: ${LOGDIR})"
"$BIN/fleetd" -listen "$ADDR" >"$LOGDIR/fleetd.log" 2>&1 &
FLEETD_PID=$!
sleep 0.7
"$BIN/fleetsim" -addr "$ADDR" -n 20 -fail-rate 0.25 -fail-version v3 \
  >"$LOGDIR/fleetsim.log" 2>&1 &
SIM_PID=$!
sleep 1.5

say "fleet registered"
"$BIN/fleetctl" devices -addr "$ADDR" | sed -n '1,4p' && echo "  ..."
"$BIN/fleetctl" devices -addr "$ADDR" | tail -1

say "PHASE 1 — good config v2, waves 5% -> 25% -> 100%, 20% failure budget, 2s health window"
"$BIN/fleetctl" rollout -addr "$ADDR" \
  -version v2 -desc "known-good config" -blob '{"telemetry_interval":"30s"}' \
  -target all -waves 5,25,100 -threshold 0.2 -window 2s -watch

say "PHASE 2 — bad config v3 (5 of 20 devices will reject it)"
if "$BIN/fleetctl" rollout -addr "$ADDR" \
  -version v3 -desc "bad config" -blob '{"mtu":90000}' \
  -target all -waves 5,25,100 -threshold 0.2 -window 2s -watch; then
  echo "UNEXPECTED: bad rollout was not halted" >&2
  exit 1
else
  rc=$?
  if [ "$rc" -ne 3 ]; then
    echo "UNEXPECTED: fleetctl exited $rc (want 3 = rolled back)" >&2
    exit "$rc"
  fi
  printf '\nguardrail verdict: exit code 3 (ROLLED_BACK) — halted before fleet-wide damage\n'
fi

sleep 1.5 # let reverted devices converge back

say "fleet after both rollouts (back on v2, v3 contained)"
"$BIN/fleetctl" devices -addr "$ADDR" | sed -n '1,7p' && echo "  ..."
"$BIN/fleetctl" devices -addr "$ADDR" | tail -1

say "demo complete"
