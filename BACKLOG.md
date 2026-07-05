# Backlog

Scoped, honest next steps — each one is a real gap, not a wishlist item.

1. **mTLS device identity.** Devices currently assert their own ID. Issue
   per-device certs (bootstrap token → CSR at `Register`), verify the peer
   cert's identity against the claimed device ID, and pin telemetry streams to
   it. Admin API gets its own client-cert class.

2. **bbolt persistence behind `registry.Store`.** The interface is already the
   seam: implement it over bbolt buckets (devices / configs / desired /
   rollout journals) so fleetd restarts don't amnesia the fleet. Rollout
   engine state needs a small WAL-style event journal to resume in-flight
   waves.

3. **Device retry with backoff + jitter.** fleetsim attempts a version once
   per assignment change; a real agent should retry failed applies with
   exponential backoff + jitter and a retry budget, reporting each attempt so
   the wave sees flapping, not silence.

4. **Prometheus metrics.** `/metrics` on fleetd: per-wave ack latency
   histograms, failure-rate gauges, rollout duration, active device count,
   telemetry ingest rate. The guardrail decision points are the obvious spots
   to instrument.

5. **Offline detection.** `LastSeen` is recorded but nothing sweeps it. A
   reaper marking devices `OFFLINE` after N missed heartbeats would let
   `ListDevices` tell the truth and let rollouts exclude dead devices from
   wave denominators (policy decision: exclude vs. count-as-fail).

6. **Delta / content-addressed configs.** Configs ship as whole blobs. For
   large configs, store chunks by hash and let devices fetch only what
   changed; the `Config.version` contract already isolates callers from this.

7. **Admin authn/authz + audit log.** AdminService is unauthenticated.
   Static bearer token first, then OIDC; every `CreateRollout` gets an
   audit record (who, what selector, what budget).

8. **Pause / resume / manual promote.** The state machine is fully automatic.
   Operators want `PauseRollout`, `ResumeRollout`, and `PromoteWave` (skip the
   remaining window) — straightforward transitions to add because the engine
   already serializes everything under one lock.

9. **Richer wave policy.** Percent-only waves are crude: support absolute
   counts (`1, 10, 100%`), label-sharded waves (per site/ring), and a max
   wave duration so a wave that never fills its ack quorum fails fast rather
   than at window end only.

10. **WatchRollout resume tokens.** Watchers get full replay + live tail;
    a reconnecting watcher replays everything. Add event sequence numbers and
    `WatchRollout(from_seq)` for cheap resumption, and cap the in-memory event
    log per rollout.

11. **Health beyond the ack.** Promotion currently weighs only apply acks.
    Fold streamed metrics (e.g. load, crash counters) into the window verdict
    so a config that applies cleanly but degrades the device still trips the
    guardrail.
