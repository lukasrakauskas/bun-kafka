---
title: Chaos testing handoff
description: Chaos testing handoff
---

## Current state

The working tree contains an uncommitted chaos test implementation. The main suite runs successfully, but it is not a complete release qualification. Do not mark any release gate in [chaos-testing.md](chaos-testing.md) as complete yet.

## Implemented work

- `test/chaos.test.ts`: deterministic mock-broker tests for bootstrap fallback, resets, partial frames, blackholes, delayed responses, leader changes, late responses, broker errors, malformed input, TLS failures, and repeated socket faults.
- `test/chaos-cluster.test.ts`: three-broker tests for leader kill, broker pause, rolling restart, leader transfer, topic recreation, and `tc netem` profiles.
- `test/chaos.compose.yml`: disposable three-node Redpanda cluster.
- `test/chaos.Dockerfile`: Redpanda image with `tc` from `iproute2`.
- `scripts/chaos.ts`: cluster lifecycle, test execution, cleanup, and basic result artifacts in `out/chaos/`.
- `test/fixtures/chaos-*.pem`: synthetic TLS test certificate and private key.
- `package.json`: `test:chaos:mock` and `test:chaos` commands.
- `src/bun/client.ts`: Fetch now uses `requestTimeoutMs + maxWaitMs` instead of a fixed extra 30 seconds. The blackhole test needs this bounded timeout.

## Last verification

These commands passed:

```bash
bun run typecheck
bun run test
CHAOS_FAULT_CYCLES=100 bun run test:chaos:mock
CHAOS_FAULT_CYCLES=10 bun run test:chaos
```

The last Docker run had 18 passing tests and one skipped external expired-certificate test. It included the three `tc netem` profiles. Its result is in `out/chaos/2026-08-24T20-11-40.908Z.json` and is ignored by Git.

The Docker runner defaults to 1,000 fault cycles, but the last full run used 10 cycles. A separate mock run used 100 cycles.

## Unfinished work

### 1. Fix and use the real Admin topic APIs

`Admin.createTopics()` closed the Redpanda connection during manual testing. The cluster suite currently uses `rpk topic create` and `rpk topic delete` as a workaround. Find the request encoding error, add a real-broker regression test, and then remove this workaround.

### 2. Complete TLS fault coverage

The local suite covers an untrusted certificate, a wrong server name, a missing client certificate, and no plaintext fallback. The expired-certificate test uses `expired.badssl.com` and runs only with `CHAOS_TLS_EXTERNAL=1`.

Add local fixtures for these remaining cases:

- Expired certificate
- Server close during the TLS handshake
- Explicit client-certificate rejection reason

Do not make the default suite depend on Internet access.

### 3. Add a physical broker storage fault

The mock suite returns Kafka storage error 56. It does not fill a real disposable broker volume or make it read-only. Add a bounded Docker storage fixture. Do not fill the host filesystem.

### 4. Add a real TCP fault proxy

Produce and Fetch reset tests currently use the Bun mock broker. Add a proxy test against Redpanda for these exact points:

- Reset after a Produce request reaches the broker but before the response is complete
- Reset in the middle of a large Fetch response
- Response truncation at selected byte offsets

### 5. Finish the data oracle

The rolling-restart test checks that acknowledged IDs occur once. It does not classify all required outcomes. Add the full classifications from `chaos-testing.md`, checksums, attempt numbers, per-partition order, and rejected-but-present results.

### 6. Add resource and retry measurements

The repeated fault test checks only the mock broker's active socket count. It does not record process sockets, file descriptors, memory, CPU, queues, pending requests, reconnects, or latency. Run and record all 1,000 cycles after these measurements exist.

The `tc netem` test checks bounded completion only. Add the queue, CPU, reconnect-loop, and latency assertions from the plan.

### 7. Expand result artifacts

`scripts/chaos.ts` records the commit, duration, broker count, cycle count, and pass state. Add:

- Exact fault commands and timestamps
- Broker and leader IDs
- Request completion counts
- Data-oracle counts
- Resource maxima
- Recovery times
- Per-scenario pass or fail reasons

### 8. Qualify recovery levels

The broker-kill and rolling-restart tests create a fresh client after recovery. This verifies application-assisted recovery, not seamless client recovery. Keep Gate C incomplete until the same client passes broker kill, leader transfer, rolling restart, and blackhole tests without recreation.

## Resume commands

```bash
bun run typecheck
bun run test:chaos:mock
bun run test:chaos
```

Use `CHAOS_KEEP=1 bun run test:chaos` to keep the cluster for diagnosis. Clean it with:

```bash
docker compose -f test/chaos.compose.yml -p bun-kafka-chaos down --volumes --remove-orphans
```
