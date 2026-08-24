# Production readiness

**Status:** usable for internal / low-risk produce–consume paths.  
**Not** a full drop-in replacement for battle-tested clients (franz-go, librdkafka C/C++, confluent-kafka) in every production shape.

This document states what is safe today, what is missing, and how to run bun-kafka if you still ship it.

## Summary

| Posture | Verdict |
|---------|---------|
| Single-service producer → topic | Reasonable with caveats below |
| Single consumer group, manual commit, reprocess-OK | Reasonable with caveats |
| Multi-instance consumer group, custom rebalance | **Not ready** |
| Strict per-message delivery tracking | **Not ready** (no delivery reports) |
| Multi-tenant / public library consumers | **Not ready** |
| Long-lived prod default on `bun:ffi` | **Avoid** — prefer NAPI |

Built on **librdkafka** (protocol, fetch batching, broker negotiation). The JS layer is thin; production risk is mostly **lifecycle, ops, and missing callbacks**, not “fake Kafka”.

## What works well enough

- Produce: `send` / `sendBatch`, keys, values, headers, partition, timestamp, `flush`
- Consume: subscribe, assign, `poll` / `pollBatch`, `messages()`, `batches()`
- Commits: sync/async, `commitMessage`, batch commit helpers
- Offsets: seek, position, committed, storeOffsets, watermarks, offsetsForTimes
- Pause / resume
- Admin: cluster metadata, cluster id
- Config: standard librdkafka property names
- Backends: `ffi` (default) and `napi` (`useNative`)
- Feature tests: `test/features.test.ts` exercises the public API surface

Performance (warm local broker) is roughly in line with **rdkafka-rust** on produce; consume is still limited by high-level `consumer_poll` (one message per poll under the hood). See [PERF notes](../native/build/PERF.md) when present.

## Must-fix before production

### 1. No rebalance callbacks

There is no application hook for partition assign/revoke.

**Risk:** multi-instance consumer groups can duplicate work, commit wrongly, or hold stale assignment assumptions when membership changes.

**Mitigations today:**

- Prefer **one consumer instance per group** until rebalance handling exists, or
- Accept **at-least-once + idempotent handlers**, and
- Do not build custom cooperative-assignor logic on this client yet.

### 2. No delivery report callbacks

Producer durability is effectively **“queue drained via `flush`”**, not per-message ack callbacks.

**Risk:** you cannot classify individual message failures without treating `flush` / errors as the boundary.

**Mitigations today:**

- Call `await producer.flush(timeout)` before success is reported upstream.
- Use `acks=all` (and consider `enable.idempotence=true`) via config.
- Keep produce concurrency bounded; handle flush timeout as failure.

### 3. Prefer NAPI over FFI for long-lived processes

`bun:ffi` is convenient and fast to iterate, but Bun documents FFI as experimental.

**Production default recommendation:**

```ts
import { useNative } from "bun-kafka";
await useNative("napi"); // bun run build:napi
```

Ship a prebuilt `native/build/bun_kafka_native.node` (or build in the image) so runtime hosts do not need an ad-hoc toolchain surprise.

### 4. Zero-copy payload lifetime (FFI)

On the FFI backend, `msg.key` / `msg.value` may be **views into librdkafka memory** until `msg.done()`.

After:

- `msg.done()`, or
- return from a `batches()` iteration body (library calls `done()` for you),

those buffers must not be used.

**Mitigation:**

```ts
const value = Buffer.from(msg.value!); // own a copy if it escapes the loop
```

### 5. Shutdown is on you

There is no built-in SIGTERM helper or `await using` integration.

**Mitigation:**

```ts
async function shutdown() {
  await consumer.close();
  await producer.flush(10_000);
  await producer.close();
  process.exit(0);
}
process.on("SIGTERM", () => { void shutdown(); });
process.on("SIGINT", () => { void shutdown(); });
```

Expect `close()` to block on group leave / coordinator in bad network conditions; use a hard deadline in the orchestrator (Kubernetes `terminationGracePeriodSeconds`, etc.).

### 6. Observability is minimal

No first-class stats, log, or error callback surface beyond thrown `KafkaError`.

**Mitigation:**

- Log `KafkaError` fields: `code`, `message`, `fatal`, `retriable`.
- Rely on broker/redpanda metrics and process-level metrics you add around send/poll loops.
- Optionally enable librdkafka `debug` conf only in non-prod (very noisy).

### 7. Packaging and supply chain are early

Current package signals (`0.1.0`, TypeScript source export, no LICENSE in-tree, no CI matrix documented here) mean:

- Pin **bun-kafka commit** and **librdkafka version** in your image.
- Do not assume semver stability yet.
- Add your own CI: ffi + napi × one Linux × one Kafka/Redpanda.

### 8. Failure modes under-tested

Happy-path and API surface tests exist. There is limited automated chaos for:

- broker restart mid-flush / mid-consume
- prolonged GC / network stalls
- auth failures (SSL/SASL misconfig)
- partition leadership moves under load

Run those in **your** staging before relying on SLOs.

## Should-fix soon

| Item | Why |
|------|-----|
| NAPI as documented production backend + prebuild | Stable native boundary |
| Fatal error checks (`fatal` flag / stop-the-world) | Avoid running after unrecoverable client state |
| Explicit produce queue-full / backpressure policy | Prevent tight retry spins |
| SSL/SASL conf examples + one integration test | Real clusters are authenticated |
| CI matrix (OS, librdkafka, backend, broker) | Regressions catch before prod |
| LICENSE + versioning policy | Legal and upgrade clarity |
| Windows stance (support or “unsupported”) | Avoid silent breakage |

## Nice-to-have (not blockers for many apps)

- Admin CRUD (create/delete topics, ACLs, configs)
- Transactions / full EOS helpers
- Delivery report API
- Rebalance callbacks / incremental assign helpers
- Schema Registry helpers
- OpenTelemetry hooks
- Typed config keys (instead of free-form librdkafka strings)

## Recommended production posture (if you ship now)

1. **Backend:** `await useNative("napi")` with a built addon in the image.  
2. **Roles:** separate producer and consumer processes when possible.  
3. **Producer config (starting point):**

   ```ts
   new Producer({
     "bootstrap.servers": process.env.KAFKA_BROKERS!,
     acks: "all",
     "enable.idempotence": true,
     "linger.ms": 5,
     "message.timeout.ms": 120_000,
   })
   ```

4. **Consumer config (starting point):**

   ```ts
   new Consumer({
     "bootstrap.servers": process.env.KAFKA_BROKERS!,
     "group.id": "my-service",
     "enable.auto.commit": false,
     "auto.offset.reset": "earliest",
   })
   ```

5. **Commit only after** side effects are durable (DB write, etc.).  
6. **Copy** key/value if retained beyond the poll/batch body.  
7. **Flush** before process exit and before ack to upstream protocols.  
8. **One live member** per group (or accept rebalance risk).  
9. **Pin** librdkafka and bun-kafka versions in deployment artifacts.  
10. **Load-test** your topic shape (size, partitions, lag) on staging.

### Consume loop sketch

```ts
await useNative("napi");

const consumer = new Consumer({ /* ... */ });
consumer.subscribe("orders");

for await (const batch of consumer.batches({ batchSize: 512, timeoutMs: 100 })) {
  for (const msg of batch) {
    const value = Buffer.from(msg.value ?? []);
    await handle(value); // idempotent handler
  }
  // commits last offset+1 per partition in the batch when eachBatchCommit: true
}
```

Or manual commit:

```ts
for await (const msg of consumer.messages({ timeoutMs: 100 })) {
  await handle(Buffer.from(msg.value ?? []));
  consumer.commitMessage(msg);
}
```

## Security

Pass SSL/SASL through librdkafka config (not wrapped APIs):

```ts
new Producer({
  "bootstrap.servers": "kafka:9093",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "SCRAM-SHA-512",
  "sasl.username": process.env.KAFKA_USER!,
  "sasl.password": process.env.KAFKA_PASS!,
  // "ssl.ca.location": "/etc/ssl/certs/ca.pem",
})
```

Validate in staging with your exact broker. This repo does not yet ship dedicated auth integration tests.

## Error handling

```ts
import { KafkaError } from "bun-kafka";

try {
  await producer.flush(30_000);
} catch (e) {
  if (e instanceof KafkaError) {
    // e.code, e.message, e.fatal, e.retriable
    if (e.fatal) process.exit(1);
  }
  throw e;
}
```

Treat unknown errors as fatal to your task; retry only when `retriable` is true **and** your side effects are safe.

## Environment

| Variable | Purpose |
|----------|---------|
| `KAFKA_BROKERS` | Bootstrap servers (apps/tests/benches) |
| `BUN_KAFKA_NATIVE` | `ffi` or `napi` |
| `LIBRDKAFKA_PATH` | Override shared library path |
| `NODE_API_INCLUDE` | `node_api.h` location for NAPI builds |

## Version support

Broker compatibility follows **librdkafka** ApiVersion negotiation. See root [README](../README.md#supported-kafka--broker-versions).

Practical target: **Kafka 2.8+** or current **Redpanda**, with a **pinned librdkafka 2.x** in production images.

## Decision checklist

Ship only if you can tick these:

- [ ] NAPI backend built and loaded in the deploy image  
- [ ] librdkafka version pinned  
- [ ] Produce path calls `flush` at a clear durability boundary  
- [ ] Consume handlers are idempotent (at-least-once)  
- [ ] Payload copies used when data escapes the poll loop  
- [ ] Graceful shutdown calls `close` / `flush` with an orchestrator hard limit  
- [ ] Staging test: broker restart during produce and consume  
- [ ] Staging test: auth (if cluster requires it)  
- [ ] Alerting on consumer lag and process crash loops  
- [ ] Accepted limitation: no custom rebalance callbacks yet  

If any box is blocked by a missing library feature, either wait for that feature or use another client for that workload.

## Priority roadmap (library)

1. Rebalance-safe consumer story (callbacks or documented single-member mode + tests)  
2. NAPI default for production docs + prebuild/install path  
3. Delivery reports **or** stronger documented flush durability contract + tests  
4. Fatal error + log/stats hooks  
5. CI matrix + LICENSE + release process  
6. Chaos tests (broker kill, partition moves)  

## Related

- [README](../README.md) — features matrix, Kafka versions, API overview  
- `test/features.test.ts` — public API coverage  
- `native/build/PERF.md` — performance notes (when generated)  
