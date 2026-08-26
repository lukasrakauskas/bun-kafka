# Observability and operations

## Stats

```ts
const kafka = new Kafka({
  brokers: ["127.0.0.1:9092"],
  statsIntervalMs: 10_000,
  onEvent: (event) => {
    if (event.type === "stats") console.log(event.stats);
    if (event.type === "retry") console.warn("retrying:", event.error, event.attempt);
    if (event.type === "throttle") console.warn("throttled", event.durationMs);
  },
});

console.log(kafka.stats());
// { connections, requests, bytesSent, bytesReceived, retries, throttles, throttleTimeMs }
```

Counters are cumulative since client creation. Ship them to your metrics system on the interval;
derive rates and error ratios there. Histograms are deliberately left to your metrics stack (see
the [gap audit](../client-gap-audit.md)).

## Health checks

`healthCheck()` pings every known broker with an ApiVersions request:

```ts
const report = await kafka.healthCheck();
// { brokers: [{ address, brokerId?, ok, latencyMs, error? }] }
report.ok; // convenience: all brokers fine
```

Wire this into Kubernetes liveness/readiness or a `/healthz` route.

## Logging hooks

```ts
const kafka = new Kafka({
  brokers: ["127.0.0.1:9092"],
  logger: {
    warn: (m) => myLogger.warn(m),
    info: (m) => myLogger.info(m),
  },
});
```

Retry decisions, throttling, and authentication events flow through these hooks — bridge them
into OpenTelemetry logs if you use it.

## Graceful shutdown

```ts
import { installShutdown } from "bun-kafka";

installShutdown([producer, consumer, kafka]);
```

On SIGTERM/SIGINT each closer is flushed then closed within `timeoutMs` (default 10 s), then the
process exits 0. Producers flush queued batches so nothing in flight is lost; consumers send a
best-effort LeaveGroup so rebalancing starts immediately instead of after the session timeout.

## Operational checklist

- Set `clientId` per service for log/quota traceability.
- Enable idempotent producers where duplicates hurt.
- Prefer cooperative-sticky assignment for fleets with many partitions.
- Monitor `retries`/`throttles` counters; sustained growth means broker pressure.
- Run the soak harness (`bun run test:soak`) against a staging cluster before major rollouts —
  it validates throughput, latency, memory, lag recovery, and record-order integrity with
  release gates ([methodology](../performance-validation.md)).
