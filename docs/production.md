# Production readiness

**Status:** usable for internal services and many single-cluster apps when you follow this guide.  
Core **must-fix** items below are implemented; remaining gaps are packaging, chaos coverage, and advanced APIs.

## Summary

| Posture | Verdict |
|---------|---------|
| Single-service producer → topic | **OK** with `flush` + optional `onDelivery` |
| Consumer group, manual commit, at-least-once | **OK** with auto-rebalance + idempotent handlers |
| Multi-instance consumer group | **OK** for standard assignors (auto rebalance on) |
| Strict per-message delivery tracking | **OK on FFI** via `onDelivery` + `poll`/`flush` |
| Long-lived process | Prefer **NAPI** (`useProductionNative()`) |
| Multi-tenant public library | Still early (semver, CI, LICENSE) |

Built on **librdkafka**. Protocol/fetch work is native; JS owns lifecycle and callbacks.

## Must-fix — implemented

### 1. Rebalance handling

**FFI:** installs `rebalance_cb` by default (`autoRebalance: true`) that:

- **EAGER** assignors: `assign` / `assign(null)` on assign/revoke  
- **COOPERATIVE:** `incremental_assign` / `incremental_unassign`  
- Optional `onRebalance({ kind, partitions, error? })` for app hooks  

**NAPI:** no custom cb → **librdkafka automatic** assign/revoke (safe default).

```ts
const consumer = new Consumer(
  { "bootstrap.servers": brokers, "group.id": "svc" },
  {
    autoRebalance: true, // default
    onRebalance: (e) => console.log(e.kind, e.partitions),
  },
);
```

Set `autoRebalance: false` only if you fully own assignment (advanced).

### 2. Delivery reports

**FFI:** `onDelivery` receives per-message results. Reports are served from `poll` / `flush`.

```ts
const producer = new Producer(
  { "bootstrap.servers": brokers, acks: "all" },
  {
    onDelivery: (r) => {
      if (r.errorCode !== 0) console.error(r.errorMessage, r.topic, r.partition);
    },
  },
);
producer.send({ topic: "t", value: "x" });
await producer.flush(30_000); // durability boundary + drains DR queue
```

**NAPI:** durability boundary remains `flush`; JS `onDelivery` bridge not wired yet.

### 3. Prefer NAPI for long-lived processes

```ts
import { useProductionNative } from "bun-kafka";
await useProductionNative(); // napi if native/build/*.node exists, else ffi
```

Or force: `BUN_KAFKA_NATIVE=napi` / `await useNative("napi")` after `bun run build:napi`.

Default when unset: **NAPI if addon present**, otherwise FFI.

### 4. Safe payload lifetime

**Default: copy** key/value out of librdkafka memory (safe after `done()`).

Zero-copy (FFI only, advanced):

```ts
// not required for production defaults
for await (const msg of consumer.messages({ copy: false })) {
  // buffers invalid after iteration body / done()
}
```

### 5. Shutdown helpers

```ts
import { installShutdown } from "bun-kafka";

const producer = new Producer({ /* ... */ });
const consumer = new Consumer({ /* ... */ });

installShutdown([producer, consumer], {
  timeoutMs: 10_000,
  exit: true, // process.exit(0) after close
});
```

Still set an orchestrator hard limit (e.g. K8s `terminationGracePeriodSeconds`).

### 6. Fatal errors

```ts
const err = producer.fatalError(); // or consumer.fatalError()
if (err) {
  // err.fatal === true
  process.exit(1);
}
// send/poll paths also throw if a fatal error is already set
```

### 7. Errors and logging hooks

```ts
new Producer(config, {
  onError: (e) => logger.error({ code: e.code, fatal: e.fatal }, e.message),
  onDelivery: (r) => metrics.delivery(r),
});
```

`KafkaError`: `code`, `message`, `fatal`, `retriable`.

---

## Still open (should-fix / packaging)

| Item | Notes |
|------|--------|
| CI matrix | OS × librdkafka × ffi/napi × broker |
| LICENSE + semver policy | Package still early (`0.1.0`) |
| Prebuilt NAPI artifacts | Build in image or publish `.node` |
| Chaos tests | Broker kill mid-flush/consume |
| SSL/SASL integration tests | Conf works via librdkafka props; add staged tests |
| NAPI `onDelivery` / `onRebalance` JS bridges | Use FFI or native defaults |
| Windows | Untested |
| Admin CRUD, transactions, Schema Registry | Not implemented |

## Recommended production posture

1. `await useProductionNative()` at process start.  
2. Pin **librdkafka** and **bun-kafka** in the image.  
3. Producer: `acks=all`, consider `enable.idempotence=true`, always `flush` at durability boundaries.  
4. Consumer: `enable.auto.commit=false`, commit after durable side effects; handlers **idempotent**.  
5. Use `installShutdown([producer, consumer])`.  
6. Check `fatalError()` on health/ready fail paths.  
7. Load-test partitions, message size, and lag on staging.  
8. Staging: broker restart during produce and consume.

### Producer

```ts
await useProductionNative();
const producer = new Producer(
  {
    "bootstrap.servers": process.env.KAFKA_BROKERS!,
    acks: "all",
    "enable.idempotence": true,
    "linger.ms": 5,
    "message.timeout.ms": 120_000,
  },
  {
    onDelivery: (r) => {
      if (r.errorCode) metrics.fail(r);
      else metrics.ok(r);
    },
    onError: (e) => logger.error(e),
  },
);
installShutdown(producer);
```

### Consumer

```ts
await useProductionNative();
const consumer = new Consumer(
  {
    "bootstrap.servers": process.env.KAFKA_BROKERS!,
    "group.id": "my-service",
    "enable.auto.commit": false,
    "auto.offset.reset": "earliest",
  },
  {
    autoRebalance: true,
    onRebalance: (e) => logger.info({ rebalance: e.kind, n: e.partitions.length }),
    onError: (e) => logger.error(e),
  },
);
consumer.subscribe("orders");
installShutdown(consumer);

for await (const batch of consumer.batches({ batchSize: 512, eachBatchCommit: true })) {
  for (const msg of batch) {
    await handle(msg.value!); // already a copy by default
  }
}
```

## Security

Pass SSL/SASL through librdkafka config:

```ts
new Producer({
  "bootstrap.servers": "kafka:9093",
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "SCRAM-SHA-512",
  "sasl.username": process.env.KAFKA_USER!,
  "sasl.password": process.env.KAFKA_PASS!,
})
```

Validate on staging; dedicated auth tests are not in-repo yet.

## Environment

| Variable | Purpose |
|----------|---------|
| `KAFKA_BROKERS` | Bootstrap servers |
| `BUN_KAFKA_NATIVE` | Force `ffi` or `napi` |
| `LIBRDKAFKA_PATH` | Shared library override |
| `NODE_API_INCLUDE` | Headers for NAPI build |

## Decision checklist

- [x] Auto rebalance (or documented single-member)  
- [x] Delivery reports (FFI) or flush durability contract  
- [x] NAPI production path (`useProductionNative`)  
- [x] Safe payload copies by default  
- [x] Shutdown helper  
- [x] Fatal error API  
- [ ] librdkafka + bun-kafka pinned in deploy image  
- [ ] Staging broker restart test  
- [ ] Auth validated if required  
- [ ] Lag + crash-loop alerts  
- [ ] CI for your target OS/backend  

## Tests

```bash
bun test test/prod-mustfix.test.ts
bun test test/features.test.ts
```

## Related

- [README](../README.md) — features, Kafka versions, API  
- `test/prod-mustfix.test.ts` — must-fix coverage  
- `test/features.test.ts` — full public API  
