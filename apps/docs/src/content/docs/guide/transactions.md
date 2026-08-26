---
title: Transactions
description: Transactions
---

Kafka transactions let one producer write records to many partitions _and_ commit consumer
offsets as a single all-or-nothing unit. Readers using `read_committed` never see aborted work.

## Transactional produce

```ts
const producer = kafka.producer({ transactionalId: "orders-1" });

await producer.beginTransaction();
try {
  await producer.send({ topic: "orders", messages: [{ value: "order-1" }] });
  await producer.send({ topic: "audit", messages: [{ value: "order-1" }] });
  await producer.commitTransaction();
} catch (error) {
  await producer.abortTransaction();
  throw error;
}
```

What happens under the hood, in order:

1. `InitProducerId` with your `transactionalId` — the broker fences any older incarnation of
   this producer (zombie protection) and bumps the epoch.
2. The transaction coordinator is resolved and touched partitions are registered
   (`AddPartitionsToTxn`).
3. Records carry the transactional batch marker.
4. `EndTxn` commits or aborts; control markers make the outcome visible to consumers.
5. The epoch bumps again so a crashed process cannot resume the old identity.

`transactionTimeoutMs` (default 60 s) bounds how long a transaction may stay open broker-side.

## Exactly-once consume-transform-produce

The classic pattern: read committed input, produce output and commit input offsets in one
transaction.

```ts
const input = kafka.consumer({
  groupId: "etl",
  isolationLevel: "read_committed",
});
const output = kafka.producer({ transactionalId: "etl-1" });

await input.subscribe("raw");
await output.beginTransaction();
try {
  const messages = await input.fetch({ maxMessages: 1000 });
  await output.send({
    topic: "clean",
    messages: messages.map((m) => ({ key: m.key, value: m.value })),
  });
  // Commit the INPUT offsets inside the OUTPUT transaction:
  await output.sendOffsetsToTransaction(
    messages.map((m) => ({ topic: m.topic, partition: m.partition, offset: m.offset + 1n })),
    "etl", // the consumer group id
  );
  await output.commitTransaction();
} catch (error) {
  await output.abortTransaction();
  throw error;
}
```

`offset + 1n` matters: committed offsets point at the _next_ record to read. On abort, both the
produced records and the offset commit disappear together — no duplicates, no lost records.

## Rules of thumb

- One `transactionalId` per pipeline instance; reuse it across restarts to fence zombies.
- Keep transactions short; long-open transactions stall read_committed consumers' high
  watermarks (LSO).
- `producer.close()` best-effort aborts an open transaction before disconnecting.
- Consumers must set `isolationLevel: "read_committed"` or they will see uncommitted/aborted
  records anyway.

## Fencing verification

A fenced zombie gets `FENCED_INSTANCE_ID`/`INVALID_PRODUCER_EPOCH` typed errors on its next
request. The real-broker suite exercises this:
`KAFKA_BROKERS=127.0.0.1:9092 bun test test/impl/transactions-fencing-real.test.ts`.
