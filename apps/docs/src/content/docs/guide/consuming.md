---
title: Consuming
description: Read records with manual assignment or managed consumer groups.
---

## Three ways to consume

**1. Manual assignment** — no group, full control:

```ts
const consumer = kafka.consumer({ fromBeginning: true });
await consumer.assign([{ topic: "events", partition: 0, offset: "earliest" }]);
```

**2. Assign whole topics** without a group (all partitions discovered from metadata):

```ts
await consumer.subscribe({ topic: "events", fromBeginning: true });
```

**3. Consumer group** — partitions are distributed across members and offsets are committed:

```ts
const consumer = kafka.consumer({ groupId: "workers", fromBeginning: true });
await consumer.subscribe("events"); // or ["a", "b"], or a RegExp
```

## Reading messages

Pull-style bounded batches:

```ts
const messages = await consumer.fetch({ maxMessages: 500, maxWaitMs: 100 });
```

Or iterate indefinitely:

```ts
for await (const message of consumer) {
  await handle(message); // backpressure is natural: the next fetch starts after you return
}
```

Each message is `{ topic, partition, offset: bigint, key, value, timestamp: bigint,
headers, brokerId }`. By default `key`/`value` are stable zero-copy views into the receive
buffer; pass `copy: true` to `fetch()` if you retain them beyond the next fetch.

## Offsets

```ts
consumer.position("events", 0); // local position: bigint | undefined
await consumer.watermarks("events", 0); // { low, high } end offsets
consumer.seek({ topic: "events", partition: 0, offset: 5n }); // jump; clears buffered data

// Groups only:
await consumer.commitOffsets(); // commit current positions
await consumer.commitOffsets([{ topic: "events", partition: 0, offset: 100n }]);
const committed = await consumer.committed([{ topic: "events", partition: 0 }]);
```

Commit after processing, not before, if you want at-least-once semantics. With
`autoCommit: false` (default) nothing is committed unless you call it.

## Flow control

```ts
consumer.pause([{ topic: "events", partition: 0 }]); // stop fetching this partition
consumer.resume([{ topic: "events", partition: 0 }]);
consumer.assignment(); // currently owned [{topic, partition}]
await consumer.close();
```

## Consumer groups and rebalancing

The default `groupProtocol: "classic"` flow is join → assignment → heartbeat → leave. Two
client-side assignment strategies are available via `partitionAssigner`:

- `"range"` (default): eager. Every rebalance revokes everything and reassigns.
- `"cooperative-sticky"`: incremental (KIP-429). Members keep their partitions across
  rebalances; only partitions that must move change hands. Prefer this for fleets with many
  partitions or frequent deploys.

Kafka 4.x also supports KIP-848 broker-side assignment:

```ts
const grouped = kafka.consumer({
  groupId: "workers",
  groupProtocol: "consumer",
  groupRemoteAssignor: "uniform", // optional; omit for the broker default
});
```

The consumer protocol uses ConsumerGroupHeartbeat and member epochs; the broker controls heartbeat and session intervals. Classic remains the default, so migration is explicit and older brokers continue to use JoinGroup/SyncGroup/Heartbeat. A broker without ConsumerGroupHeartbeat v0 rejects the opt-in during API negotiation.

Static membership (`groupInstanceId`) prevents rebalance storms during rolling restarts:
members with the same identity rejoin without triggering reassignment.

Regex subscriptions expand against live cluster metadata at subscribe time:

```ts
await grouped.subscribe(/^events\./);
```

## Deserializers

Typed decode at the edge:

```ts
type Event = { id: string };
const consumer = kafka.consumer<{ userId: number }, Event>({
  groupId: "workers",
  keyDeserializer: (data) => (data ? Number(new TextDecoder().decode(data)) : -1),
  valueDeserializer: (data) => (data ? JSON.parse(new TextDecoder().decode(data)) : null),
});
// message.key: number, message.value: Event | null
```

## Isolation for transactional topics

Pair consumers of transactional producers with:

```ts
kafka.consumer({ groupId: "g", isolationLevel: "read_committed" });
```

Aborted records and transaction control markers are filtered so you only see committed data.
See [transactions](transactions.md).
