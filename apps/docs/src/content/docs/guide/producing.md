---
title: Producing
description: Send records with batching, compression, partitioning, and delivery guarantees.
---

## The basics

```ts
const producer = kafka.producer();
const results = await producer.send({
  topic: "orders",
  acks: "all", // 0 | 1 | "all"
  messages: [{ key: "user-1", value: JSON.stringify({ item: "book" }), headers: { trace: "abc" } }],
});
```

`send()` resolves with one `ProduceResult` per partition written:

```ts
{ topic: "orders", partition: 3, baseOffset: 42n, logAppendTime: 0n }
```

A message accepts `key`, `value`, `headers`, `partition`, `timestamp`, and an `onDelivery`
callback. Keys and values are `string | Uint8Array | ArrayBuffer | null`.

## Acknowledgements

| `acks`  | Meaning                                                      | Durability                   |
| ------- | ------------------------------------------------------------ | ---------------------------- |
| `0`     | Fire-and-forget; the send resolves once queued on the socket | Lowest, no retry possible    |
| `1`     | The partition leader acknowledged                            | Leader loss can lose records |
| `"all"` | All in-sync replicas acknowledged                            | Strongest                    |

## Batching

Concurrent `send()` calls are collected into one Produce request per broker/topic/partition for
up to `lingerMs` (default 5 ms) or `batchMaxMessages` (default 1,000), whichever comes first.
You rarely need to tune this; higher `lingerMs` trades latency for larger batches.

Multi-topic sends group automatically:

```ts
await producer.sendBatch({
  topicMessages: [
    { topic: "orders", messages: [{ value: "o-1" }] },
    { topic: "payments", messages: [{ value: "p-1" }] },
  ],
});
```

Call `producer.flush()` to force queued work out now (also done by `close()`).

## Compression

```ts
const producer = kafka.producer({ compression: "zstd" });
// per-request override:
await producer.send({ topic: "t", compression: "lz4", messages });
```

Supported codecs: `none`, `gzip`, `snappy`, `lz4`, `zstd`. Gzip/zstd use Bun's native codecs;
snappy and lz4 ship as pure TypeScript in this package. Consumers decompress transparently.

## Partitioning

When a message has no explicit `partition`:

1. A non-null key is hashed with Kafka's Murmur2 — identical keys land on identical partitions,
   preserving per-key order.
2. Keyless messages round-robin across partitions.
3. A custom `partitioner` overrides both:

```ts
const producer = kafka.producer({
  partitioner: ({ topic, partitionCount, key }) => {
    if (!key) return 0;
    return Number(new TextDecoder().decode(key)) % partitionCount;
  },
});
```

## Idempotent producers

```ts
const producer = kafka.producer({ idempotent: true });
```

The client obtains a producer identity from the broker and stamps sequence numbers per
partition. Retries after network errors then cannot create duplicates, and `acks` is forced to
`"all"`. Enable it whenever duplicates would hurt.

## Delivery callbacks

Each message gets exactly one callback when the broker acknowledges it or it fails for good:

```ts
await producer.send({
  topic: "orders",
  messages: [
    {
      value: "x",
      onDelivery: (error, result) => {
        if (error) console.error("failed:", error);
        else console.log("stored at", result!.baseOffset);
      },
    },
  ],
});
```

Retriable failures are retried internally first (`retry.maxRetries`); the callback fires only
after the final outcome.

See [configuration reference](configuration.md) for timeouts and retry tuning.
