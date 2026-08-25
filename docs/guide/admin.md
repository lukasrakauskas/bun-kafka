# Administration

All admin operations go through `kafka.admin()`:

```ts
const admin = kafka.admin();
```

## Metadata and topics

```ts
await admin.metadata(); // brokers, topics, partitions, leaders, clusterId
await admin.metadata(["events"]); // restrict to named topics

await admin.createTopics([{ name: "events", numPartitions: 3, replicationFactor: 1 }]);
await admin.createPartitions([{ name: "events", count: 6 }]); // grow only
await admin.deleteTopics(["events"]);
```

`createTopics` returns per-topic results (`{ topic, errorCode, errorMessage? }`) so partial
success is visible.

## Configs

```ts
await admin.describeConfigs([{ resource: 2 /* TOPIC */, name: "events" }]);

// Replace-mode: entries not included are removed for that resource.
await admin.alterConfigs([
  { resource: 2, name: "events", entries: [{ name: "retention.ms", value: "604800000" }] },
]);

// Incremental (preferred; Apache Kafka 2.3+): change individual entries without clobbering.
await admin.incrementalAlterConfigs([
  {
    resourceType: 2,
    resourceName: "events",
    ops: [
      { name: "retention.ms", operation: "set", value: "604800000" }, // set | delete | append | subtract
    ],
  },
]);
await admin.incrementalAlterConfigs(
  [
    {
      resourceType: 2,
      resourceName: "events",
      ops: [{ name: "cleanup.policy", operation: "append", value: "compact" }],
    },
  ],
  { validateOnly: true }, // dry run
);
```

`incrementalAlterConfigs` resolves per-resource results (`{ name, error, message? }`), so a bad
entry never hides the others. `delete` reverts an entry to its cluster default. Note that some
brokers restrict specific operations — Redpanda, for example, rejects APPEND/SUBTRACT on
`cleanup.policy` and broker properties on named brokers (use the empty resource name for
cluster-wide broker configs).

## Consumer groups

```ts
await admin.listGroups();
await admin.describeGroups(["workers"]);
await admin.deleteGroups(["workers"]);

await admin.groupOffsets("workers"); // committed offsets per topic/partition
await admin.setGroupOffsets("workers", [{ topic: "events", partition: 0, offset: 0n }]);
await admin.resetGroupOffsets("workers", "events", true); // true = earliest
```

Resetting requires the group to be inactive (no running members).

## Records and offsets

```ts
await admin.topicOffsets("events"); // low/high per partition
await admin.offsetByTimestamp("events", 0, Date.now() - 3_600_000); // offset at an hour ago
await admin.deleteRecords([
  { topic: "events", partition: 0, offset: 1000n }, // truncate below this offset
]);
```

## Quotas (KIP-546)

```ts
await admin.describeClientQuotas({ entityType: "client-id", match: { client-id: "etl" } });
await admin.alterClientQuotas([
  {
    entity: { type: "client-id", name: "etl" },
    ops: [{ key: "producer_byte_rate", value: 1048576 }],
  },
]);
```

## ACLs

```ts
import type { AclBinding } from "bun-kafka";

await admin.createAcls([binding]); // AclBinding: resourceType/name/pattern + operation + permission + principal/host
await admin.describeAcls(filter); // AclFilter narrows by any combination
await admin.deleteAcls(filter);
```

ACL administration requires an authorizer enabled on the broker.

## Delegation tokens

Short-lived credentials issued by the cluster for clients (requires SASL auth to the broker):

```ts
const token = await admin.createDelegationToken({ ownerName: "svc-etl" });
await admin.renewDelegationToken(token.value /* hmac bytes */);
await admin.expireDelegationToken(token.value);
await admin.describeDelegationTokens();
```
