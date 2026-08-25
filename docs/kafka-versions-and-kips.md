# Kafka versions, wire APIs, and KIPs — mapped to bun-kafka

This document is written for engineers who are new to Kafka. It explains how Kafka
versions and its improvement proposals (KIPs) relate to the wire protocol this client
speaks, which parts bun-kafka implements, where the implementation lives, and how each
claim is verified by a test you can run. The exhaustive per-API version table lives in
[feature completeness](feature-completeness.md); this page is the "why".

## 1. Kafka concepts in five minutes

- **Broker**: one Kafka server. A cluster is several brokers for scale and fault tolerance.
- **Topic**: a named stream of records (like a table or channel).
- **Partition**: a topic is split into ordered, numbered partitions (0, 1, 2 …). Order is
  guaranteed only within one partition. Each partition has a **leader** broker; writes go to
  the leader, followers copy it.
- **Offset**: a record's position number inside its partition (`0n`, `1n`, …). Consumers track
  their position as an offset.
- **Producer / consumer**: writers and readers of records.
- **Consumer group**: several consumers sharing the work. The cluster assigns each partition of
  a subscribed topic to exactly one member; members periodically report ("commit") their offsets
  to a special coordinator broker so a restart can resume where they stopped.
- **Record batch**: the on-disk/network unit. Modern batches are "magic 2" format: they carry a
  base offset, per-record timestamps, headers, CRC32C integrity, and optional compression.

The client in this repository speaks Kafka's binary TCP protocol directly — there is no JVM,
librdkafka, or proxy involved. `src/bun/connection.ts` owns one TCP/TLS socket per broker;
`src/bun/protocol.ts` implements the shared binary encoders/decoders.

## 2. How the protocol versions things

Every request/response pair is identified by an **API key** (Produce = 0, Fetch = 1, Metadata =
3 …). Each key evolves through integer **versions**. A broker advertises, per key, the range
`[minVersion, maxVersion]` it accepts:

```
client ── ApiVersions(18, v0) ──► broker
client ◄── [{key:0,min,max}, {key:1,min,max}, ...] ── broker
```

Newer versions add fields or replace fixed-width fields with variable-length ones. Since Kafka
2.1 some requests are **flexible** (KIP-482): strings/arrays use compact lengths and every struct
carries a tagged-field section that lets future versions append optional data without breaking
older clients.

### This client's strategy: conservative fixed versions + fail-fast validation

bun-kafka serializes each API at one deliberately old-but-sufficient version (for example
Metadata v2, Produce v3, Fetch v7). On every new connection it runs the ApiVersions handshake
and refuses to send any API whose fixed version falls outside the broker's advertised range
(`#prepare` in src/bun/connection.ts), raising a typed error naming the API, the required
version, and the supported range.

Why this works without downgrade logic: the chosen versions predate nothing a modern broker
lacks. Every Apache Kafka release since 0.11 (2017) and every current Redpanda build supports
them. The floor is documented in the README ("requires Kafka 0.11 or newer").

What this costs: a hypothetical ancient broker (< 0.11) fails fast with a clear message instead
of silently misbehaving. What it saves: ~30 APIs × multiple serializers of maintenance, which is
why mature clients like franz-go carry thousands of lines of per-version codecs while bun-kafka
ships in about five thousand lines total.

## 3. Kafka releases → what bun-kafka uses

| Release | Year | Headline change                                                                                                      | Relevance to bun-kafka                                                                         |
| ------- | ---- | -------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| 0.9     | 2015 | Wire-level consumer groups, SASL PLAIN handshake                                                                     | JoinGroup/SyncGroup/Heartbeat/LeaveGroup flow implemented                                      |
| 0.10.x  | 2016 | ApiVersions negotiation, SCRAM auth (KIP-43)                                                                         | Handshake on every connection; SCRAM-SHA-256/512 via Web Crypto                                |
| 0.11    | 2017 | Idempotent producer + transactions (KIP-98), magic-2 record format with headers (KIP-82)                             | **The client's floor.** Record batches, headers, idempotence, transactions all depend on it    |
| 1.1     | 2018 | Incremental fetch sessions (KIP-227), CreatePartitions (KIP-195), DeleteGroups (KIP-229), delegation tokens (KIP-37) | All implemented                                                                                |
| 2.0     | 2018 | SASL/OAUTHBEARER (KIP-255)                                                                                           | Implemented incl. async token providers                                                        |
| 2.1     | 2018 | ZSTD compression (KIP-110); flexible versions begin (KIP-482)                                                        | ZSTD via Bun's native codec; flexible encoding used where the client's chosen versions need it |
| 2.2     | 2019 | Timed SASL reauthentication (KIP-368)                                                                                | OAUTHBEARER re-auth at 80% of session lifetime                                                 |
| 2.3     | 2019 | Static membership (KIP-345)                                                                                          | `group.instance.id` through group APIs v3                                                      |
| 2.4     | 2019 | Cooperative-sticky assignment (KIP-429)                                                                              | Second assignor alongside eager range                                                          |
| 2.6     | 2020 | Client quota admin APIs (KIP-546)                                                                                    | DescribeClientQuotas/AlterClientQuotas v1                                                      |
| 3.3+    | 2022 | KRaft replaces ZooKeeper (KIP-500)                                                                                   | Broker-side only — invisible to clients. Works unchanged                                       |
| 4.0     | 2025 | Next-gen consumer protocol GA (KIP-848)                                                                              | Not adopted; see §6                                                                            |

## 4. KIP catalog: claim → code → verification

"Verify" here means: run the listed command against the mock-broker suites (no cluster needed)
or, marked real, against a live broker via `KAFKA_BROKERS=127.0.0.1:9092`.

| KIP             | In plain words                                                                                                                                                                     | Status            | Where                                                                                        | Verify                                                                                                                                          |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------- | -------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| KIP-98          | Idempotent producer (`producerId` + per-partition sequence numbers so retries never duplicate) and transactions (group many produces + offset commits into an all-or-nothing unit) | Yes               | producer.ts (idempotent init, txn lifecycle), consumer.ts (aborted-range filtering)          | `bun test test/features/transactions.test.ts`; real: `bun test test/impl/transactions-real.test.ts test/impl/transactions-fencing-real.test.ts` |
| KIP-82          | Record headers (key/value metadata attached to each record)                                                                                                                        | Yes               | protocol.ts batch encode/decode                                                              | `bun test test/unit/protocol.test.ts`                                                                                                           |
| KIP-43          | SASL/SCRAM-SHA-256/512 authentication without storing plaintext passwords server-side                                                                                              | Yes               | connection.ts `#scram` (PBKDF2/HMAC via Web Crypto)                                          | `bun test test/features/sasl-scram.test.ts`                                                                                                     |
| KIP-227         | Incremental fetch sessions: consumer says "same as last time, plus/minus these", cutting request size and letting brokers push only changes                                        | Yes               | consumer.ts session id/epoch, forgotten-topic pruning                                        | `bun test test/features/fetch-sessions.test.ts`; real: `test/impl/fetch-sessions-real.test.ts`                                                  |
| KIP-229         | DeleteGroups admin call                                                                                                                                                            | Yes               | admin.ts `deleteGroups`                                                                      | `bun test test/features/admin-groups.test.ts`                                                                                                   |
| KIP-195         | Increase partition count online                                                                                                                                                    | Yes               | admin.ts `createPartitions`                                                                  | integration suite `test/impl/admin.test.ts`                                                                                                     |
| KIP-255         | SASL/OAUTHBEARER: bearer tokens instead of passwords                                                                                                                               | Yes               | connection.ts oauthbearer branch                                                             | `bun test test/features/oauth-provider-callback.test.ts`                                                                                        |
| KIP-368         | Clients re-authenticate before the SASL session expires instead of being disconnected                                                                                              | Yes               | connection.ts `scheduleReauthentication` (fires at 80% of lifetime)                          | `bun test test/features/oauth-reauth.test.ts`                                                                                                   |
| KIP-345         | Static membership: stable `group.instance.id` avoids rebalance storms on rolling restarts                                                                                          | Yes               | consumer.ts JoinGroup/SyncGroup v3 fields                                                    | `bun test test/features/consumer-features.test.ts`                                                                                              |
| KIP-429         | Cooperative rebalancing: members keep their partitions across rebalances; only moved partitions change hands (two-phase protocol)                                                  | Yes               | consumer.ts cooperative assignor + subscription v1 owned-partition sets                      | `bun test test/features/cooperative.test.ts`; real: `test/impl/cooperative-real.test.ts`                                                        |
| KIP-546         | Admin APIs to describe/alter per-client quota overrides                                                                                                                            | Yes               | admin.ts `describeClientQuotas`/`alterClientQuotas`                                          | `bun test test/features/admin-quotas.test.ts`; real: `test/impl/admin-quotas-real.test.ts`                                                      |
| KIP-37          | Delegation tokens: short-lived credentials issued by the cluster                                                                                                                   | Yes               | admin.ts token CRUD (needs SASL + broker support)                                            | mock wire-shape tests in features suite                                                                                                         |
| KIP-110         | ZSTD record compression                                                                                                                                                            | Yes               | Bun native zstd codec                                                                        | `bun test test/unit/compression.test.ts`                                                                                                        |
| KIP-482         | Flexible versions: compact encodings + tagged fields                                                                                                                               | Partial by design | protocol.ts Writer/Reader helpers; used by quota APIs and any chosen version marked flexible | `bun test test/unit/protocol.test.ts test/features/admin-quotas.test.ts`                                                                        |
| KIP-219         | Brokers may throttle slow clients and signal the delay in responses                                                                                                                | Yes (surfaced)    | throttle durations emitted as `onEvent({type:"throttle"})`                                   | `bun test test/features/observability.test.ts`                                                                                                  |
| KIP-500 / KRaft | Broker architecture replacing ZooKeeper                                                                                                                                            | N/A               | Broker-side; no client-visible protocol change                                               | Any KRaft cluster (Redpanda dev container qualifies)                                                                                            |
| KIP-405         | Tiered storage offload to object stores                                                                                                                                            | N/A               | Broker-side; consumers just see normal offsets                                               | —                                                                                                                                               |
| KIP-848         | Next-generation consumer group protocol (broker-side balancing, much lighter heartbeats)                                                                                           | No                | Client still speaks the classic group protocol every current broker supports                 | Gap tracked in [gap audit](client-gap-audit.md)                                                                                                 |
| KIP-881         | Rack-aware incremental fetch (prefer replicas in the consumer's rack)                                                                                                              | No                | Consumer sends no rack id                                                                    | Gap tracked in [gap audit](client-gap-audit.md)                                                                                                 |

## 5. Proposal: full versioned-method support (documented, deferred)

If bun-kafka ever needs to talk at different protocol versions per broker — today it does not —
this is the design other clients converge on, and the phased path this project would take.

1. **Registry**: replace each hardcoded `apiVersion` argument with
   `negotiate(apiKey)` returning the version to use: `min(clientMax[apiKey], brokerMax[apiKey])`
   clamped to the serializer set available for that key. The ApiVersions map already exists per
   connection (connection.ts `#versions`).
2. **Per-version serializers**: split each `Writer`/`Reader` body into functions keyed by version
   (only where versions differ meaningfully — most APIs differ in trailing fields only).
   franz-go proves the shape: one file per API, a `switch` over version inside.
3. **Feature gates**: expose what the extra versions unlock (e.g. newer ListOffsets for
   timestamp-based offset lookup variants) behind option flags, defaulting to today's behavior.
4. **Tests**: golden-frame tests per version against the mock broker (the harness already
   asserts exact wire bytes).

Estimated cost: the registry is small; the serializer matrix is the bulk — roughly one
person-week per ten APIs done properly across encode, decode, error paths, and tests.

**Trigger conditions to start**: (a) a must-support broker deprecates one of the client's fixed
versions, or (b) a feature lands that only exists at a higher version (e.g. KIP-848-only
clusters). Neither holds for any broker released since 2017, so the lazy-but-correct choice
remains: fixed versions, fail-fast validation, clear error text.

## 6. Verifying your own cluster

From the Kafka distribution:

```bash
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

prints every API key with the broker's `[min, max]`. Everything above the client's fixed
versions (see the table in [feature completeness](feature-completeness.md)) confirms
compatibility. From Node/Bun, `kafka.admin().metadata()` plus `kafka.healthCheck()` give you
cluster id, broker list, and per-broker liveness.
