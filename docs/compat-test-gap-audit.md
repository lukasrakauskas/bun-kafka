# kafkajs-compat external suite results & test-gap audit

Companion to `docs/feature-completeness.md` (which tracks *implementation*
coverage). This document records (a) what the external compatibility suites
prove and (b) where bun-kafka's **own test suites** still have gaps against
the full Kafka feature set.

## External suites

Runner: `~/dev/bun-kafka-test-suites/runner.ts` (deterministic order, pinned
vendor SHAs, writes `REPORT.md`; skips cleanly when no broker is reachable).
Adaptation is dependency-injection only: stub packages `kafkajs` and
`@confluentinc/kafka-javascript` resolve to `bun-kafka/kafkajs-compat`, so the
vendored application code runs unmodified under Bun.

| Suite | Vendored project | What it exercises |
|---|---|---|
| node-red-kafka-suite | blanpa/node-red-contrib-kafka-suite | Full client matrix: adapter lifecycle events, send/sendBatch, per-key ordering, commit offsets across consumer restarts, pause/resume, seek semantics, admin surface (describeCluster, createTopics w/ configEntries, fetchTopicOffsets, resetOffsets, list/describe/delete groups), codec-gating parity, native snappy/lz4/zstd batches |
| railway-kafka-package | Pritam-25/distributed_railway_booking_platform `packages/kafka` | `@confluentinc/kafka-javascript` nested-config shape, idempotent producer singleton, consumer-runner with OTel span propagation headers, DLQ routing for SyntaxError/ZodError poison messages with metadata headers, retry-policy presets |
| trench-kafka-service | FrigadeHQ/trench | NestJS KafkaService as shipped: env-driven SSL/SASL config branches, topic bootstrap via admin, JSON event ingestion at scale through `eachBatch` + `autoCommitInterval` + `partitionsConsumedConcurrently=4`, batched vs one-by-one callbacks, per-key ordering |
| otel-orders-contract | open-telemetry/opentelemetry-demo | Cross-language wire contract: hand-encoded `OrderResult` protobufs (demo.proto) byte-exact round trip, keyed murmur2 affinity, fraud-detection consumption pattern |

Current status: 22 pass / 0 fail (`REPORT.md`). PostHog was evaluated and
excluded for now: its Node ingestion moved to node-rdkafka, not kafkajs; its
integration scenarios are candidates for a future ported-scenarios suite.

## Bugs the external suites caught in bun-kafka

Each of these passed bun-kafka's own suites before:

1. **ListGroups version drift** — core requested v1 but parsed a v4-style
   three-field entry; Apache brokers always include `group_state` only in v4+
   (and v4 is flexible). Any compliant broker overran. Fixed by speaking v1's
   actual two-field shape; states filtering is applied client-side.
2. **`consumer.subscribe({topic})` singular form** crashed on `undefined.topics`.
3. **DescribeGroups nullable `error_message`** — some brokers omit the field
   entirely where Apache writes an explicit null; compat layer trial-parses
   both shapes and keeps the exact-consuming one.
4. **Idle-fetch sleep** made `pause()`/`resume()` unresponsive when apps set
   multi-second `maxWaitTimeInMs`; nap is now bounded to 250 ms.
5. **Compat-layer subscription merge** — kafkajs accumulates topics across
   `subscribe()` calls into one group subscription; naive delegation replaced it.

## Gaps in bun-kafka's own suites versus the Kafka feature set

Ordered by risk. "Impl" = behavior exists in `src/bun`, "suite" = covered by a
test today.

### Protocol / API coverage

- **ListGroups v4/v5**: no suite pins the flexible v4+ encoding (per-group
  state, error message, pagination token in v5). Impl currently targets v1;
  when v4 support lands it needs fixture tests against both encodings.
  *(Found by external suite — see above.)*
- **OffsetFetch v5 nullable-topics** ("all topics for group"): suites only
  cover explicit topic lists; the compat admin approximates all-topic queries
  by scanning cluster metadata. A dedicated OffsetFetch v5 flexible test is
  missing.
- **DescribeConfigs v1+/AlterConfigs v1 (incremental)**: only v0 exercised;
  `isDefault`/`configSource` mapping is heuristic. No suite asserts config
  sources against a live broker matrix.
- **ListOffsets v5+**: only v1 used; MaxTimestamp lookup and isolation-level
  variants untested.
- **Produce v9 / Fetch v12+ (flexible, leader epoch, KIP-951 streaming
  acknowledgements)**: fixed at v3/v7. Suites never negotiate upward even when
  brokers advertise newer versions; add a version-ceiling test so silent
  downgrades are visible.
- **ApiVersions broker-compat fallback** (v0 auto-downgrade for pre-0.10
  brokers): untested path.

### Consumer semantics

- **Consumer-level retry policy** (`retry`, `restartOnFailure`): kafkajs
  retries handler errors N times before crashing/stopping; compat crashes the
  loop after `onCrash`. No suite pins either semantic.
- **Rebalance lifecycle events**: `REBALANCING`/`GROUP_JOIN` payloads are
  emitted but nothing fires `REBALANCING` because core rejoins silently inside
  heartbeat recovery. Need an eager-rebalance detection suite (kill a member,
  assert event stream) plus EAGER vs COOPERATIVE revoke assertion on
  assignment snapshots.
- **Static membership (KIP-345)** rejoin-with-same-instanceId across restart:
  unit tests exist for join encoding; no live suite asserts broker-side member
  continuity across process restarts.
- **Offset reset policies** (`earliest`/`latest`/`none` as config rather than
  call-site flag): only implicit via fromBeginning.
- **read_committed under aborted transactions during rebalance windows**:
  transactions feature tests cover steady state, not the race where LSO moves
  mid-fetch.
- **maxBytes partition splitting / message larger than fetch window**: unit
  tests cover decoder limits, no integration asserts partial-batch refetch.

### Producer semantics

- **Per-send compression override with mixed batches** (two compression codecs
  interleaved within one linger window): compat creates per-codec producers,
  but no suite asserts ordering/isolation between them.
- **Idempotent sequence-reset after broker restart** (UNKNOWN_PRODUCER_ID →
  InitProducerId retry): chaos tests cover connection loss, not epoch reset.
- **acks=0 fire-and-forget error accounting**: covered in features, missing
  from any external-style end-to-end suite.
- **Transaction + transactional.id fencing (PRODUCER_FENCED) recovery**:
  transactions-real covers happy path and abort; fenced-producer takeover is
  untested.

### Security

- **SASL/OAUTHBEARER with oauthBearerProvider callback** (token refresh +
  reauth KIP-368): oauth-reauth feature test uses static token providers only;
  provider-callback shape added for node-red parity has no suite.
- **SCRAM-SHA-512 vs 256 negotiation matrix against live broker**: impl tests
  use one mechanism; matrix coverage lives only in docker-compose profiles.
- **TLS hostname verification off/on with IP SANs**: chaos TLS fixtures cover
  CA trust, not identity checks.

### Admin

- **describeGroups cross-broker tolerance**: Redpanda omits nullable
  error_message where Apache writes null; compat handles both, but bun-kafka's
  own suites have no Redpanda-shaped negative fixture. Add recorded-bytes
  regression fixtures (hex frames) so broker quirks can't regress silently.
- **IncrementalAlterConfigs (KIP-248)**: not implemented, not tested.
- **ListPartitionReassignments / AlterPartitionReassignments (KIP-455)**:
  absent from impl and suites.
- **ElectLeaders (KIP-460)**: absent.

### Operations

- **Metrics/events parity**: producer/consumer network.request* events are
  declared but never emitted; suites that listen for them would hang. Either
  emit or document as unsupported.
- **Graceful shutdown under in-flight batches**: stop() awaits loop boundary;
  no test asserts zero-duplicate/zero-loss guarantees when stopping mid-batch
  with autoCommitInterval pending.

## Recommended next suites

1. Recorded-frame regression fixtures for every admin API (kills broker-quirk
   bugs like ListGroups/DescribeGroups permanently).
2. Live broker matrix job: Apache Kafka + Redpanda × PLAINTEXT/SASL/TLS.
3. Rebalance storm suite: 3 consumers churning membership while producing,
   asserting REBALANCING events + zero offset loss.
4. Port posthog nodejs integration scenarios (idempotent produce, rebalance)
   as a fourth vendor suite.
