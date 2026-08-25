# Handoff: bun-kafka feature completion task

## Objective

Implement all missing Kafka-client features listed in `docs/feature-completeness.md`, plus tests.
Soak tests are explicitly skipped. Reference implementation: https://github.com/platformatic/kafka (cloned at `/tmp/plat-kafka`).

## Environment

- **Broker**: single-node Redpanda `v25.2.1` docker container `bun-kafka-dev` on `127.0.0.1:9092`.
  Recreate with:
  ```bash
  docker run -d --name bun-kafka-dev -p 9092:9092 \
    docker.redpanda.com/redpandadata/redpanda:v25.2.1 redpanda start \
    --mode dev-container \
    --kafka-addr=PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr=PLAINTEXT://127.0.0.1:9092
  ```
- **CRITICAL broker config**: `internal_topic_replication_factor` defaults to **3** in this image,
  which can never be satisfied single-node. On-demand creation of `kafka_internal/tx` then fails
  silently → all transactional InitProducerId calls return `NOT_COORDINATOR`. Set it before use:
  ```bash
  docker exec bun-kafka-dev rpk cluster config set internal_topic_replication_factor 1
  # then restart the container so it takes effect for topic creation
  ```
  Verify group flows work against the real broker (`/tmp/group-probe2.ts`) after any broker restart.
- Run unit/mock tests: `bun test test/bun-native.test.ts test/chaos.test.ts` (no broker needed).
- Run real-broker tests: `bun test test/features.test.ts ...` etc. (broker must be up).
- Typecheck: `bun run typecheck`.

## Commits completed (all pushed to local master)

| Commit | Content |
|---|---|
| `d82656e` | **Pure-TS Snappy + LZ4 codecs** (`src/bun/snappy.ts`, `src/bun/lz4.ts` incl. xxhash32), wired into record-batch encode/decode (attributes 2/3). Also fixed CreateTopics/CreatePartitions sending `null` arrays (Redpanda silently kills the connection on those). Tests: `test/compression.test.ts`. |
| `df72e7a` | **Producer**: acks=0 (fire-and-forget via `Connection.sendOnly` + `Cluster.fireAndForget`), custom `partitioner` option, per-message `onDelivery(error, result)` callbacks invoked exactly once. Tests: `test/delivery.test.ts`. |
| `649f051` | **Consumer**: static membership via `groupInstanceId` (JoinGroup/SyncGroup/Heartbeat/LeaveGroup **v3** — note field order: instance id goes AFTER member_id; responses add throttle_time_ms in v3), read-committed isolation (Fetch isolation byte + aborted-txn filtering + control-record handling in `RecordSetDecoder`; `encodeRecordBatch` gained `baseOffset` + `control` attrs params), RegExp topic subscription expansion, key/value deserializers (`ConsumedMessage` union return). Tests: `test/consumer-features.test.ts`. |
| `d2e3aa1` | **Admin**: Metadata **v2** request + `clusterId` on ClusterMetadata (all mock metadata builders updated with `.string(null)` cluster id), `listGroups`/`describeGroups`/`deleteGroups` (v1), `deleteRecords` (**field order is index→lowWatermark→error**), ACLs create/describe/delete (v0; DeleteAcls matching_acls are FLAT entries each carrying own error/message). Tests: `test/admin-groups.test.ts` (includes real-broker ACL round trip). |
| `b384d67` | **Observability**: `Connection.stats` counters, `Cluster.stats()`, `statsIntervalMs` events, `Kafka.healthCheck()` (ApiVersions ping per broker), `logger` hooks, complete error-code catalog `kafkaErrorName()` in `src/errors.ts`. Tests: `test/observability.test.ts`. |
| `84b8a82` | **OAuth timed reauthentication** (KIP-368): captures SaslAuthenticate session lifetime, re-auths at 80% of lifetime with fresh provider tokens; fatal-failure handling. Test: `test/oauth-reauth.test.ts`. |

All tests pass: 48–57 tests across suites against mock brokers AND the live Redpanda container.

## Work in progress: TRANSACTIONS (uncommitted)

Files touched: `src/bun/client.ts` (AddPartitionsToTxn 24 / AddOffsetsToTxn 25 / TxnOffsetCommit 26 / EndTxn 28 APIs; `beginTransaction`/`commitTransaction`/`abortTransaction`/`sendOffsetsToTransaction`/`initProducerId` with retries; acks forced to all for transactional producers; close() best-effort abort), `src/bun/connection.ts` (nothing pending), `test/transactions.test.ts` (mock tests pass: commit flow API sequence [22,24,0,28], abort flag, sendOffsets [25,26]; real-broker tests FAIL — see below).

Also uncommitted: **OffsetCommit fixes** (see bugs below) — these are DONE and verified, just not committed yet. Commit them first!

### Bugs found & fixed (verify they stay fixed)

1. **OffsetCommit v2 retention_period_ms is INT64**, we sent INT32 → every real-broker group commit killed the connection ("applying protocol, short read"). Fixed: `.i64(-1n)` in `commitOffsets`.
2. **OffsetCommit RESPONSE throttle_time_ms exists only in v3+** (per spec; Redpanda omits it in v2). We over-read an i32 → "Malformed array". Fixed: no throttle read at v2. Mocks updated accordingly.
3. Metadata response parse assumed v2 shape (is_internal) — made official by requesting v2 everywhere.

### THE transaction blocker — analysis so far

Symptom: `InitProducerId` with a transactional_id returns `NOT_COORDINATOR` (16) forever against Redpanda, even though kafkajs transactions WORK against the same broker.

Facts established by source-diving Redpanda v25.2.1 (`/tmp/rp-src`):

- `tx_gateway_frontend.cc:init_tm_tx` (InitProducerId) does **NOT** create `kafka_internal/tx`; it only waits for it to exist, then returns NOT_COORDINATOR.
- `tx_gateway_frontend.cc:find_coordinator(tid)` **IS the on-demand creator** — it calls `tx_topic_manager.create_and_wait_for_coordinator_topic()`. It is reached only via FindCoordinator with `key_type=1 (TRANSACTION)`.
- Therefore a correct client MUST send FindCoordinator(key_type=transaction) BEFORE InitProducerId. kafkajs does exactly this (FC **v2**, non-flexible body `[key_type INT8][coordinator_key STRING]`, response `[throttle][error][coordinator][host][port]`) and succeeds.
- FindCoordinator v1/v2 advertise v0–v3 in ApiVersions but behave oddly here: v1+key_type=group returns polite `UNSUPPORTED_VERSION`(35); v1/v2+key_type=txn silently closes the connection when sent from our raw probes.

Current WIP adds `Cluster.findTxnCoordinator(id)` (FC v2) called at the top of `initProducerId()` when `transactionalId` is set, routing InitProducerId to the returned coordinator. **Latest observation**: with this change the real-broker probe now fails differently — `Kafka broker closed the connection` during the FC call (broker treats our FC v2 frame as malformed → "control character"/short-read logs), while kafkajs's equivalent request succeeds.

### Next steps (in order)

1. Commit the OffsetCommit fixes separately NOW.
2. Diff our FC v2 wire bytes vs kafkajs's. Capture kafkajs's exact bytes: patch `node_modules/kafkajs/src/network/connection.js` (in `/tmp/txn-check`) to hexdump outgoing payloads for apiKey 10, or proxy with advertised-address rewriting. Suspects:
   - kafkajs negotiates **ApiVersions v2** first (we send v0) — try upgrading our handshake;
   - kafkajs sends **Metadata v6** before FC — try adding a metadata call before FC;
   - compare clientId strings ("txn-check" vs "bun-kafka") — unlikely but free to test.
   Note: my earlier "flexible header" probe attempts were WRONG for v2 (v2 is NOT flexible) — don't repeat those.
3. Once FC-TXN works end-to-end, the existing `test/transactions.test.ts` real-broker cases should pass; also re-run `test/transactions.test.ts -t mock`.
4. Remove the temporary `DEBUG_COMMIT` block in `commitOffsets` before final commit.
5. Update `docs/feature-completeness.md` + `README.md` matrices for everything shipped.

## Remaining features after transactions

- **Fetch sessions** (incremental fetch, Fetch v7+ with session_id/session_epoch/forgotten_topics_data/rack_id). Needs multi-version serializer support; moderate effort. NOTE discovered earlier: Redpanda answers our Fetch **v4** with v4-shaped responses ([throttle][responses], no error_code/session_id — those arrive in v7+), which matches our existing parser; keep that parser when touching fetch.
- **Client quotas** (DescribeClientQuotas/AlterClientQuotas) and **Delegation tokens** — both require COMPACT string/varint flexible encoding helpers on Writer/Reader (not yet implemented). Medium effort.
- **Cooperative-sticky rebalancing** (KIP-429 two-phase protocol) — largest remaining item; currently we ship eager range assignment only.
- Docs update (README + feature matrix) as final commit.

## Key gotchas learned about THIS broker

- Silently closes connections on some malformed requests instead of erroring (e.g., null arrays, short bodies). Always suspect frame-shape bugs when you see "closed the connection" + `(applying protocol, short read)` in `docker logs bun-kafka-dev`.
- ApiVersions advertises ranges that are not fully honored (FC v1+ behaves unsupported; Fetch caps at v4-shaped responses).
- Internal topics are created ON DEMAND via specific API flows (FindCoordinator-TXN creates kafka_internal/tx); group offsets live in an internal raft store, no __consumer_offsets topic appears.
- Mock-broker tests can't catch frame-shape bugs where the mock mirrors the client's mistakes — always validate new request encodings against the real broker early.

## Useful debug artifacts

- `/tmp/probe-v2.ts`, `/tmp/fc-matrix.ts`, `/tmp/initpid-probe*.ts` – raw-socket protocol probes.
- `/tmp/txn-check/` – kafkajs reference environment (`check.ts` runs a working transaction; `proxy2.ts` logs C→S frames on :9094).
- `DEBUG_TXKEYS=1` env var logs every outgoing API key/version from our client (instrumentation currently present in `src/bun/connection.ts` — remove or keep behind env).
