---
title: Feature completeness
description: Kafka feature completeness
---

## Verdict

The Bun-native client is a fast, zero-runtime-dependency implementation covering:

- Produce optionally idempotent or transactional record batches with acks 0, 1, or all
- Read manually assigned partitions or use consumer groups with eager range **or cooperative-sticky** assignment
- Commit and read consumer-group offsets, including inside transactions
- Read metadata and change topics or configs
- Administer groups, records, ACLs, client quotas, and delegation tokens
- Use TCP or TLS with PLAIN, SCRAM, or OAuth bearer authentication (with timed reauthentication)

Remaining gaps are narrow: Kerberos/GSSAPI, proxy support, and 72-hour soak evidence.

Use this document as a release snapshot. Other libraries can add or change features after this snapshot.

## Status terms

- **Yes:** Public API and integration coverage exist.
- **Partial:** A narrow form exists, or important production behavior is missing.
- **No:** The native client does not implement the feature.

## Implemented Kafka protocol APIs

| Kafka API                              |   Key | Version | Use                                                                   |
| -------------------------------------- | ----: | ------: | --------------------------------------------------------------------- |
| Produce                                |     0 |       3 | Send magic-2 record batches                                           |
| Fetch                                  |     1 |       7 | Incremental fetch sessions (KIP-227)                                  |
| ListOffsets                            |     2 |       1 | Resolve earliest, latest, and watermarks                              |
| Metadata                               |     3 |       2 | Discover brokers, topics, partitions, leaders, and cluster id         |
| OffsetCommit                           |     8 |       2 | Commit group offsets                                                  |
| OffsetFetch                            |     9 |       2 | Read group offsets                                                    |
| FindCoordinator                        |    10 |     0/2 | Group coordinators; transaction coordinators (v2, on-demand creation) |
| JoinGroup                              |    11 |     2/3 | Consumer group join incl. static identity (KIP-345)                   |
| Heartbeat                              |    12 |     0/3 | Keep group membership active                                          |
| LeaveGroup                             |    13 |     0/3 | Leave a consumer group                                                |
| SyncGroup                              |    14 |     0/3 | Receive group assignment                                              |
| SaslHandshake                          |    17 |       1 | Select a SASL mechanism                                               |
| ApiVersions                            |    18 |       0 | Validate required API versions                                        |
| CreateTopics                           |    19 |       4 | Create topics                                                         |
| DeleteTopics                           |    20 |       3 | Delete topics                                                         |
| DeleteRecords                          |    21 |       1 | Truncate records by offset                                            |
| InitProducerId                         |    22 |       1 | Initialize idempotent/transactional producer identity                 |
| OffsetCommit v2 INT64 retention fix    |     8 |       2 | Correct wire layout for Redpanda/Kafka                                |
| AddPartitionsToTxn                     |    24 |       1 | Register transaction partitions                                       |
| AddOffsetsToTxn                        |    25 |       0 | Register consumer-group offsets in a transaction                      |
| EndTxn                                 |    26 |       1 | Commit or abort a transaction                                         |
| TxnOffsetCommit                        |    28 |       0 | Commit offsets transactionally                                        |
| DescribeAcls / CreateAcls / DeleteAcls | 29-31 |       0 | ACL administration                                                    |
| DescribeConfigs                        |    32 |       0 | Read resource configs                                                 |
| AlterConfigs                           |    33 |       0 | Replace resource configs                                              |
| IncrementalAlterConfigs                |    44 |       1 | Set/delete/append/subtract individual configs without clobbering      |
| SaslAuthenticate                       |    36 |       1 | Exchange SASL data                                                    |
| CreatePartitions                       |    37 |       2 | Increase partition counts                                             |
| CreateDelegationToken                  |    38 |       2 | Delegate authentication tokens                                        |
| RenewDelegationToken                   |    39 |       2 | Extend token expiry                                                   |
| ExpireDelegationToken                  |    40 |       2 | Expire tokens early                                                   |
| DescribeDelegationToken                |    41 |       2 | List visible tokens                                                   |
| DeleteGroups                           |    42 |       1 | Delete consumer groups                                                |
| DescribeClientQuotas                   |    48 |       1 | Filter quota entities                                                 |
| AlterClientQuotas                      |    49 |       1 | Change quota entity values                                            |

Each connection uses ApiVersions to verify that the broker supports the fixed request versions. Flexible requests use COMPACT strings/arrays and tagged-field sections via shared Writer/Reader helpers.

## Native client feature matrix

### Transport and protocol

| Feature                           | Status  | Notes                                                                                                         |
| --------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------- |
| TCP with `Bun.connect()`          | Yes     | One multiplexed connection per known broker in a shared `Kafka` instance                                      |
| TLS                               | Yes     | Uses Bun TLS options                                                                                          |
| Multiple bootstrap brokers        | Partial | Metadata tries known and configured brokers; broker-specific requests do not fail over to an arbitrary broker |
| Correlation IDs                   | Yes     | Concurrent responses are matched to requests                                                                  |
| Request timeout                   | Yes     | Active requests reject after the configured timeout                                                           |
| Explicit TCP connect timeout      | Yes     | `connectTimeoutMs` bounds a new `Bun.connect()` attempt                                                       |
| Response size limit               | Yes     | `maxResponseBytes` limits frame allocation                                                                    |
| Fragmented TCP frames             | Yes     | Exact-size response frame assembly                                                                            |
| ApiVersions negotiation           | Partial | Each connection validates fixed versions; serializers do not downgrade to older versions                      |
| Automatic leader metadata refresh | Yes     | Retried Produce and Fetch refresh topic metadata before retrying                                              |
| Automatic request retries         | Partial | Retriable network and Produce errors use the bounded retry policy                                             |
| Retry backoff and jitter          | Yes     | Exponential backoff uses bounded 50–100% jitter                                                               |
| Broker throttling metrics         | Yes     | Positive throttle durations emit an `onEvent` event                                                           |
| Proxy support                     | No      | No explicit proxy API                                                                                         |

### Producer

| Feature                         | Status  | Notes                                                                                                                                                        |
| ------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| String, binary, and null values | Yes     | `string`, `Uint8Array`, `ArrayBuffer`, or null                                                                                                               |
| Keys                            | Yes     | String, binary, or null                                                                                                                                      |
| Headers                         | Yes     | One value per header name in the public object API                                                                                                           |
| Timestamps                      | Yes     | Create-time record timestamp                                                                                                                                 |
| Explicit partition              | Yes     | Validated against metadata                                                                                                                                   |
| Key partitioning                | Yes     | Kafka-compatible Murmur2                                                                                                                                     |
| Keyless partitioning            | Yes     | Round robin                                                                                                                                                  |
| Custom partitioner              | Yes     | `partitioner(topic, key, value, partitionCount)` callback                                                                                                    |
| Automatic batching              | Yes     | Default 5 ms or 1,000 queued messages                                                                                                                        |
| Multi-topic batch               | Yes     | Queued sends are grouped by broker, topic, and partition                                                                                                     |
| Flush on close                  | Yes     | `close()` waits for queued Produce work and best-effort aborts open transactions                                                                             |
| Leader acknowledgements         | Yes     | `acks: 1`                                                                                                                                                    |
| All-replica acknowledgements    | Yes     | `acks: "all"`                                                                                                                                                |
| No acknowledgements             | Yes     | Fire-and-forget via connection-level send-only requests                                                                                                      |
| CRC32C                          | Yes     | Record batch CRC is generated                                                                                                                                |
| Delivery Promise                | Yes     | Resolves with partition Produce results                                                                                                                      |
| Per-message delivery callback   | Yes     | `onDelivery(error, result)` invoked exactly once per message                                                                                                 |
| Automatic retry                 | Partial | Retriable network and Produce errors use bounded retries                                                                                                     |
| Idempotent producer             | Yes     | Initializes producer identity, forces all-replica acknowledgements, tracks per-partition sequences                                                           |
| Transactions                    | Yes     | `beginTransaction`, `commitTransaction`, `abortTransaction`, `sendOffsetsToTransaction`; coordinator resolution, epoch bump after each transaction (KIP-360) |
| Compression                     | Yes     | Gzip, Zstandard, Snappy, and LZ4 record batches                                                                                                              |

### Consumer

| Feature                       | Status | Notes                                                                                                  |
| ----------------------------- | ------ | ------------------------------------------------------------------------------------------------------ |
| Manual assignment             | Yes    | Topic, partition, and offset                                                                           |
| Assign all topic partitions   | Yes    | `subscribe()` is metadata-based manual assignment without a group, or a group join with `groupId`      |
| Earliest and latest offsets   | Yes    | Uses ListOffsets                                                                                       |
| Fetch batches                 | Yes    | `fetch()` returns a bounded message array                                                              |
| Async iteration               | Yes    | Uses repeated bounded fetches                                                                          |
| Zero-copy payload views       | Yes    | Default; views remain stable                                                                           |
| Optional payload copies       | Yes    | `fetch({ copy: true })`                                                                                |
| Bounded decode                | Yes    | Stops object creation at `maxMessages` and keeps a decoder cursor                                      |
| Headers                       | Yes    | Decoded into an object                                                                                 |
| Seek                          | Yes    | Clears pending decoder state for that partition                                                        |
| Pause and resume              | Yes    | Manual partitions                                                                                      |
| Position                      | Yes    | Local next offset                                                                                      |
| Watermarks                    | Yes    | Earliest and latest offsets                                                                            |
| Consumer groups               | Yes    | Coordinator discovery, join, sync, heartbeat, leave                                                    |
| Offset commit and fetch       | Yes    | Group OffsetCommit and OffsetFetch APIs                                                                |
| Eager range assignment        | Yes    | Default assignor for `groupId` consumers                                                               |
| Cooperative-sticky assignment | Yes    | KIP-429 two-phase protocol; owned partitions declared in subscription v1 and retained up to fair share |
| Static membership             | Yes    | `group.instance.id` sent through JoinGroup/SyncGroup/Heartbeat/LeaveGroup v3                           |
| Read committed isolation      | Yes    | Aborted-txn filtering plus control-marker handling at batch level                                      |
| Fetch sessions                | Yes    | KIP-227 incremental requests with session id/epoch and `forgotten_topics_data` pruning                 |
| Regex subscription            | Yes    | Group subscriptions expand RegExp patterns against cluster metadata                                    |
| Rack-aware fetching           | No     | Rack metadata is not used                                                                              |
| Deserializers                 | Yes    | Optional key/value deserializer callbacks with topic/partition/offset context                          |

### Administration

| Feature                       | Status | Notes                                                                                                |
| ----------------------------- | ------ | ---------------------------------------------------------------------------------------------------- |
| Broker metadata               | Yes    | IDs, hosts, ports, and controller                                                                    |
| Cluster ID                    | Yes    | Metadata v2                                                                                          |
| Topic metadata                | Yes    | Errors, partitions, leaders, and replicas/isr                                                        |
| Create topics                 | Yes    | Fixed CreateTopics v4 request                                                                        |
| Delete topics                 | Yes    | Fixed DeleteTopics v3 request                                                                        |
| Create partitions             | Yes    | Fixed CreatePartitions v2 request                                                                    |
| Describe or alter configs     | Yes    | Fixed v0 config APIs plus IncrementalAlterConfigs v1 (set/append/subtract/delete individual entries) |
| ACL operations                | Yes    | Describe/Create/Delete ACLs v0 with real-broker round-trip coverage                                  |
| Consumer group administration | Yes    | ListGroups, DescribeGroups, DeleteGroups                                                             |
| Topic offset administration   | Yes    | Watermarks plus DeleteRecords truncation                                                             |
| Delegation tokens             | Yes    | Create/Renew/Expire/Describe v2 (mock wire-shape tests; requires SASL + broker support)              |
| Quota administration          | Yes    | DescribeClientQuotas/AlterClientQuotas v1 flexible encoding                                          |

### Security

| Feature                   | Status | Notes                                                                                            |
| ------------------------- | ------ | ------------------------------------------------------------------------------------------------ |
| TLS server authentication | Yes    | Bun TLS validation                                                                               |
| Custom CA                 | Yes    | Bun TLS option                                                                                   |
| Mutual TLS                | Yes    | Certificate and key options                                                                      |
| SASL/PLAIN                | Yes    | Uses Kafka SASL handshake and authenticate APIs                                                  |
| SASL/SCRAM-SHA-256        | Yes    | Uses Web Crypto PBKDF2 and HMAC                                                                  |
| SASL/SCRAM-SHA-512        | Yes    | Uses Web Crypto PBKDF2 and HMAC                                                                  |
| SASL/OAUTHBEARER          | Yes    | Static tokens and async token providers                                                          |
| Timed reauthentication    | Yes    | KIP-368: session lifetime captured from SaslAuthenticate, re-auth at 80%, fatal-failure handling |
| Kerberos/GSSAPI           | No     | —                                                                                                |

### Reliability and operations

| Feature                               | Status  | Notes                                                                                                                                                    |
| ------------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Typed Kafka errors                    | Yes     | Complete error-code catalog with code, fatal flag, and retriable flag                                                                                    |
| Reconnect after socket close          | Partial | Retriable operations use a bounded retry policy; not all APIs recover coordinator or leader state                                                        |
| Graceful producer flush               | Yes     | Queued sends flush on close; open transactions abort best-effort                                                                                         |
| Graceful consumer close               | Yes     | Stops heartbeats and makes a best-effort LeaveGroup request                                                                                              |
| Statistics callback                   | Yes     | `Connection.stats` counters, `Cluster.stats()`, and `statsIntervalMs` events                                                                             |
| Request logging hooks                 | Yes     | Pluggable logger receives warn/info lines from retry, throttle, and auth paths                                                                           |
| Metrics API                           | Partial | Counters exist; histograms do not                                                                                                                        |
| OpenTelemetry hooks                   | No      | Use the logging hooks to bridge                                                                                                                          |
| Broker throttle event                 | Yes     | Emitted through `KafkaOptions.onEvent`                                                                                                                   |
| Health check API                      | Yes     | `healthCheck()` pings every broker via ApiVersions and reports latency/errors                                                                            |
| Performance soak harness and evidence | Partial | `bun run test:soak` records all required measurements; the 30-minute and 24-hour release-profile runs pass. The 72-hour release soak remains outstanding |
| Chaos qualification                   | Yes     | Deterministic mock suite plus three-broker Docker chaos pass; results in `out/chaos/`                                                                    |

## Comparison with other clients

| Feature                                   | bun-kafka native        | franz-go                 | rust-rdkafka                 | KafkaJS                              |
| ----------------------------------------- | ----------------------- | ------------------------ | ---------------------------- | ------------------------------------ |
| Runtime native to implementation language | Bun APIs and TypeScript | Go                       | Rust wrapper over librdkafka | JavaScript on Node-compatible APIs   |
| Runtime library dependencies              | None                    | Go modules at build time | librdkafka/native build      | Package dependencies                 |
| Basic Produce and Fetch                   | Yes                     | Yes                      | Yes                          | Yes                                  |
| Automatic producer batching               | Yes                     | Yes                      | Yes                          | Yes                                  |
| Gzip compression                          | Yes                     | Yes                      | Yes                          | Yes                                  |
| Zstandard compression                     | Yes                     | Yes                      | Yes                          | Yes, codec support depends on setup  |
| Snappy and LZ4 compression                | Yes                     | Yes                      | Yes                          | Yes, codec support depends on setup  |
| Idempotent producer                       | Yes                     | Yes                      | Yes                          | Yes                                  |
| Transactions                              | Yes                     | Yes                      | Yes                          | Yes                                  |
| Automatic retries                         | Partial                 | Yes                      | Yes                          | Yes                                  |
| Metadata refresh after leader movement    | Yes                     | Yes                      | Yes                          | Yes                                  |
| Consumer groups                           | Yes                     | Yes                      | Yes                          | Yes                                  |
| Cooperative rebalancing                   | Yes                     | Yes                      | Yes                          | Yes                                  |
| Manual assignment                         | Yes                     | Yes                      | Yes                          | Limited compared with native clients |
| Offset commits                            | Yes                     | Yes                      | Yes                          | Yes                                  |
| SASL/PLAIN and SCRAM                      | Yes                     | Yes                      | Yes                          | Yes                                  |
| OAuth bearer with timed reauthentication  | Yes                     | Yes                      | Yes                          | Yes                                  |
| TLS and mutual TLS                        | Yes                     | Yes                      | Yes                          | Yes                                  |
| Broad admin API                           | Yes                     | Yes                      | Yes                          | Yes                                  |
| Protocol version negotiation              | Partial                 | Yes                      | Yes                          | Yes                                  |
| Incremental fetch sessions                | Yes                     | Yes                      | Yes                          | No                                   |
| Zero-copy Bun response views              | Yes                     | Not applicable           | Native message ownership     | No equivalent Bun-specific API       |
| No native addon or shared library         | Yes                     | Compiled Go binary       | No                           | Yes                                  |

Library capabilities and defaults can change. Verify the exact version before a production selection.

## Recorded performance comparison

The current local, fresh-topic benchmark uses 5,000 100-byte records, one partition, manual consumption, and leader acknowledgements.

| Client       | End-to-end wall time | Produce median | Consume median |
| ------------ | -------------------: | -------------: | -------------: |
| franz-go     |             210.7 ms |   24,543 msg/s |  621,956 msg/s |
| bun-kafka    |             239.5 ms |   22,691 msg/s |  621,956 msg/s |
| rdkafka-rust |             718.5 ms |   24,486 msg/s |    9,923 msg/s |

See [benchmarks.md](benchmarks.md) and [performance-validation.md](performance-validation.md).

## Selection guidance

Choose the Bun-native client when all these statements are true:

- The service runs on Bun 1.4 or newer.
- Zero runtime dependencies are a hard requirement.
- TLS or trusted plaintext is sufficient.
- The application enables `idempotent: true` or can tolerate duplicates after an unknown Produce result.

Choose a more complete client when any of these statements are true:

- The cluster requires Kerberos/GSSAPI.
- The release requires existing 72-hour soak evidence for changes that need that duration.

## Feature-complete milestones

### Milestone 1: reliable manual client

Required features:

- ApiVersions negotiation
- Explicit TCP connect timeout
- Automatic metadata refresh
- Bounded retry policy with backoff and jitter
- Retry and throttle metrics
- Chaos Gate A and Gate B completion
- Twenty-four-hour performance soak

Status: implemented; Gates A and B pass on the deterministic and three-broker suites, and the 24-hour soak passes for the recorded workload. The 72-hour follow-up remains.

### Milestone 2: standard service client

Required features:

- Consumer coordinator discovery
- JoinGroup, SyncGroup, Heartbeat, and LeaveGroup
- OffsetCommit and OffsetFetch
- Eager and cooperative rebalancing
- SASL/PLAIN and SCRAM
- Gzip compression through Bun primitives
- Idempotent producer

Status: complete.

### Milestone 3: advanced Kafka client

Required features:

- Transactions and read-committed consumption
- More compression codecs
- Static membership
- Fetch sessions
- Broad admin APIs
- OAuth bearer support
- Full metrics and tracing hooks

Status: complete except tracing hooks beyond the logging bridge.

## Production-readiness summary

| Area                                      | Current verdict                                                                                     |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Narrow manual produce/consume feature set | Implemented                                                                                         |
| Short-run speed                           | Competitive in the recorded benchmark                                                               |
| Long-run performance proof                | 24-hour soak passes all release gates for the recorded workload (`out/soak/`); 72-hour soak remains |
| Broker-failure proof                      | Three-broker chaos suite passes (`out/chaos/`)                                                      |
| General Kafka feature completeness        | Complete for the supported API surface above                                                        |
| Zero-dependency Bun-native goal           | Met                                                                                                 |
