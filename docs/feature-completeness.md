# Kafka feature completeness

## Verdict

The Bun-native client is a fast, zero-runtime-dependency implementation for these use cases:

- Produce record batches with leader or all-replica acknowledgements
- Read manually assigned partitions
- Read metadata and offsets
- Use TCP or TLS

It is **not a feature-complete general Kafka client**. The largest missing areas are consumer groups, offset commits, compression, SASL, request retries, idempotent production, transactions, and administration changes.

Use this document as a release snapshot. Other libraries can add or change features after this snapshot.

## Status terms

- **Yes:** Public API and integration coverage exist.
- **Partial:** A narrow form exists, or important production behavior is missing.
- **No:** The native client does not implement the feature.

## Implemented Kafka protocol APIs

The native client uses fixed, non-flexible protocol versions.

| Kafka API | Key | Version | Use |
|---|---:|---:|---|
| Produce | 0 | 3 | Send magic-2 record batches |
| Fetch | 1 | 4 | Fetch manually assigned partitions |
| ListOffsets | 2 | 1 | Resolve earliest, latest, and watermarks |
| Metadata | 3 | 1 | Discover brokers, topics, partitions, and leaders |

No ApiVersions negotiation occurs. The fixed versions require Kafka 0.11 or newer. A broker that removes or rejects one of these versions is not supported.

## Native client feature matrix

### Transport and protocol

| Feature | Status | Notes |
|---|---|---|
| TCP with `Bun.connect()` | Yes | One multiplexed connection per known broker in a shared `Kafka` instance |
| TLS | Yes | Uses Bun TLS options |
| Multiple bootstrap brokers | Partial | Metadata tries known and configured brokers; request retry is not automatic |
| Correlation IDs | Yes | Concurrent responses are matched to requests |
| Request timeout | Yes | Active requests reject after the configured timeout |
| Explicit TCP connect timeout | No | Bun or the operating system controls a blackholed connect attempt |
| Response size limit | Yes | `maxResponseBytes` limits frame allocation |
| Fragmented TCP frames | Yes | Exact-size response frame assembly |
| ApiVersions negotiation | No | API versions are fixed |
| Automatic leader metadata refresh | No | Leader errors reach the caller |
| Automatic request retries | No | Caller owns retry behavior |
| Retry backoff and jitter | No | Not applicable until retries exist |
| Broker throttling metrics | No | Throttle fields are read and discarded |
| Proxy support | No | No explicit proxy API |

### Producer

| Feature | Status | Notes |
|---|---|---|
| String, binary, and null values | Yes | `string`, `Uint8Array`, `ArrayBuffer`, or null |
| Keys | Yes | String, binary, or null |
| Headers | Yes | One value per header name in the public object API |
| Timestamps | Yes | Create-time record timestamp |
| Explicit partition | Yes | Validated against metadata |
| Key partitioning | Yes | Kafka-compatible Murmur2 |
| Keyless partitioning | Yes | Round robin |
| Automatic batching | Yes | Default 5 ms or 1,000 queued messages |
| Multi-topic batch | Yes | Queued sends are grouped by broker, topic, and partition |
| Flush on close | Yes | `close()` waits for queued Produce work |
| Leader acknowledgements | Yes | `acks: 1` |
| All-replica acknowledgements | Yes | `acks: "all"` |
| No acknowledgements | No | `acks: 0` is not exposed |
| CRC32C | Yes | Record batch CRC is generated |
| Delivery Promise | Yes | Resolves with partition Produce results |
| Per-message delivery callback | No | Promise result is partition-level |
| Automatic retry | No | Retriable errors are typed but not retried |
| Idempotent producer | No | No producer ID, epoch, or sequence state |
| Transactions | No | Transactional ID is always null |
| Compression | No | Compressed record batches are rejected by the consumer |
| Custom partitioner | No | No callback API |
| Quotas and throttle events | No | No public metric or event |

### Consumer

| Feature | Status | Notes |
|---|---|---|
| Manual assignment | Yes | Topic, partition, and offset |
| Assign all topic partitions | Yes | `subscribe()` is metadata-based manual assignment, not a group join |
| Earliest and latest offsets | Yes | Uses ListOffsets |
| Fetch batches | Yes | `fetch()` returns a bounded message array |
| Async iteration | Yes | Uses repeated bounded fetches |
| Zero-copy payload views | Yes | Default; views remain stable |
| Optional payload copies | Yes | `fetch({ copy: true })` |
| Bounded decode | Yes | Stops object creation at `maxMessages` and keeps a decoder cursor |
| Headers | Yes | Decoded into an object |
| Seek | Yes | Clears pending decoder state for that partition |
| Pause and resume | Yes | Manual partitions |
| Position | Yes | Local next offset |
| Watermarks | Yes | Earliest and latest offsets |
| Consumer groups | No | No coordinator, join, sync, heartbeat, or leave |
| Offset commit and fetch | No | No group offset API |
| Rebalancing | No | No eager or cooperative rebalance |
| Static membership | No | No group membership |
| Read committed isolation | No | Fetch uses read-uncommitted isolation |
| Fetch sessions | No | Full Fetch requests only |
| Regex subscription | No | No group subscription protocol |
| Rack-aware fetching | No | Rack metadata is not used |
| Deserializers | No | Application owns value decoding |

### Administration

| Feature | Status | Notes |
|---|---|---|
| Broker metadata | Yes | IDs, hosts, and ports |
| Topic metadata | Yes | Errors, partitions, and leaders |
| Cluster ID | No | Metadata v1 does not return it |
| Create topics | No | Current tests rely on broker auto-creation |
| Delete topics | No | — |
| Create partitions | No | — |
| Describe or alter configs | No | — |
| ACL operations | No | — |
| Consumer group administration | No | — |
| Topic offset administration | Partial | Earliest/latest watermarks only |
| Delegation tokens | No | — |
| Quota administration | No | — |

### Security

| Feature | Status | Notes |
|---|---|---|
| TLS server authentication | Yes | Bun TLS validation |
| Custom CA | Yes | Bun TLS option |
| Mutual TLS | Yes | Certificate and key options |
| SASL/PLAIN | No | — |
| SASL/SCRAM-SHA-256 | No | — |
| SASL/SCRAM-SHA-512 | No | — |
| SASL/OAUTHBEARER | No | — |
| Kerberos/GSSAPI | No | — |
| Credential refresh | No | — |

### Reliability and operations

| Feature | Status | Notes |
|---|---|---|
| Typed Kafka errors | Partial | Code, fatal flag, and retriable flag; error catalog is incomplete |
| Reconnect after socket close | Partial | A later request can reconnect; failed operation is not retried |
| Graceful producer flush | Yes | Queued sends flush on close |
| Graceful consumer close | Yes | No group leave is needed because groups are absent |
| Statistics callback | No | — |
| Request logging hooks | No | — |
| Metrics API | No | — |
| OpenTelemetry hooks | No | — |
| Broker throttle event | No | — |
| Health check API | No | Metadata can be used by the application |
| Performance soak evidence | No | Short benchmark exists; see performance validation plan |
| Chaos qualification | No | See broker failure plan |

## Comparison with other clients

This comparison covers common public features. It does not compare every configuration property.

| Feature | bun-kafka native | franz-go | rust-rdkafka | KafkaJS |
|---|---|---|---|---|
| Runtime native to implementation language | Bun APIs and TypeScript | Go | Rust wrapper over librdkafka | JavaScript on Node-compatible APIs |
| Runtime library dependencies | None | Go modules at build time | librdkafka/native build | Package dependencies |
| Basic Produce and Fetch | Yes | Yes | Yes | Yes |
| Automatic producer batching | Yes | Yes | Yes | Yes |
| Compression | No | Yes | Yes | Yes, codec support depends on setup |
| Idempotent producer | No | Yes | Yes | Yes |
| Transactions | No | Yes | Yes | Yes |
| Automatic retries | No | Yes | Yes | Yes |
| Metadata refresh after leader movement | No | Yes | Yes | Yes |
| Consumer groups | No | Yes | Yes | Yes |
| Cooperative rebalancing | No | Yes | Yes | Yes |
| Manual assignment | Yes | Yes | Yes | Limited compared with native clients |
| Offset commits | No | Yes | Yes | Yes |
| SASL/PLAIN and SCRAM | No | Yes | Yes | Yes |
| OAuth bearer | No | Yes | Yes | Yes |
| TLS and mutual TLS | Yes | Yes | Yes | Yes |
| Broad admin API | No | Yes | Yes | Yes |
| Protocol version negotiation | No | Yes | Yes | Yes |
| Metrics and hooks | No | Yes | Yes | Yes |
| Zero-copy Bun response views | Yes | Not applicable | Native message ownership | No equivalent Bun-specific API |
| No native addon or shared library | Yes | Compiled Go binary | No | Yes |

Library capabilities and defaults can change. Verify the exact version before a production selection.

## Recorded performance comparison

The current local, fresh-topic benchmark uses 5,000 100-byte records, one partition, manual consumption, and leader acknowledgements.

| Client | End-to-end wall time | Produce median | Consume median |
|---|---:|---:|---:|
| franz-go | 210.7 ms | 24,543 msg/s | 2,196,377 msg/s |
| bun-kafka | 239.5 ms | 22,691 msg/s | 621,956 msg/s |
| rdkafka-rust | 718.5 ms | 24,486 msg/s | 9,923 msg/s |

The Rust comparison uses `BaseConsumer::poll` one message at a time. The Go and Bun comparisons drain fetched batches. These values compare the tested client APIs, not language speed.

See [benchmarks.md](benchmarks.md) and [performance-validation.md](performance-validation.md).

## Selection guidance

Choose the Bun-native client when all these statements are true:

- The service runs on Bun 1.4 or newer.
- Zero runtime dependencies are a hard requirement.
- Manual partition ownership is acceptable.
- TLS or trusted plaintext is sufficient.
- The application can handle visible request failures.
- The application does not require compressed records.
- The application can tolerate duplicates when it retries an unknown Produce result.

Choose a more complete client when any of these statements are true:

- The service needs consumer groups or automatic rebalancing.
- The service needs committed offsets.
- The cluster requires SASL.
- The service requires compression.
- The producer requires idempotence or transactions.
- The client must recover from leader movement without application action.
- The service requires a broad admin API.
- The release requires existing soak and chaos evidence.

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

### Milestone 2: standard service client

Required features:

- Consumer coordinator discovery
- JoinGroup, SyncGroup, Heartbeat, and LeaveGroup
- OffsetCommit and OffsetFetch
- Eager and cooperative rebalancing
- SASL/PLAIN and SCRAM
- Gzip compression through Bun primitives
- Idempotent producer

### Milestone 3: advanced Kafka client

Required features:

- Transactions and read-committed consumption
- More compression codecs
- Static membership
- Fetch sessions
- Broad admin APIs
- OAuth bearer support
- Full metrics and tracing hooks

## Production-readiness summary

| Area | Current verdict |
|---|---|
| Narrow manual produce/consume feature set | Implemented |
| Short-run speed | Competitive in the recorded benchmark |
| Long-run performance proof | Not complete |
| Broker-failure proof | Not complete |
| General Kafka feature completeness | Not complete |
| Zero-dependency Bun-native goal | Met |
