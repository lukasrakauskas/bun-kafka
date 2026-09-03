# Client gap audit

A line-by-line look at what kafkajs (pure JS), node-rdkafka (librdkafka binding), and franz-go
(Go) can do that bun-kafka cannot, with an explicit disposition for every finding. Companion to
the implementation snapshot in [feature completeness](feature-completeness.md); the protocol/KIP
background lives in [versions and KIPs](kafka-versions-and-kips.md).

Dispositions:

- **Fixed** — implemented since this audit opened.
- **Won't** — deliberately out of scope, with the reason.
- **Deferred** — reasonable future work; the trigger condition to start is stated.
- **Documented** — behavior differs from other clients by design; users must know.

## Transport and protocol

| Finding (who has it)                                                                                                         | Status here                                                                                           | Disposition                                                                                                                                                                   |
| ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Per-broker API-version downgrade with multiple serializers per API (franz-go, kafkajs, node-rdkafka)                         | Fixed conservative versions, validated per connection via ApiVersions, fail-fast typed error          | Deferred. Full design + trigger conditions in [versions and KIPs §5](kafka-versions-and-kips.md). No supported broker (≥ 0.11, 2017) lacks any fixed version used             |
| SOCKS/HTTP proxy connections (node-rdkafka via librdkafka settings)                                                          | No proxy option                                                                                       | Won't until requested: `Bun.connect()` exposes no proxy hook; a manual SOCKS5 preamble would bypass Bun TLS integration. VPN/sidecar covers the deployment need today         |
| Kerberos/GSSAPI SASL (node-rdkafka, franz-go via cgo/system libs)                                                            | Not implemented                                                                                       | Won't: GSSAPI needs OS credential caches and native libraries, incompatible with the zero-dependency pure-TS goal. Use SCRAM-SHA-512, OAUTHBEARER, mTLS, or delegation tokens |
| Automatic reconnect loop with backoff while idle (all three)                                                                 | Connections re-establish lazily on the next request; bounded retries cover in-flight operations       | Documented: no background reconnect timer exists; first request after a broker outage pays one reconnect. Behavior matches kafkajs closer than librdkafka                     |
| Failover of broker-specific requests to another broker holding the data (franz-go refreshes leaders transparently mid-retry) | Produce/Fetch refresh topic metadata and retry against the new leader; coordinator lookups re-resolve | Documented: failover happens through the bounded retry policy (`retry.maxRetries`), not an unbounded loop                                                                     |

## Producer

| Finding                                                                                      | Status here                                             | Disposition                                                                                                                                                        |
| -------------------------------------------------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Configurable gzip/zstd compression **levels** (franz-go, kafkajs codec opts)                 | Codec choice yes; level knob no                         | Deferred. Bun's native gzip/zstd codecs don't accept a level parameter portably; revisit when Bun exposes one. Default levels are the codecs' own (zstd 3, gzip 6) |
| Unbounded send queue with memory-based flush thresholds (librdkafka `queue.buffering.max.*`) | Bounded by `batchMaxMessages`; `queuedMessages` exposed | Documented: backpressure surfaces as promise latency instead of unbounded buffering — safer default for services                                                   |
| Sticky partitioner for keyless batches (franz-go sticky, kafkajs strict round-robin)         | Round-robin keyless; custom `partitioner` hook          | Deferred: sticky batching helps tail latency at very high keyless rates; the hook lets users supply their own today                                                |

## Consumer

| Finding                                                                                               | Status here                                                                                                                                                                | Disposition                                                                                                                                                                |
| ----------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Next-generation consumer protocol (KIP-848, GA Kafka 4.0) — all mature clients are adopting           | Classic group protocol (JoinGroup/SyncGroup/Heartbeat)                                                                                                                     | Deferred. Every current broker runs both protocols side-by-side; adopt once KIP-848 client libraries stabilize across vendors. Tracked as the largest future protocol item |
| Rack-aware fetching (KIP-881; franz-go `ClientOpts.rack`, librdkafka `client.rack`)                   | Not sent                                                                                                                                                                   | Deferred until a multi-AZ user needs egress-cost savings; requires Fetch request rack field + replica selection and real cross-rack testing                                |
| Long-lived blocking iteration driven by background fetch pipeline (kafkajs `run()`, librdkafka queue) | Pull-style `fetch()` / async iterator with bounded batches; kafkajs-shaped `run()`/`eachBatch` lives in [kafkajs-compat](../../packages/kafka/src/kafkajs-compat/index.ts) | Documented: pull model keeps memory bounded and backpressure explicit                                                                                                      |

## Administration

| Finding                                                                                                           | Status here                                                                                                                                                                                                                    | Disposition                                                                                                             |
| ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| IncrementalAlterConfigs (append/remove single values without clobbering, Apache Kafka 2.3+)                       | ~~AlterConfigs v0 replace-mode only~~ **Fixed**: `admin.incrementalAlterConfigs()` speaks IncrementalAlterConfigs v1 flexible and supports set/append/subtract/delete plus `validateOnly`; mock coverage pins all four operations, while live-broker coverage verifies set, non-clobbering updates, delete, and per-resource errors | Resolved in this release cycle. Requires Apache Kafka 2.3+ / current Redpanda; older brokers answer UNSUPPORTED_VERSION |
| Partition reassignment, leader election (KIP-460), DescribeLogDirs (franz-go, kafkajs via Confluent REST usually) | Partition reassignment and leader election are implemented with typed per-partition results; DescribeLogDirs remains absent                                                                                                                            | Reassignment/election resolved; DescribeLogDirs remains deferred                                                        |
| Offset lookup by max timestamp, ListOffsets variants                                                              | earliest/latest/timestamp covered via ListOffsets v1 + watermarks                                                                                                                                                              | Fixed enough for all documented use cases                                                                               |

## Observability and operations

| Finding                                                             | Status here                                                                                              | Disposition                                                                                                                                                                           |
| ------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Histogram latency metrics (librdkafka stats JSON, franz-go metrics) | Counters + p95/p99 sampled in soak harness; `statsIntervalMs` events; `healthCheck()` per-broker latency | Deferred: histograms belong to the app's metrics system; export counters there. Soak harness shows exactly which samples matter ([performance validation](performance-validation.md)) |
| First-class OpenTelemetry hooks                                     | Pluggable logger bridge; external suites prove header-context propagation works through the compat layer | Deferred: thin wrapper risk of wrong span semantics; documented recipe instead                                                                                                        |
| 24-hour/72-hour soak evidence                                       | 30-minute and 24-hour release-profile soaks pass and are recorded; the 72-hour follow-up remains         | Resolved for the 24-hour gate. Run the 72-hour follow-up for releases that require the longer duration                                                                                |

## Release engineering

| Finding                                                                                      | Status                                                                                                    | Disposition                 |
| -------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- | --------------------------- |
| npm package hygiene (`files` whitelist, repository metadata) so published tarballs stay lean | Being fixed alongside this audit                                                                          | Fixed in this release cycle |
| CI gates green before publish                                                                | Typecheck + unit/features coverage gate + Redpanda integration + chaos mock in `.github/workflows/ci.yml` | In place                    |

## Verdict

No gap above blocks a production release for the documented scope (Bun ≥ 1.4, Kafka/Redpanda ≥
0.11, PLAIN/SCRAM/OAUTHBEARER/mTLS auth). The remaining items to schedule are the KIP-848
adoption decision (ecosystem timing) and the 72-hour follow-up soak before releases that touch
protocol/connection/producer/consumer code.
