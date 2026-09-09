# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0](https://github.com/lukasrakauskas/bun-kafka/compare/v0.1.0...v0.2.0) (2026-09-09)


### Features

* add rack-aware fetching ([7f4d506](https://github.com/lukasrakauskas/bun-kafka/commit/7f4d506a3d60b2e84ff63ad965a4c09edddf9d42))
* **admin:** add partition reassignment and leader election ([88b1a1e](https://github.com/lukasrakauskas/bun-kafka/commit/88b1a1e84aea1dc37b74df386da3e2a6d20fbe68))
* **consumer:** support KIP-848 group protocol ([392b834](https://github.com/lukasrakauskas/bun-kafka/commit/392b8342b0a68e68451ff1b4d905ac6d63d7f342))


### Bug Fixes

* adapt KafkaJS partitioner factories ([9ae8f72](https://github.com/lukasrakauskas/bun-kafka/commit/9ae8f72f43806e8b04223cf3f682f30fd3b2324b))
* adapt KafkaJS partitioner factories ([c60082b](https://github.com/lukasrakauskas/bun-kafka/commit/c60082bc1ae55e625ab8f81207ef854b32e8e513))
* **consumer:** recover initial group heartbeats ([0f0ab15](https://github.com/lukasrakauskas/bun-kafka/commit/0f0ab1581700234d92cdcdf3fd0bcff476fd8c0d))
* **consumer:** retry group coordinator discovery ([ad8ff31](https://github.com/lukasrakauskas/bun-kafka/commit/ad8ff3169b17a1a0e8c5387f73609edaa89cfbd7))
* emit truthful KafkaJS compatibility events ([b0c7db6](https://github.com/lukasrakauskas/bun-kafka/commit/b0c7db63b7e2b859b0f9f2754ee3afd156180a6b))
* emit truthful KafkaJS compatibility events ([eaac92e](https://github.com/lukasrakauskas/bun-kafka/commit/eaac92e110d102d6eff0cfcbcaf09cf7c4b80cf4))
* **producer:** release completed send payloads ([#62](https://github.com/lukasrakauskas/bun-kafka/issues/62)) ([9e1173e](https://github.com/lukasrakauskas/bun-kafka/commit/9e1173e02d08fc5739d547568f73b7bed2fc77de))
* release describeGroup admin on failure ([35628d1](https://github.com/lukasrakauskas/bun-kafka/commit/35628d11d8b865e66522380dae00735d8b5ccfc3))
* support alterConfigs validateOnly ([462f0e6](https://github.com/lukasrakauskas/bun-kafka/commit/462f0e629bbe06e046d7af61fbcb2f568d6a6bf7))
* support AlterConfigs validateOnly ([247ccf2](https://github.com/lukasrakauskas/bun-kafka/commit/247ccf269adb9b0a7cb9aa4a97fa50aba36027e0))
* **transport:** preserve request frames under TCP backpressure ([#58](https://github.com/lukasrakauskas/bun-kafka/issues/58)) ([35fac0a](https://github.com/lukasrakauskas/bun-kafka/commit/35fac0ad944f816040df5a8f6561b273a48bd0a5))
* type-check KafkaJS compatibility export ([f4dff57](https://github.com/lukasrakauskas/bun-kafka/commit/f4dff57f2f5087f7217fda16b148eddc0067f605))
* type-check KafkaJS compatibility export ([ff0e8a7](https://github.com/lukasrakauskas/bun-kafka/commit/ff0e8a7d105d0f22f04bc491cfed0ca904ed8336))


### Performance Improvements

* **consumer:** avoid per-record work for raw decoded batches ([#63](https://github.com/lukasrakauskas/bun-kafka/issues/63)) ([21cc5e3](https://github.com/lukasrakauskas/bun-kafka/commit/21cc5e340395c2f982e438f0c9b400a32ccf1e5f))
* **producer:** coalesce automatic flush scheduling ([#55](https://github.com/lukasrakauskas/bun-kafka/issues/55)) ([01aa82d](https://github.com/lukasrakauskas/bun-kafka/commit/01aa82d3508f5e720eed1e80107349be0e3f9574))
* **protocol:** accelerate compressed record decoding ([#54](https://github.com/lukasrakauskas/bun-kafka/issues/54)) ([48956cf](https://github.com/lukasrakauskas/bun-kafka/commit/48956cf0f5072409421924aee2bd6fb9ac47d4d7))
* **protocol:** accelerate CRC32C with slicing-by-four ([#53](https://github.com/lukasrakauskas/bun-kafka/issues/53)) ([b64c36b](https://github.com/lukasrakauskas/bun-kafka/commit/b64c36b9708a3596c5ad6d863471e80fe0254d15))
* **protocol:** build request frames in one exact-sized buffer ([#65](https://github.com/lukasrakauskas/bun-kafka/issues/65)) ([69a4f64](https://github.com/lukasrakauskas/bun-kafka/commit/69a4f64f344af459555b1da9a559631295ba7093))
* **protocol:** remove redundant record batch buffer copies ([#52](https://github.com/lukasrakauskas/bun-kafka/issues/52)) ([7e3ba6f](https://github.com/lukasrakauskas/bun-kafka/commit/7e3ba6fb508f69dfa596105ec5b4801743663de2))
* **transport:** avoid copying complete response frames ([#56](https://github.com/lukasrakauskas/bun-kafka/issues/56)) ([b1a9b0d](https://github.com/lukasrakauskas/bun-kafka/commit/b1a9b0d4388dd0bd7e36e8a901c6a17e52ddacaa))

## [Unreleased]

### Added

- Opt-in KIP-848 consumer groups for Kafka 4.x via `groupProtocol: "consumer"`, including broker-side assignment, member-epoch heartbeats, offset v9, protocol fixtures, rebalance coverage, and a live Kafka 4 matrix

## [0.1.0] - 2026-09-06

### Added

- Initial Kafka client implementation with produce, batch consume, and documentation
- Test coverage across the listed feature set and performance improvements
- Missing-feature notes and partial chaos testing
- Incremental config updates: `admin.incrementalAlterConfigs()` (IncrementalAlterConfigs v1,
  set/delete/append/subtract individual entries with validate-only support), mock and real-broker
  verified
- Documentation website: nine usage guides (getting started, producing, consuming, transactions,
  administration, security, configuration reference, observability, kafkajs migration) rendered
  into a static site by `bun run docs:build` / `docs:serve`
- `docs/kafka-versions-and-kips.md`: newcomer-friendly mapping of Kafka releases, wire API
  versioning, and KIPs to this client, with a documented proposal for full versioned-method
  support
- `docs/client-gap-audit.md`: feature-by-feature gap audit versus kafkajs, node-rdkafka, and
  franz-go with explicit dispositions
- Transactions: coordinator resolution, transactional produce, EndTxn/TxnOffsetCommit, and control-batch filtering
- Incremental fetch sessions (KIP-227), cooperative-sticky assignment (KIP-429), static membership (KIP-345), and timed SASL/OAUTHBEARER reauthentication (KIP-368)
- Client quotas, delegation tokens, group admin, deleteRecords, ACLs, and cluster id via Metadata v2
- Observability: stats, healthCheck, logging hooks, and a full error catalog
- Pure-TS Snappy and LZ4 compression codecs alongside gzip/zstd
- acks=0 produce, custom partitioners, per-message delivery callbacks, read-committed isolation, regex subscription
- Typed `ConsumedMessage` generics inferred from key/value deserializers
- Soak harness with release-gate checks, chaos suite qualification, and 30-minute soak evidence
- CI: typecheck, unit/features suites with coverage gate, Redpanda integration, chaos mock

### Fixed

- npm packaging: `files` whitelist plus repository/keywords metadata keep the published tarball
  to source and docs
- OffsetCommit v2 wire format: INT64 retention period and no v2 response throttle

### Changed

- Repository restructured as a Bun monorepo: library in `packages/kafka`, documentation site in
  `apps/docs` (Starlight); all guides and reference docs moved into the site's content
- Split the client god class into cluster/producer/consumer/admin modules
- Reorganized tests into unit/features/impl scopes with coverage tracking

[Unreleased]: https://github.com/lukasrakauskas/bun-kafka/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/lukasrakauskas/bun-kafka/releases/tag/v0.1.0
