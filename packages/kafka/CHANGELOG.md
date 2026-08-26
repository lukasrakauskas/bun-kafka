# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

### Fixed

- npm packaging: `files` whitelist plus repository/keywords metadata keep the published tarball
  to source and docs

## [0.2.0] - 2026-08-24

### Added

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

- OffsetCommit v2 wire format: INT64 retention period and no v2 response throttle

### Changed

- Repository restructured as a Bun monorepo: library in `packages/kafka`, documentation site in
  `packages/docs` (Starlight); all guides and reference docs moved into the site's content
- Split the client god class into cluster/producer/consumer/admin modules
- Reorganized tests into unit/features/impl scopes with coverage tracking

## [0.1.0] - 2026-08-24

### Added

- Initial Kafka client implementation with produce, batch consume, and documentation
- Test coverage across the listed feature set and performance improvements
- Missing-feature notes and partial chaos testing

[Unreleased]: https://github.com/lukasrakauskas/bun-kafka/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/lukasrakauskas/bun-kafka/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/lukasrakauskas/bun-kafka/releases/tag/v0.1.0
