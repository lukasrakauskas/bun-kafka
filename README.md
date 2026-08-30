# bun-kafka

> [!WARNING]
> **Early development:** bun-kafka is under active development and is not yet recommended for
> production workloads. APIs may change between releases.

This repository contains `bun-kafka`, a zero-dependency Kafka client for Bun, and its documentation
site.

## Use bun-kafka

Install the package:

```bash
bun add bun-kafka
```

Start with the [package README](packages/kafka/README.md) or the
[user documentation](apps/docs/src/content/docs/index.mdx).

## Contribute

```bash
bun install
bun run test
bun run typecheck
bun run lint
bun run docs:build
```

Commands run from the repository root and delegate to the correct workspace. See the
[developer documentation](docs/README.md) for test plans, release gates, audits, and maintenance
notes.

## Repository layout

| Path                               | Purpose                                        |
| ---------------------------------- | ---------------------------------------------- |
| [`packages/kafka`](packages/kafka) | Published Kafka client and tests               |
| [`apps/docs`](apps/docs)           | User documentation site                        |
| [`docs`](docs/README.md)           | Contributor and maintainer documentation       |
| [`bench`](bench)                   | Cross-client benchmark programs                |
| [`native`](native)                 | Native comparison clients and benchmark output |
