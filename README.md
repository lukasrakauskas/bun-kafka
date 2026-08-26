# bun-kafka

Monorepo for [bun-kafka](packages/kafka) — a zero-dependency Kafka wire protocol client for Bun —
and its [Starlight documentation site](apps/docs).

## Layout

| Package                            | Description                                                                |
| ---------------------------------- | -------------------------------------------------------------------------- |
| [`packages/kafka`](packages/kafka) | The `bun-kafka` library: producer, consumer, admin, transactions, SASL/TLS |
| [`apps/docs`](apps/docs)   | Documentation site ([Starlight](https://starlight.astro.build))            |

## Commands

```bash
bun install                 # install all workspaces

# library (packages/kafka)
bun run test                # unit + feature suites
bun run lint                # oxlint
bun run typecheck           # tsc --noEmit

# docs site (apps/docs)
bun run docs:dev            # dev server with hot reload
bun run docs:build          # static build into apps/docs/dist
```

Package scripts delegate to the owning workspace (`--cwd`), so they can run from anywhere in the
repository. See [packages/kafka/README.md](packages/kafka/README.md) for the full client
documentation and release process.
