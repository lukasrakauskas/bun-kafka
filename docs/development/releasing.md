# Testing and releasing

Run commands from the repository root unless a command says otherwise.

## Test

```bash
bun run test            # unit and feature suites against the mock broker
bun run typecheck
bun run lint
bun run --cwd packages/kafka test:coverage
KAFKA_BROKERS=127.0.0.1:9092 bun test packages/kafka/test/impl/bun-integration.test.ts
```

Implementation tests that need a real broker live in `packages/kafka/test/impl/`.

## Chaos and soak tests

```bash
bun run --cwd packages/kafka test:chaos:mock
bun run --cwd packages/kafka test:chaos
bun run --cwd packages/kafka test:soak
```

The soak harness writes JSON and Markdown artifacts to `packages/kafka/out/soak/`. See
[performance validation](performance-validation.md) for workloads and release gates, and
[chaos testing](chaos-testing.md) for broker failure scenarios.

## Benchmark

Install [hyperfine](https://github.com/sharkdp/hyperfine), then run:

```bash
bun run --cwd packages/kafka bench:build
KAFKA_BROKERS=127.0.0.1:9092 bun run --cwd packages/kafka bench
```

The user-facing result summary is in the
[benchmark reference](../../apps/docs/src/content/docs/reference/benchmarks.md).

## Release

1. Bump `version` in `packages/kafka/package.json`.
2. In `packages/kafka/CHANGELOG.md`, rename `[Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and add a
   new `[Unreleased]` section.
3. Confirm that CI and the applicable performance and chaos gates pass.
4. Tag `vX.Y.Z` and push the tag.

Changes to protocol, connection, producer, or consumer behavior also require the follow-up soak
described in [performance validation](performance-validation.md).
