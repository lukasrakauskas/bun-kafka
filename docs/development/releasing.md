# Testing and releasing

Run commands from the repository root unless a command says otherwise.

## Commit and branch conventions

Pull request titles use [Conventional Commits](https://www.conventionalcommits.org/):
`type(optional-scope): description`. Allowed types are `feat`, `fix`, `docs`, `style`, `refactor`,
`perf`, `test`, `build`, `ci`, `chore`, and `revert`.

Branches use `<type>/<lowercase-slug>` (for example, `feat/batch-consume`) or
`issue-<number>-<lowercase-slug>`. Dependabot and Release Please branches are also allowed. CI checks
both conventions. The repository only permits squash merges and uses the PR title for the resulting
commit, keeping `main` conventional.

Release Please derives versions from commits on `main`:

- `fix:` creates a patch release.
- `feat:` creates a minor release.
- `type!:` or a `BREAKING CHANGE:` footer creates a major release.

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

The repository needs these secrets before automation can run:

- Repository secret `RELEASE_PLEASE_TOKEN`: a fine-grained GitHub PAT with repository Contents,
  Issues, and Pull requests read/write access. A PAT is required so Release Please PRs trigger CI.
- Secret `NPM_TOKEN` in the `npm` environment: an npm token allowed to publish `bun-kafka`.

Release Please maintains a release PR from Conventional Commits merged to `main`. That PR updates
`packages/kafka/package.json` and `packages/kafka/CHANGELOG.md`. Confirm CI and the applicable
performance and chaos gates, then squash-merge it. The merge creates the `vX.Y.Z` tag and GitHub
release, and the same workflow publishes the package to npm.

Changes to protocol, connection, producer, or consumer behavior also require the follow-up soak
described in [performance validation](performance-validation.md).
