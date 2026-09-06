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

The repository needs this setup before automation can run:

- Repository secret `RELEASE_PLEASE_TOKEN`: a fine-grained GitHub PAT with repository Contents,
  Issues, and Pull requests read/write access. A PAT is required so Release Please PRs trigger CI.
- An npm [trusted publisher](https://docs.npmjs.com/trusted-publishers/) for `bun-kafka`, configured
  for GitHub Actions: owner `lukasrakauskas`, repository `bun-kafka`, workflow `release.yml`, and
  environment `npm`. Allow `npm stage publish`; direct publishing is not needed.
  The package must already exist on npm. No `NPM_TOKEN` secret is required.

The publish job grants `id-token: write` for OIDC authentication and uses npm CLI `12.0.2` on Node
24 (24.15.0 or newer). Bun remains the project runtime/package manager; npm is used only for its
staged-publishing support.

Release Please maintains a release PR from Conventional Commits merged to `main`. That PR updates
`packages/kafka/package.json` and `packages/kafka/CHANGELOG.md`. Confirm CI and the applicable
performance and chaos gates, then squash-merge it. The merge creates the `vX.Y.Z` tag and GitHub
release, and the same workflow **stages** the package on npm. It does not make the npm version
public. A maintainer must inspect and approve the staged package with npm account authentication
and 2FA (not the CI OIDC token):

```bash
bunx npm@12.0.2 stage list bun-kafka
bunx npm@12.0.2 stage view <stage-id>
bunx npm@12.0.2 stage approve <stage-id>
```

Only approval publishes the npm version. Staged versions reserve their version number; if a stage
needs replacing, reject it with `bunx npm@12.0.2 stage reject <stage-id>` before staging it again.
See [npm staged publishing](https://docs.npmjs.com/staged-publishing).

Changes to protocol, connection, producer, or consumer behavior also require the follow-up soak
described in [performance validation](performance-validation.md).
