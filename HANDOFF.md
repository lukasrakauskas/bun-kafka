# bun-kafka feature completion — DONE

All features listed in apps/docs/src/content/docs/feature-completeness.md are implemented and tested.

## Release-prep cycle (2026-08-26)

Work streams, each on its own branch off `feat/oxlint-oxfmt-with-custom-rules` and merged back:

- `docs/kafka-versions-and-kips.md` — newcomer-oriented mapping of Kafka releases, wire API
  versioning strategy, KIP catalog (claim → code → verifying test), plus a documented proposal
  for full per-broker version negotiation with trigger conditions.
- `docs/client-gap-audit.md` — gap audit vs kafkajs / node-rdkafka / franz-go with explicit
  dispositions. Its top open code item, IncrementalAlterConfigs, is now implemented and merged
  (see below).
- `feat/incremental-alter-configs` — `admin.incrementalAlterConfigs()` (API key 44, v1 flexible:
  set/delete/append/subtract per entry, validate-only). Wire schema taken from the Apache
  protocol sources; mock wire-shape test plus real-broker round trip verified against a
  dedicated Redpanda container (SET persisted + confirmed via DescribeConfigs, sibling-entry
  safety, delete-reverts-to-default, error path). Docs updated (feature matrix, gap audit,
  admin guide, changelog).
- `feat/release-packaging` — npm publish hygiene: `files` whitelist + repository/keywords;
  dry-run tarball verified at 31 files / ~260 KB.
- `feat/docs-website` — nine usage guides under docs/guide/ plus a static documentation website:
  `bun run docs:build` emits website/dist (16 pages), `bun run docs:serve` serves it.
- Merged previously unmerged branches: feat/release-soak (24 h soak profile + wrap-up checklist)
  and feat/ci (perf-ratio CI flake fix).

Gates at merge time: oxlint --deny-warnings clean, oxfmt clean, tsc clean, 68/68 tests pass.

Remaining before tagging a release: the live 24-hour soak finishes ~2026-08-26 12:20 UTC — run
the wrap-up checklist below, then flip the status lines in apps/docs/src/content/docs/performance-validation.md.

## Completed (commits e46f149..HEAD)

- OffsetCommit v2 INT64 retention / no-v2-throttle fix
- Transactions end-to-end: FindCoordinator v2 (key-first wire order), InitProducerId,
  AddPartitionsToTxn v1, Produce v3 transactional_id + isTransactional batch bit,
  EndTxn v1, TxnOffsetCommit v0, sendOffsetsToTransaction, epoch bump (KIP-360).
  Root cause of the old blocker: EndTxn(26)/TxnOffsetCommit(28) API keys were swapped.
- Client quotas: DescribeClientQuotas/AlterClientQuotas v1 flexible encoding.
- Delegation tokens: Create/Renew/Expire/Describe v2 (mock-tested; Redpanda lacks these APIs).
- Fetch sessions (KIP-227): Fetch v7, per-leader session id/epoch, incremental pruning,
  forgotten_topics_data, UNKNOWN_SESSION/EPOCH recovery.
- Cooperative-sticky assignor (KIP-429): subscription v1 owned partitions, sticky leader
  assignment up to fair share, retained in-memory positions across rebalances.
- Docs updated: README matrix + apps/docs/src/content/docs/feature-completeness.md.

## Qualification evidence (2026-08-25)

- Three-broker chaos suite passes end to end (18 pass / 0 fail), including the
  blackholed-leader pause/resume scenario that needed a bounded recovery window after a
  raft leader election during the pause. Artifacts in out/chaos/.
- Long-running soak harness added at scripts/soak.ts (`bun run test:soak` (from packages/kafka)): samples every
  required metric per apps/docs/src/content/docs/performance-validation.md every 10 s, injects bursts, validates a
  per-partition sequence oracle, evaluates release gates, and writes JSON+MD artifacts to
  out/soak/. BunProducer exposes `queuedMessages` for the queue-depth gate.
- Recorded passing soak: 30 min at 1,000 msg/s of 1 KiB acks=all across 6 partitions with
  1.5x bursts — 1,959,750 offered = acknowledged = consumed, zero failures/duplicates/
  order violations/missing, RSS stable (+7.5 MiB range), no throughput decay. The 24-hour
  and 72-hour release soaks remain outstanding by design.

## Live 24-hour release soak (in progress)

Started 2026-08-25 ~12:15 UTC on the `feat/release-soak` tree (PR #14), detached via
`setsid` so it outlives any terminal session:

    SOAK_DURATION_S=86400 SOAK_BURST_INTERVAL_S=3600 SOAK_BURST_S=600 bun scripts/soak.ts

Same as `bun run test:soak:release`. Profile: 86,400 s at 1,000 msg/s (1 KiB acks=all,
6 partitions) with a 10-minute 150% burst every hour — the 24-hour gate profile from
apps/docs/src/content/docs/performance-validation.md.

- Progress: `tail -f out/soak/release-24h.log`
- Artifacts on completion: newest `out/soak/<timestamp>.{json,md}`
- Broker: local Redpanda dev container (`bun-kafka-dev`) — keep it up for the full run.
- ETA ≈ 2026-08-26 ~12:20 UTC.

### When it finishes (wrap-up checklist)

1. Check gates in the log tail / MD artifact: zero failed acks/duplicates/order
   violations/missing records, RSS growth < 64 MiB over 24 h, throughput decay < 5 %,
   p95/p99 drift within limits, lag recovery after every burst.
2. Commit evidence: `out/` is gitignored, so `git add -f out/soak/release-24h.log` plus
   the final `<timestamp>.{json,md}` pair.
3. Flip status in apps/docs/src/content/docs/performance-validation.md ("Current status" line and the soak
   sections) to 24-hour soak-proven with the run numbers; link the artifacts.
4. Merge PR #14 (stacked on #13 → #12); issue #11 closes with it.
5. Still outstanding afterwards: the 72-hour follow-up soak at 75 % of max stable rate,
   required before releases that touch protocol/connection/producer/consumer code.

## Broker notes

Single-node Redpanda dev container `bun-kafka-dev` (image be202e716d34). Recreate with:

    docker run -d --name bun-kafka-dev --ulimit nofile=65536:65536 -p 9092:9092 \
      be202e716d34 redpanda start --mode dev-container \
      --kafka-addr=PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr=PLAINTEXT://127.0.0.1:9092
    docker exec bun-kafka-dev rpk cluster config set internal_topic_replication_factor 1
    docker restart bun-kafka-dev   # config applies after restart

The ulimit matters: the default FD limit makes topic creation fail with INVALID_PARTITIONS
("Can not increase partition count due to FD limit") after a few hundred test topics.

Run everything: `cd packages/kafka && bun test test/` (soak/three-broker chaos suites stay skipped by design).

## Monorepo layout (2026-08-26)

The library now lives in `packages/kafka`; markdown documentation moved to the Starlight site in
`apps/docs/src/content/docs/`. Root scripts delegate with `--cwd`. The live soak writes to
`out/soak/` at the repository root (unchanged, gitignored).
