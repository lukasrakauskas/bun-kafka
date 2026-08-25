# bun-kafka feature completion — DONE

All features listed in docs/feature-completeness.md are implemented and tested.

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
- Docs updated: README matrix + docs/feature-completeness.md.

## Qualification evidence (2026-08-25)

- Three-broker chaos suite passes end to end (18 pass / 0 fail), including the
  blackholed-leader pause/resume scenario that needed a bounded recovery window after a
  raft leader election during the pause. Artifacts in out/chaos/.
- Long-running soak harness added at scripts/soak.ts (`bun run test:soak`): samples every
  required metric per docs/performance-validation.md every 10 s, injects bursts, validates a
  per-partition sequence oracle, evaluates release gates, and writes JSON+MD artifacts to
  out/soak/. BunProducer exposes `queuedMessages` for the queue-depth gate.
- Recorded passing soak: 30 min at 1,000 msg/s of 1 KiB acks=all across 6 partitions with
  1.5x bursts — 1,959,750 offered = acknowledged = consumed, zero failures/duplicates/
  order violations/missing, RSS stable (+7.5 MiB range), no throughput decay. The 24-hour
  and 72-hour release soaks remain outstanding by design.

## Broker notes

Single-node Redpanda dev container `bun-kafka-dev` (image be202e716d34). Recreate with:

    docker run -d --name bun-kafka-dev --ulimit nofile=65536:65536 -p 9092:9092 \
      be202e716d34 redpanda start --mode dev-container \
      --kafka-addr=PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr=PLAINTEXT://127.0.0.1:9092
    docker exec bun-kafka-dev rpk cluster config set internal_topic_replication_factor 1
    docker restart bun-kafka-dev   # config applies after restart

The ulimit matters: the default FD limit makes topic creation fail with INVALID_PARTITIONS
("Can not increase partition count due to FD limit") after a few hundred test topics.

Run everything: `bun test test/` (soak/three-broker chaos suites stay skipped by design).
