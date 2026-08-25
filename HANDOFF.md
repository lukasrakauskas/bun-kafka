# bun-kafka feature completion — DONE

All features listed in docs/feature-completeness.md are implemented and tested.

## Completed (commits e46f149..ae0fa53)

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
