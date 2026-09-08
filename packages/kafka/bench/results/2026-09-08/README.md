# Benchmark snapshot: 2026-09-08

Library commit: `4b88c455f611d32b86b26b014d733d741a6c210d`.

- Intel Core i5-4570, four cores, 7.7 GiB RAM, Linux 6.12.63.
- Bun 1.4.0, hyperfine 1.20.0, Go 1.27.1, rustc/cargo 1.96.1; KafkaJS 2.2.4.
- Redpanda v25.2.1, local Docker, two threads, 2 GiB broker memory; one default partition and automatic topic creation enabled.
- Shared host: existing services remained running. No CPU pinning or cache isolation. Do not interpret changes from older snapshots as isolated library regressions.
- Two warmups and ten measured processes per lane, run sequentially. No samples discarded.

`samples.json` contains direct-run wall times and benchmark-reported timed sections for every benchmark entry point. `codec-hyperfine.json` and `clients-hyperfine.json` contain separate hyperfine measurements (seconds). The docs use hyperfine for wall-time tables and medians of direct-run metrics for timed sections. Hyperfine reported codec outliers; these remain in the data.

The command runner was interrupted by the harness time limit during the Rust lane, after saving all completed lanes. `BENCH_RESUME=1` skipped those completed lanes and reran the entire Rust lane, followed by KafkaJS. No partial-lane samples were retained.

## Reproduce

Install Bun, hyperfine, Go, Rust/Cargo and native build prerequisites, then from the repository root:

```sh
bun install --frozen-lockfile
docker compose -p bun-kafka-bench-refresh -f packages/kafka/test/impl/soak.compose.yml up -d --wait
mkdir -p /tmp/bun-kafka-bench-deps
bun install --cwd /tmp/bun-kafka-bench-deps kafkajs@2.2.4
cd packages/kafka
bun run bench:build
# Confirm both native/build/bench-go and native/build/bench-rust exist.
NODE_PATH=/tmp/bun-kafka-bench-deps/node_modules bun bench/results/2026-09-08/run.ts
BENCH_COUNT=20000 BENCH_RUNS=10 bun run bench
cp native/build/bun-native-hyperfine.json bench/results/2026-09-08/codec-hyperfine.json
KAFKA_BROKERS=127.0.0.1:19092 MSG_SIZE=100 hyperfine --warmup 2 --runs 10 \
  --export-json bench/results/2026-09-08/clients-hyperfine.json \
  --command-name bun-kafka 'bun bench/bun-produce-consume.ts refresh-hf-bun-$(date +%s%N) 5000' \
  --command-name franz-go 'native/build/bench-go refresh-hf-go-$(date +%s%N) 5000' \
  --command-name rdkafka-rust 'native/build/bench-rust refresh-hf-rust-$(date +%s%N) 5000'
cd ../..
docker compose -p bun-kafka-bench-refresh -f packages/kafka/test/impl/soak.compose.yml down -v
```

The runner overwrites this snapshot; copy it elsewhere to preserve the recorded evidence before rerunning. It defaults to `127.0.0.1:19092`; set `KAFKA_BROKERS` to override. Never point it at a production broker: every broker invocation creates a fresh topic. The cleanup command removes only this dedicated Compose project's containers and volume.

KafkaJS is deliberately reported separately: explicit topic creation outside production timing, 1,000-record sends, default all-replica acknowledgements, consumer-group joining and per-message callbacks differ from the other client lanes. The two Bun broker entry points are equivalent except for their labels and default topic prefixes.
