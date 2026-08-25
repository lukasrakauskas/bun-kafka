#!/usr/bin/env bun

const hyperfine = Bun.which("hyperfine");
if (!hyperfine) throw new Error("hyperfine is required: https://github.com/sharkdp/hyperfine");

await Bun.$`mkdir -p native/build`;
const runs = process.env.BENCH_RUNS ?? "10";
const total = process.env.BENCH_COUNT ?? "100000";
const args = [
  "--warmup",
  "2",
  "--runs",
  runs,
  "--export-json",
  "native/build/bun-native-hyperfine.json",
  "--export-markdown",
  "native/build/bun-native-hyperfine.md",
  "--command-name",
  "one record per batch",
  `bun bench/bun-codec.ts 1 ${total}`,
  "--command-name",
  "100 records per batch",
  `bun bench/bun-codec.ts 100 ${total}`,
];

if (process.env.KAFKA_BROKERS) {
  const count = process.env.BENCH_KAFKA_COUNT ?? "10000";
  const topic = `bun-kafka-bench-${Date.now()}`;
  args.push(
    "--command-name",
    "bun-kafka",
    `bun bench/bun-produce-consume.ts ${topic}-bun-$(date +%s%N) ${count}`,
  );
  if (await Bun.file("native/build/bench-go").exists())
    args.push(
      "--command-name",
      "franz-go",
      `native/build/bench-go ${topic}-go-$(date +%s%N) ${count}`,
    );
  if (await Bun.file("native/build/bench-rust").exists())
    args.push(
      "--command-name",
      "rdkafka-rust",
      `native/build/bench-rust ${topic}-rust-$(date +%s%N) ${count}`,
    );
}

const result = Bun.spawnSync([hyperfine, ...args], {
  cwd: new URL("..", import.meta.url).pathname,
  env: process.env,
  stdout: "inherit",
  stderr: "inherit",
});
process.exit(result.exitCode ?? 1);
