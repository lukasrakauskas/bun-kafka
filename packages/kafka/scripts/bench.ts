#!/usr/bin/env bun
import { $ } from "bun";
import { existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

const count = process.env.BENCH_COUNT ?? "10000";
const brokers = process.env.KAFKA_BROKERS ?? "127.0.0.1:9092";
const root = join(import.meta.dir, "..");
const build = join(root, "native/build");
mkdirSync(build, { recursive: true });

const goBin = join(build, "bench-go");
const rustBin = join(build, "bench-rust");
if (!existsSync(goBin) || !existsSync(rustBin)) {
  console.log("building comparison benches...");
  await $`bun run scripts/build-benches.ts`;
}

const topicBase = `hf-${Date.now()}`;
const env = { ...process.env, KAFKA_BROKERS: brokers, MSG_SIZE: process.env.MSG_SIZE ?? "100" };
const cmds: Array<[string, string]> = [
  ["bun-kafka", `bun ${root}/bench/produce_consume.ts ${topicBase}-bun ${count}`],
];
if (existsSync(goBin)) cmds.push(["franz-go", `${goBin} ${topicBase}-go ${count}`]);
if (existsSync(rustBin)) cmds.push(["rdkafka-rust", `${rustBin} ${topicBase}-rust ${count}`]);
try {
  await import("kafkajs");
  cmds.push(["kafkajs", `bun ${root}/bench/kafkajs.ts ${topicBase}-kjs ${count}`]);
} catch {}

const args = [
  "hyperfine",
  "--warmup",
  "1",
  "--runs",
  process.env.BENCH_RUNS ?? "3",
  "--export-json",
  join(build, "hyperfine.json"),
  "--export-markdown",
  join(build, "hyperfine.md"),
];
for (const [name, command] of cmds) args.push("--command-name", name, command);

console.log(
  "Running hyperfine:",
  cmds.map(([name]) => name),
);
const result = Bun.spawnSync(args, { cwd: root, env, stdout: "inherit", stderr: "inherit" });
process.exit(result.exitCode ?? 1);
