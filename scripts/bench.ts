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
const rsBin = join(build, "bench-rust");

if (!existsSync(goBin) || !existsSync(rsBin)) {
  console.log("building comparison benches...");
  await $`bun run scripts/build-benches.ts`;
}

const topicBase = `hf-${Date.now()}`;
const env = { ...process.env, KAFKA_BROKERS: brokers, MSG_SIZE: process.env.MSG_SIZE ?? "100" };

const cmds: string[] = [
  `BUN_KAFKA_NATIVE=ffi bun ${root}/bench/produce_consume.ts ${topicBase}-ffi ${count}`,
];

if (existsSync(join(build, "bun_kafka_native.node"))) {
  cmds.push(`BUN_KAFKA_NATIVE=napi bun ${root}/bench/produce_consume.ts ${topicBase}-napi ${count}`);
}
if (existsSync(goBin)) cmds.push(`${goBin} ${topicBase}-go ${count}`);
if (existsSync(rsBin)) cmds.push(`${rsBin} ${topicBase}-rs ${count}`);

try {
  await import("kafkajs");
  cmds.push(`bun ${root}/bench/kafkajs.ts ${topicBase}-kjs ${count}`);
} catch {}

const args = [
  "hyperfine",
  "--warmup", "1",
  "--runs", process.env.BENCH_RUNS ?? "3",
  "--export-json", join(build, "hyperfine.json"),
  "--export-markdown", join(build, "hyperfine.md"),
];

const seen = new Set<string>();
for (const c of cmds) {
  let name = "cmd";
  if (c.includes("NATIVE=ffi")) name = "bun-kafka/ffi";
  else if (c.includes("NATIVE=napi")) name = "bun-kafka/napi";
  else if (c.includes("bench-go")) name = "franz-go";
  else if (c.includes("bench-rust")) name = "rdkafka-rust";
  else if (c.includes("kafkajs")) name = "kafkajs";
  if (seen.has(name)) continue;
  seen.add(name);
  args.push("--command-name", name, c);
}

console.log("Running hyperfine:", [...seen]);
const r = Bun.spawnSync(args, { cwd: root, env, stdout: "inherit", stderr: "inherit" });
process.exit(r.exitCode ?? 1);
