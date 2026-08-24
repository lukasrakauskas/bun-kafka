#!/usr/bin/env bun
import { $ } from "bun";
import { existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

const outDir = join(import.meta.dir, "..", "native", "build");
mkdirSync(outDir, { recursive: true });
const out = join(outDir, "bun_kafka_native.node");

const cflags = (await $`pkg-config --cflags rdkafka`.text()).trim().split(/\s+/).filter(Boolean);
const libs = (await $`pkg-config --libs rdkafka`.text()).trim().split(/\s+/).filter(Boolean);

function findNodeApiInclude(): string {
  const env = process.env.NODE_API_INCLUDE;
  if (env && existsSync(join(env, "node_api.h"))) return env;
  const candidates = [
    join(process.execPath, "..", "..", "include"),
    "/opt/homebrew/include/node",
    "/usr/local/include/node",
  ];
  try {
    const node = Bun.which("node");
    if (node) candidates.unshift(join(node, "..", "..", "include", "node"));
  } catch {}
  for (const c of candidates) {
    if (existsSync(join(c, "node_api.h"))) return c;
  }
  throw new Error("node_api.h not found; install node or set NODE_API_INCLUDE");
}

const nodeInc = findNodeApiInclude();
const args = [
  "cc",
  "-shared",
  "-fPIC",
  "-O2",
  join(import.meta.dir, "..", "native", "binding.c"),
  "-o",
  out,
  ...cflags,
  ...libs,
  `-I${nodeInc}`,
  "-I/opt/homebrew/include",
  "-I/usr/local/include",
  "-DNAPI_VERSION=8",
];
if (process.platform === "darwin") {
  args.splice(1, 0, "-undefined", "dynamic_lookup", "-dynamiclib");
}

console.log(args.join(" "));
const r = Bun.spawnSync(args, { stdout: "inherit", stderr: "inherit" });
if (r.exitCode !== 0) process.exit(r.exitCode ?? 1);
console.log("built", out);
