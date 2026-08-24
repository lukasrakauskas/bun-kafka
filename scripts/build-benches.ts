#!/usr/bin/env bun
import { $ } from "bun";

await $`mkdir -p native/build`;

console.log("building go bench...");
const go = await $`cd bench/go && go mod tidy && go build -o ../../native/build/bench-go .`.nothrow();
if (go.exitCode !== 0) console.warn("go bench build failed");

console.log("building rust bench (may take a while)...");
const rs = await $`cd bench/rust && cargo build --release`.nothrow();
if (rs.exitCode !== 0) console.warn("rust bench build failed");
else await $`cp bench/rust/target/release/bun-kafka-bench-rust native/build/bench-rust`.nothrow();

console.log("done");
