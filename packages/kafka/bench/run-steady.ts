import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import { Kafka } from "../index.ts";

// Linux /proc supplies the same external CPU/RSS measurement for all three runtimes.
const brokers = process.env.KAFKA_BROKERS ?? "127.0.0.1:19092";
const output = process.env.BENCH_OUTPUT ?? `/tmp/bun-kafka-steady-${Date.now()}`;
await mkdir(output); // Refuse to overwrite evidence.
const ticks = Number(await Bun.$`getconf CLK_TCK`.text());
const repeats = Number(process.env.BENCH_REPEATS ?? 3);
assert(Number.isSafeInteger(repeats) && repeats > 0);
const kafka = new Kafka({ brokers: brokers.split(",") });
const admin = kafka.admin();
const samples: unknown[] = [];
const fault = process.env.BENCH_FAULT;
assert(fault === undefined || fault === "pause" || fault === "restart");
const compose = [
  "sudo",
  "-n",
  "docker",
  "compose",
  "-p",
  "bun-kafka-bench-57",
  "-f",
  "test/impl/soak.compose.yml",
];
async function docker(...args: string[]): Promise<void> {
  const child = Bun.spawn([...compose, ...args], {
    stdout: "pipe",
    stderr: "pipe",
    signal: AbortSignal.timeout(60_000),
  });
  const stderr = new Response(child.stderr).text();
  await new Response(child.stdout).text();
  assert.equal(await child.exited, 0, await stderr);
}
if (fault) {
  assert.equal(brokers, "127.0.0.1:19092", "faults are restricted to the dedicated local broker");
  await docker(
    "exec",
    "-T",
    "redpanda",
    "rpk",
    "cluster",
    "health",
    "-X",
    "brokers=localhost:9092",
  );
}
async function injectFault(): Promise<Record<string, number | string>> {
  const start = performance.now();
  if (fault === "pause") {
    await docker("pause", "redpanda");
    const applied = performance.now();
    try {
      await Bun.sleep(1000);
    } finally {
      await docker("unpause", "redpanda");
    }
    return { fault, command_ms: applied - start, stalled_ms: performance.now() - applied };
  }
  await docker("restart", "--timeout", "0", "redpanda");
  await docker("up", "-d", "--wait", "redpanda");
  return { fault: "restart", restart_and_health_ms: performance.now() - start };
}
const lanes = [
  { lib: "bun-kafka", mode: "individual", command: ["bun", "bench/bun-steady.ts"] },
  { lib: "bun-kafka", mode: "bulk", command: ["bun", "bench/bun-steady.ts"] },
  {
    lib: "franz-go",
    mode: "individual",
    command: [process.env.GO_BENCH ?? "native/build/steady-go"],
  },
  {
    lib: "rdkafka-rust",
    mode: "individual",
    command: [process.env.RUST_BENCH ?? "bench/rust/target/release/steady"],
  },
];
const profiles = fault
  ? [{ name: "large-response", size: 65536, batch: 8, windows: 200 }]
  : [
      { name: "small-latency", size: 100, batch: 1, windows: 1000 },
      { name: "small-burst", size: 100, batch: 100, windows: 1000 },
      { name: "large-response", size: 65536, batch: 8, windows: 200 },
    ];

async function usage(pid: number): Promise<{ cpu_ms: number; rss_kib: number }> {
  const stat = await Bun.file(`/proc/${pid}/stat`).text();
  const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
  const status = await Bun.file(`/proc/${pid}/status`).text();
  return {
    cpu_ms: ((Number(fields[11]) + Number(fields[12])) * 1000) / ticks,
    rss_kib: Number(status.match(/^VmRSS:\s+(\d+)/m)?.[1] ?? 0),
  };
}

try {
  for (const profile of profiles) {
    for (let repeat = 0; repeat < repeats; repeat++) {
      // Rotate order to reduce consistent first/last-client bias on a shared host.
      for (let index = 0; index < lanes.length; index++) {
        const lane = lanes[(index + repeat) % lanes.length]!;
        const topic = `steady-57-${crypto.randomUUID()}`;
        await admin.createTopics([{ name: topic, numPartitions: 1, replicationFactor: 1 }]);
        try {
          console.log(`${profile.name} ${lane.lib}/${lane.mode} ${repeat + 1}/${repeats}`);
          const child = Bun.spawn(lane.command, {
            env: {
              ...process.env,
              KAFKA_BROKERS: brokers,
              BENCH_TOPIC: topic,
              MSG_SIZE: String(profile.size),
              BENCH_BATCH: String(profile.batch),
              BENCH_WINDOWS: String(profile.windows),
              BENCH_MODE: lane.mode,
            },
            stdout: "pipe",
            stderr: "pipe",
          });
          const stderr = new Response(child.stderr).text();
          const timer = setTimeout(() => child.kill("SIGKILL"), 130_000);
          let start: Awaited<ReturnType<typeof usage>> | undefined;
          let peakRss = 0;
          let faultTask: Promise<Record<string, number | string>> | undefined;
          let result: Record<string, unknown> | undefined;
          let resource: Record<string, number> | undefined;
          const poll = setInterval(() => {
            if (start)
              void usage(child.pid)
                .then((sample) => {
                  peakRss = Math.max(peakRss, sample.rss_kib);
                })
                .catch(() => {});
          }, 20);
          try {
            let pending = "";
            const decoder = new TextDecoder();
            for await (const chunk of child.stdout) {
              pending += decoder.decode(chunk, { stream: true });
              let newline: number;
              while ((newline = pending.indexOf("\n")) >= 0) {
                const line = pending.slice(0, newline);
                pending = pending.slice(newline + 1);
                const event = JSON.parse(line);
                if (event.phase === "start") {
                  assert(!start, "duplicate start");
                  start = await usage(child.pid);
                  peakRss = start.rss_kib;
                  if (fault) {
                    faultTask = injectFault();
                    // Observe early rejection while the client is still running.
                    void faultTask.catch(() => {});
                  }
                } else if (event.phase === "result") {
                  assert(start && !result, "missing start or duplicate result");
                  const end = await usage(child.pid);
                  result = event;
                  resource = {
                    cpu_ms: end.cpu_ms - start.cpu_ms,
                    start_rss_kib: start.rss_kib,
                    end_rss_kib: end.rss_kib,
                    sampled_peak_rss_kib: Math.max(peakRss, end.rss_kib),
                  };
                } else throw new Error(`unexpected output: ${line}`);
              }
            }
            const exitCode = await child.exited;
            const errors = await stderr;
            const faultResult = await faultTask;
            if (fault) assert(start && faultTask, "client failed before fault injection");
            if (fault && exitCode !== 0) {
              samples.push({
                profile,
                repeat,
                lib: lane.lib,
                mode: lane.mode,
                topic,
                fault: faultResult,
                exit_code: exitCode,
                stderr: errors,
                sampled_peak_rss_kib: peakRss,
              });
              await Bun.write(
                `${output}/samples.json`,
                JSON.stringify({ brokers, samples }, null, 2) + "\n",
              );
              continue;
            }
            assert.equal(exitCode, 0, errors);
            assert(result && resource, "missing result");
            assert.equal(result.count, profile.batch * profile.windows);
            assert.equal(result.consume_count, profile.batch * (profile.windows - 1));
            assert.equal(result.validated, profile.batch * (profile.windows + 100));
            assert.equal(result.lib, lane.lib);
            const latency = result.window_ack_ms as number[];
            assert.equal(latency.length, profile.windows);
            assert(latency.every((n) => Number.isFinite(n) && n >= 0));
            if (fault === "pause")
              assert(Math.max(...latency) >= 500, "pause did not affect a measured request");
            const sorted = latency.toSorted((a, b) => a - b);
            samples.push({
              profile,
              repeat,
              lib: lane.lib,
              mode: lane.mode,
              topic,
              fault: faultResult,
              ...result,
              ...resource,
              p50_ms: sorted[Math.ceil(sorted.length * 0.5) - 1],
              p95_ms: sorted[Math.ceil(sorted.length * 0.95) - 1],
              p99_ms: sorted[Math.ceil(sorted.length * 0.99) - 1],
            });
            await Bun.write(
              `${output}/samples.json`,
              JSON.stringify(
                { brokers, ticks_per_second: ticks, rss_sample_interval_ms: 20, samples },
                null,
                2,
              ) + "\n",
            );
          } finally {
            clearTimeout(timer);
            clearInterval(poll);
            child.kill();
            await child.exited;
            await faultTask;
          }
        } finally {
          await admin.deleteTopics([topic]);
        }
      }
    }
  }
} finally {
  await kafka.disconnect();
}
console.log(output);
