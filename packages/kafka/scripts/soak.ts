/**
 * Long-running soak test harness for bun-kafka.
 *
 * Implements the measurement requirements from performance-validation.md (docs site):
 * one long-running Bun process that produces at a configurable offered rate,
 * drains with a consumer, samples metrics every SOAK_SAMPLE_INTERVAL_S seconds,
 * injects periodic bursts at a higher rate, validates a per-partition sequence
 * oracle, evaluates release gates, and writes JSON + Markdown result artifacts
 * to out/soak/.
 *
 * Environment configuration (all optional):
 *   SOAK_DURATION_S        total run length in seconds      (default 1800)
 *   SOAK_RATE              base offered messages/second     (default 1000)
 *   SOAK_PARTITIONS        topic partitions                 (default 6)
 *   SOAK_MSG_BYTES         value size in bytes              (default 1024)
 *   SOAK_ACKS              "all" | "1"                      (default "all")
 *   SOAK_BURST_INTERVAL_S  seconds between bursts           (default 300, 0 = off)
 *   SOAK_BURST_S           burst length                     (default 60)
 *   SOAK_BURST_FACTOR      burst rate multiplier            (default 1.5)
 *   SOAK_SAMPLE_INTERVAL_S metric sampling interval          (default 10)
 *   SOAK_LOG_INTERVAL_S    console progress interval         (default 60)
 *   SOAK_RETENTION_MS      topic retention window             (default 3600000)
 *   SOAK_RETENTION_BYTES   topic retention per partition      (default 268435456)
 *   SOAK_SEGMENT_BYTES     topic segment size                  (default 16777216)
 *   SOAK_MAX_MESSAGES      consumer maxMessages per fetch    (default 500)
 *   SOAK_MAX_STABLE_RATE   measured maximum stable rate      (default 0 = unrecorded)
 *   SOAK_BROKER_VERSION    broker name and version           (default "unknown")
 *   KAFKA_BROKERS          comma-separated bootstrap brokers (default 127.0.0.1:9092)
 */

import { mkdirSync, readdirSync } from "node:fs";
import { Kafka } from "../index.ts";
import type { ConsumedMessage } from "../src/types.ts";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const env = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw === "") {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`Invalid ${name}: ${raw}`);
  }
  return value;
};

const DURATION_S = env("SOAK_DURATION_S", 1800);
const BASE_RATE = env("SOAK_RATE", 1_000);
const PARTITIONS = Math.floor(env("SOAK_PARTITIONS", 6));
const MSG_BYTES = Math.floor(env("SOAK_MSG_BYTES", 1024));
const ACKS: 1 | "all" = process.env.SOAK_ACKS === "1" ? 1 : "all";
const BURST_INTERVAL_S =
  process.env.SOAK_BURST_INTERVAL_S === "0" ? 0 : env("SOAK_BURST_INTERVAL_S", 300);
const BURST_S = env("SOAK_BURST_S", 60);
const BURST_FACTOR = Number(process.env.SOAK_BURST_FACTOR ?? 1.5) || 1.5;
const SAMPLE_INTERVAL_S = env("SOAK_SAMPLE_INTERVAL_S", 10);
const LOG_INTERVAL_S = env("SOAK_LOG_INTERVAL_S", 60);
const RETENTION_MS = Math.floor(env("SOAK_RETENTION_MS", 3_600_000));
const RETENTION_BYTES = Math.floor(env("SOAK_RETENTION_BYTES", 256 * 1024 * 1024));
const SEGMENT_BYTES = Math.floor(env("SOAK_SEGMENT_BYTES", 16 * 1024 * 1024));
const MAX_MESSAGES = Math.floor(env("SOAK_MAX_MESSAGES", 500));
const MAX_STABLE_RATE = Math.floor(Number(process.env.SOAK_MAX_STABLE_RATE ?? 0));
const BROKER_VERSION = process.env.SOAK_BROKER_VERSION ?? "unknown";
const BROKERS = (process.env.KAFKA_BROKERS ?? "127.0.0.1:9092").split(",");
const WARMUP_FRACTION = 0.2; // memory gate ignores growth during this fraction of the run

const topic = `soak-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
const startedMs = Date.now();
const endAt = startedMs + DURATION_S * 1_000;
const commit = Bun.spawnSync(["git", "rev-parse", "HEAD"], { stdout: "pipe" })
  .stdout.toString()
  .trim();

// ---------------------------------------------------------------------------
// Metrics helpers
// ---------------------------------------------------------------------------

/** Fixed-bucket latency histogram in milliseconds. */
class Histogram {
  readonly #bounds = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000];
  #counts = Array.from({ length: this.#bounds.length + 1 }, () => 0);
  count = 0;
  max = 0;

  add(ms: number): void {
    this.count++;
    if (ms > this.max) {
      this.max = ms;
    }
    let i = 0;
    while (i < this.#bounds.length && ms > this.#bounds[i]) {
      i++;
    }
    this.#counts[i]++;
  }

  /** Percentile in milliseconds. Bucket upper bounds make results conservative. */
  percentile(p: number): number {
    if (!this.count) {
      return 0;
    }
    let target = this.count * p;
    for (let i = 0; i < this.#counts.length; i++) {
      target -= this.#counts[i];
      if (target <= 0) {
        return i < this.#bounds.length ? this.#bounds[i] : this.max;
      }
    }
    return this.max;
  }

  snapshot() {
    return {
      p50: this.percentile(0.5),
      p95: this.percentile(0.95),
      p99: this.percentile(0.99),
      max: Math.round(this.max),
      count: this.count,
    };
  }

  reset(): void {
    this.#counts.fill(0);
    this.count = 0;
    this.max = 0;
  }
}

function fdCount(): number {
  try {
    return readdirSync("/proc/self/fd").length;
  } catch {
    return -1;
  }
}

let lastCpuWall = performance.now();
let lastCpuUsage = process.cpuUsage();
function cpuPercent(): number {
  const now = performance.now();
  const delta = process.cpuUsage(lastCpuUsage);
  lastCpuUsage = process.cpuUsage();
  const pct = ((delta.user + delta.system) / 1_000 / (now - lastCpuWall)) * 100;
  lastCpuWall = now;
  return Math.round(pct * 10) / 10;
}

const round = (n: number): number => Math.round(n * 10) / 10;

// ---------------------------------------------------------------------------
// Shared client and state
// ---------------------------------------------------------------------------

const kafka = new Kafka({
  brokers: BROKERS,
  clientId: "bun-kafka-soak",
  onEvent: (event) => {
    if (event.type === "retry") {
      retries++;
    } else if (event.type === "throttle") {
      throttles++;
    }
  },
});

const counters = {
  offered: 0,
  acknowledged: 0,
  failed: 0,
  consumed: 0,
  duplicates: 0,
  orderViolations: 0,
};
let retries = 0;
let throttles = 0;
let pendingSends = 0;

const producedPerPartition = Array.from({ length: PARTITIONS }, () => 0);
const consumedPerPartition = Array.from({ length: PARTITIONS }, () => 0);
const lastSeqPerPartition = Array.from({ length: PARTITIONS }, () => -1);

const windowSend = new Histogram();
const windowFetch = new Histogram();
const cumulativeSend = new Histogram();
const cumulativeFetch = new Histogram();

type Sample = Record<string, number>;
const samples: Sample[] = [];
const unhandled: unknown[] = [];
process.on("unhandledRejection", (error) => unhandled.push(error));

function lag(): number {
  let total = 0;
  for (let p = 0; p < PARTITIONS; p++) {
    total += Math.max(0, producedPerPartition[p] - consumedPerPartition[p]);
  }
  return total;
}

// ---------------------------------------------------------------------------
// Topic setup
// ---------------------------------------------------------------------------

async function setup(): Promise<void> {
  const admin = kafka.admin();
  try {
    await admin.createTopics([
      {
        name: topic,
        numPartitions: PARTITIONS,
        // The consumer keeps up with production, so retaining the whole run
        // only measures broker disk growth rather than client behavior.
        configs: {
          "cleanup.policy": "delete",
          "retention.ms": String(RETENTION_MS),
          "retention.bytes": String(RETENTION_BYTES),
          "segment.bytes": String(SEGMENT_BYTES),
        },
      },
    ]);
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      const meta = await admin.metadata([topic]);
      const found = meta.topics.find((t) => t.name === topic);
      if (
        found &&
        !found.err &&
        found.partitions.length === PARTITIONS &&
        found.partitions.every((p) => p.leader >= 0)
      ) {
        return;
      }
      await Bun.sleep(150);
    }
    throw new Error(`Topic ${topic} did not become ready`);
  } finally {
    await admin.close();
  }
}

// ---------------------------------------------------------------------------
// Producer loop
// ---------------------------------------------------------------------------

let queuedRef: { readonly queuedMessages: number } | null = null;

async function producerLoop(): Promise<void> {
  const producer = kafka.producer({ lingerMs: 5, batchMaxMessages: 1_000 });
  queuedRef = producer;
  const TICK_MS = 50;
  const payload = new Uint8Array(MSG_BYTES).fill(0x61);
  let seq = 0;

  try {
    while (Date.now() < endAt) {
      const elapsedS = (Date.now() - startedMs) / 1_000;
      const burstStart = BURST_INTERVAL_S / 2;
      const inBurst =
        BURST_INTERVAL_S > 0 &&
        elapsedS >= burstStart &&
        (elapsedS - burstStart) % BURST_INTERVAL_S < BURST_S;
      const rate = inBurst ? BASE_RATE * BURST_FACTOR : BASE_RATE;
      const batchSize = Math.max(1, Math.round((rate * TICK_MS) / 1_000));

      const t0 = performance.now();
      const batch: Promise<unknown>[] = [];
      for (let i = 0; i < batchSize; i++) {
        const partition = seq % PARTITIONS;
        const messageSeq = seq;
        pendingSends++;
        counters.offered++;
        producedPerPartition[partition]++;
        batch.push(
          producer
            .send({
              topic,
              acks: ACKS,
              messages: [{ partition, key: String(messageSeq), value: payload }],
            })
            .then(
              (results) => {
                pendingSends--;
                counters.acknowledged++;
                void results;
              },
              () => {
                pendingSends--;
                counters.failed++;
              },
            ),
        );
        seq++;
      }
      await Promise.all(batch).catch(() => {});
      sendLatency(performance.now() - t0);

      // Pace to the tick boundary so the offered rate stays constant even when
      // acknowledgements are faster than a tick.
      const spent = performance.now() - t0;
      if (spent < TICK_MS) {
        await Bun.sleep(TICK_MS - spent);
      }
    }
  } finally {
    try {
      await producer.flush();
    } finally {
      await producer.close();
    }
  }
}

function sendLatency(ms: number): void {
  windowSend.add(ms);
  cumulativeSend.add(ms);
}

// ---------------------------------------------------------------------------
// Consumer loop
// ---------------------------------------------------------------------------

async function consumerLoop(): Promise<void> {
  const consumerKafka = new Kafka({ brokers: BROKERS, clientId: "bun-kafka-soak-consumer" });
  const consumer = consumerKafka.consumer({ fromBeginning: true });
  const decoder = new TextDecoder();
  await consumer.assign(
    Array.from({ length: PARTITIONS }, (_, partition) => ({
      topic,
      partition,
      offset: "earliest" as const,
    })),
  );
  try {
    while (Date.now() < endAt + 120_000) {
      const t0 = performance.now();
      const messages = await consumer.fetch({ maxMessages: MAX_MESSAGES, maxWaitMs: 250 });
      fetchLatency(performance.now() - t0);
      processConsumedMessages(messages, decoder);
      if (!messages.length && Date.now() >= endAt && lag() === 0) {
        break;
      }
    }
  } finally {
    await consumer.close();
    await consumerKafka.disconnect();
  }
}

function processConsumedMessages(messages: ConsumedMessage[], decoder: TextDecoder): void {
  for (const message of messages) {
    const sequence = Number(decoder.decode(message.key));
    if (sequence < 0) {
      continue;
    }
    consumedPerPartition[message.partition]++;
    updateSequence(message.partition, sequence);
    counters.consumed++;
  }
}

function updateSequence(partition: number, sequence: number): void {
  const previous = lastSeqPerPartition[partition];
  if (sequence > previous) {
    lastSeqPerPartition[partition] = sequence;
  } else if (sequence === previous) {
    counters.duplicates++;
  } else {
    counters.orderViolations++;
  }
}

function fetchLatency(ms: number): void {
  windowFetch.add(ms);
  cumulativeFetch.add(ms);
}

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------

async function samplingLoop(): Promise<void> {
  let lastLogAt = Number.NEGATIVE_INFINITY;
  while (Date.now() < endAt) {
    await Bun.sleep(SAMPLE_INTERVAL_S * 1_000);
    const stats = kafka.stats();
    const mem = process.memoryUsage();
    const elapsedS = Math.max(1, (Date.now() - startedMs) / 1_000);
    const s = windowSend.snapshot();
    const f = windowFetch.snapshot();
    const sample = {
      t_s: round(elapsedS),
      offered: counters.offered,
      acknowledged: counters.acknowledged,
      failed: counters.failed,
      consumed: counters.consumed,
      produce_mps: round(counters.offered / elapsedS),
      consume_mps: round(counters.consumed / elapsedS),
      produced_bytes_s: round((counters.acknowledged * MSG_BYTES) / elapsedS),
      send_p50_ms: s.p50,
      send_p95_ms: s.p95,
      send_p99_ms: s.p99,
      send_max_ms: s.max,
      fetch_p50_ms: f.p50,
      fetch_p95_ms: f.p95,
      fetch_p99_ms: f.p99,
      fetch_max_ms: f.max,
      lag_total: lag(),
      pending_sends: pendingSends,
      queued_messages: queuedRef?.queuedMessages ?? 0,
      connections: Number(stats.connections ?? 0),
      requests: Number(stats.requests ?? 0),
      retries: Number(stats.retries ?? 0),
      throttle_events: throttles,
      rss_bytes: mem.rss,
      heap_used_bytes: mem.heapUsed,
      cpu_percent: cpuPercent(),
      fds: fdCount(),
    } satisfies Sample;
    samples.push(sample);
    windowSend.reset();
    windowFetch.reset();
    if (sample.t_s - lastLogAt >= LOG_INTERVAL_S) {
      console.log(
        `t=${String(sample.t_s).padStart(6)}s prod=${sample.produce_mps}/s cons=${sample.consume_mps}/s ` +
          `lag=${sample.lag_total} sendP95=${s.p95}ms fetchP95=${f.p95}ms rss=${Math.round(mem.rss / 1048576)}MiB ` +
          `cpu=${sample.cpu_percent}% fds=${sample.fds} fail=${counters.failed}`,
      );
      lastLogAt = sample.t_s;
    }
  }
}

// ---------------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------------

interface GateResult {
  name: string;
  passed: boolean;
  detail: string;
}

function evaluateGates(durationS: number): GateResult[] {
  return [
    ...basicGates(),
    ...memoryGates(),
    ...throughputGates(durationS),
    ...latencyGates(),
    ...burstGates(durationS),
  ];
}

function basicGates(): GateResult[] {
  const missing = lag();
  return [
    gate("zero-failed-acks", counters.failed === 0, `${counters.failed} failed acknowledgements`),
    gate(
      "zero-duplicates",
      counters.duplicates === 0,
      `${counters.duplicates} duplicate records observed by the oracle`,
    ),
    gate(
      "per-partition-order",
      counters.orderViolations === 0,
      `${counters.orderViolations} out-of-order sequences`,
    ),
    gate(
      "zero-missing-after-drain",
      missing === 0,
      `${missing} acknowledged-but-unconsumed records after final drain`,
    ),
    gate(
      "no-unhandled-rejections",
      unhandled.length === 0,
      `${unhandled.length} unhandled promise rejections`,
    ),
  ];
}

function memoryGates(): GateResult[] {
  const postWarmup = samples.filter((sample) => sample.t_s >= DURATION_S * WARMUP_FRACTION);
  if (postWarmup.length < 2) {
    return [];
  }
  const rss = postWarmup.map((sample) => sample.rss_bytes);
  const growthMiB = (Math.max(...rss) - Math.min(...rss)) / 1048576;
  return [
    gate(
      "memory-growth-below-64mib",
      growthMiB < 64,
      `${growthMiB.toFixed(1)} MiB RSS range after ${round(DURATION_S * WARMUP_FRACTION)}s warmup`,
    ),
  ];
}

function throughputGates(durationS: number): GateResult[] {
  if (samples.length < 4) {
    return [];
  }
  const quarter = Math.max(1, Math.floor(samples.length / 4));
  const first = avg(samples.slice(0, quarter), (sample) => sample.produce_mps);
  const last = avg(samples.slice(-quarter), (sample) => sample.produce_mps);
  const enforced = durationS >= 900;
  return [
    gate(
      "throughput-decay-below-5-percent",
      !enforced || last >= first * 0.95,
      `first quarter ${first.toFixed(0)} msg/s vs final quarter ${last.toFixed(0)} msg/s${
        enforced ? "" : " (informational; run shorter than 15 min)"
      }`,
    ),
  ];
}

function latencyGates(): GateResult[] {
  if (samples.length < 2) {
    return [];
  }
  const half = Math.max(1, Math.floor(samples.length / 2));
  const first = samples.slice(0, half);
  const last = samples.slice(-half);
  const p95First = avg(first, (sample) => sample.send_p95_ms);
  const p95Last = avg(last, (sample) => sample.send_p95_ms);
  const p99First = avg(first, (sample) => sample.send_p99_ms);
  const p99Last = avg(last, (sample) => sample.send_p99_ms);
  const drift95 = p95First > 0 ? (p95Last / p95First - 1) * 100 : 0;
  const drift99 = p99First > 0 ? (p99Last / p99First - 1) * 100 : 0;
  return [
    gate(
      "send-p95-drift-below-20-percent",
      drift95 <= 20,
      `p95 ${p95First.toFixed(1)}ms -> ${p95Last.toFixed(1)}ms (${drift95.toFixed(1)}%)`,
    ),
    gate(
      "send-p99-drift-below-25-percent",
      drift99 <= 25,
      `p99 ${p99First.toFixed(1)}ms -> ${p99Last.toFixed(1)}ms (${drift99.toFixed(1)}%)`,
    ),
  ];
}

function burstGates(durationS: number): GateResult[] {
  if (BURST_INTERVAL_S <= 0) {
    return [];
  }
  const recoverBoundS = Math.min(600, SAMPLE_INTERVAL_S * 3);
  let recovered = true;
  let worstOvershoot = 0;
  for (const burstStart of burstTimes()) {
    const burstEnd = burstStart + BURST_S;
    const preLag = minLagBetween(burstStart - BURST_INTERVAL_S / 2, burstStart);
    const deadline = Math.min(burstEnd + recoverBoundS, durationS + 120);
    const postWindow = samples.filter((sample) => sample.t_s >= burstEnd && sample.t_s <= deadline);
    const peak = postWindow.length
      ? Math.max(...postWindow.map((sample) => sample.lag_total))
      : lag();
    if (preLag === null) {
      continue;
    }
    const overshoot = peak - preLag;
    worstOvershoot = Math.max(worstOvershoot, overshoot);
    if (peak > preLag + BASE_RATE * BURST_FACTOR * 0.05) {
      recovered = false;
    }
  }
  return [
    gate(
      "lag-recovers-after-each-burst",
      recovered,
      `worst post-burst lag overshoot ${worstOvershoot} records within ${recoverBoundS}s of burst end`,
    ),
  ];
}

function gate(name: string, passed: boolean, detail: string): GateResult {
  return { name, passed, detail };
}

function burstTimes(): number[] {
  const times: number[] = [];
  if (BURST_INTERVAL_S <= 0) {
    return times;
  }
  for (let t = BURST_INTERVAL_S / 2; t < DURATION_S; t += BURST_INTERVAL_S) {
    times.push(t);
  }
  return times;
}

function minLagBetween(fromS: number, toS: number): number | null {
  const window = samples.filter((s) => s.t_s >= fromS && s.t_s <= toS);
  if (!window.length) {
    return null;
  }
  return Math.min(...window.map((s) => s.lag_total));
}

function avg(items: Sample[], pick: (item: Sample) => number): number {
  if (!items.length) {
    return 0;
  }
  return items.reduce((sum, item) => sum + pick(item), 0) / items.length;
}

// ---------------------------------------------------------------------------
// Artifacts
// ---------------------------------------------------------------------------

type ArtifactValue = string | number | boolean;
type ArtifactFields = { readonly [key: string]: ArtifactValue };

interface SoakArtifact {
  commit: string;
  started_at: string;
  duration_seconds: number;
  environment: ArtifactFields;
  workload: ArtifactFields;
  result: ArtifactFields;
  samples: Sample[];
  gates: GateResult[];
  passed: boolean;
}

function burstDescription(workload: ArtifactFields): string {
  const interval = Number(workload.burst_interval_s);
  return interval
    ? `${workload.burst_factor}x for ${workload.burst_length_s}s every ${interval}s`
    : "off";
}

function renderReport(a: SoakArtifact): string {
  const lines: string[] = [
    "# Soak test result",
    "",
    `- Commit: \`${a.commit}\``,
    `- Started: ${new Date(startedMs).toISOString()}`,
    `- Duration: ${a.duration_seconds} seconds`,
    `- Bun: ${a.environment.bun}`,
    `- Broker: ${a.environment.broker_version}`,
    "",
    "## Workload",
    "",
    `- Topic partitions: ${a.workload.partitions}`,
    `- Message size: ${a.workload.message_bytes} bytes`,
    `- Base offered rate: ${a.workload.producer_rate} msg/s`,
    `- Measured maximum stable rate: ${a.workload.maximum_stable_rate || "not recorded"} msg/s`,
    `- Acks: ${a.workload.acks}`,
    `- Burst: ${burstDescription(a.workload)}`,
    `- Retention: ${a.workload.retention_ms} ms / ${a.workload.retention_bytes_per_partition} bytes per partition`,
    `- Segment size: ${a.workload.segment_bytes} bytes`,
    "",
    "## Result",
    "",
    `- Offered: ${a.result.offered}`,
    `- Acknowledged: ${a.result.acknowledged}`,
    `- Failed: ${a.result.failed}`,
    `- Consumed: ${a.result.consumed}`,
    `- Missing after drain: ${a.result.missing}`,
    `- Duplicates: ${a.result.duplicates}`,
    `- Order violations: ${a.result.order_violations}`,
    `- Send p50/p95/p99/max: ${a.result.send_p50_ms}/${a.result.send_p95_ms}/${a.result.send_p99_ms}/${a.result.send_max_ms} ms`,
    `- Fetch p50/p95/p99/max: ${a.result.fetch_p50_ms}/${a.result.fetch_p95_ms}/${a.result.fetch_p99_ms}/${a.result.fetch_max_ms} ms`,
    `- RSS start -> end: ${Math.round(Number(a.result.rss_start_bytes) / 1048576)} MiB -> ${Math.round(Number(a.result.rss_end_bytes) / 1048576)} MiB`,
    `- Requests total: ${a.result.requests_total}; retries: ${a.result.retries}; throttles: ${a.result.throttle_events}`,
    "",
    "## Gates",
    "",
    ...a.gates.map((g) => `- ${g.passed ? "**PASS**" : "**FAIL**"} ${g.name}: ${g.detail}`),
    "",
    `**Overall: ${a.passed ? "PASS" : "FAIL"}**`,
    "",
  ];
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<number> {
  const burst = BURST_INTERVAL_S
    ? `${BURST_FACTOR}x/${BURST_S}s every ${BURST_INTERVAL_S}s`
    : "off";
  console.log(
    `soak: topic=${topic} duration=${DURATION_S}s rate=${BASE_RATE}/s partitions=${PARTITIONS} ` +
      `msg=${MSG_BYTES}B acks=${ACKS} burst=${burst}`,
  );
  await setup();

  const producing = producerLoop().catch(async (error) => {
    console.error("producer loop failed:", error);
    return Promise.reject(error);
  });
  const sampling = samplingLoop();
  const consuming = consumerLoop().catch((error) => {
    console.error("consumer loop failed:", error);
  });

  await producing.catch(() => {});
  await sampling;
  await consuming;

  const statsFinal = kafka.stats();
  const memFinal = process.memoryUsage();
  const durationS = (Date.now() - startedMs) / 1_000;
  await kafka.disconnect();

  const gates = evaluateGates(durationS);
  const allPassed = gates.every((check) => check.passed);
  const sendSummary = cumulativeSend.snapshot();
  const fetchSummary = cumulativeFetch.snapshot();

  // Best-effort cleanup so soak topics do not accumulate on the dev broker.
  try {
    const admin = kafka.admin();
    await admin.deleteTopics([topic]);
    await admin.close();
  } catch (error) {
    console.warn(`soak: topic cleanup skipped (${error})`);
  }

  const artifact: SoakArtifact = {
    commit,
    started_at: new Date(startedMs).toISOString(),
    duration_seconds: round(durationS),
    environment: {
      bun: Bun.version,
      broker_version: BROKER_VERSION,
      platform: `${process.platform}-${process.arch}`,
      cpus: navigator.hardwareConcurrency,
      brokers: BROKERS.join(","),
    },
    workload: {
      topic,
      message_bytes: MSG_BYTES,
      partitions: PARTITIONS,
      producer_rate: BASE_RATE,
      maximum_stable_rate: MAX_STABLE_RATE,
      maximum_stable_rate_fraction: MAX_STABLE_RATE ? round(BASE_RATE / MAX_STABLE_RATE) : 0,
      acks: ACKS,
      tls: false,
      copy: false,
      burst_interval_s: BURST_INTERVAL_S,
      burst_factor: BURST_FACTOR,
      burst_length_s: BURST_S,
      sample_interval_s: SAMPLE_INTERVAL_S,
      log_interval_s: LOG_INTERVAL_S,
      retention_ms: RETENTION_MS,
      retention_bytes_per_partition: RETENTION_BYTES,
      segment_bytes: SEGMENT_BYTES,
      consumer_max_messages: MAX_MESSAGES,
    },
    result: {
      offered: counters.offered,
      acknowledged: counters.acknowledged,
      failed: counters.failed,
      consumed: counters.consumed,
      missing: lag(),
      duplicates: counters.duplicates,
      order_violations: counters.orderViolations,
      retries: retries,
      throttle_events: throttles,
      requests_total: Number(statsFinal.requests ?? 0),
      connections_final: Number(statsFinal.connections ?? 0),
      produce_mps_avg: round(counters.offered / Math.max(1, durationS)),
      consume_mps_avg: round(counters.consumed / Math.max(1, durationS)),
      send_p50_ms: sendSummary.p50,
      send_p95_ms: sendSummary.p95,
      send_p99_ms: sendSummary.p99,
      send_max_ms: sendSummary.max,
      fetch_p50_ms: fetchSummary.p50,
      fetch_p95_ms: fetchSummary.p95,
      fetch_p99_ms: fetchSummary.p99,
      fetch_max_ms: fetchSummary.max,
      rss_start_bytes: samples[0]?.rss_bytes ?? memFinal.rss,
      rss_end_bytes: memFinal.rss,
      heap_end_bytes: memFinal.heapUsed,
      fds_final: fdCount(),
      unhandled_rejections: unhandled.length,
    },
    samples,
    gates,
    passed: allPassed,
  };

  const directory = new URL("../out/soak/", import.meta.url).pathname;
  mkdirSync(directory, { recursive: true });
  const stamp = new Date(startedMs).toISOString().replaceAll(":", "-");
  await Bun.write(`${directory}${stamp}.json`, `${JSON.stringify(artifact, null, 2)}\n`);
  await Bun.write(`${directory}${stamp}.md`, renderReport(artifact));

  console.log(`\nsoak: ${allPassed ? "PASS" : "FAIL"}`);
  for (const check of gates) {
    console.log(`  ${check.passed ? "ok  " : "FAIL"} ${check.name}: ${check.detail}`);
  }
  console.log(`artifacts: out/soak/${stamp}.{json,md}`);
  return allPassed ? 0 : 1;
}

await main().then((code) => process.exit(code));
