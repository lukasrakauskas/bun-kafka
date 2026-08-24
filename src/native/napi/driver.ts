import { existsSync } from "node:fs";
import { join } from "node:path";
import { KafkaError } from "../../errors.ts";
import type {
  ClusterMetadata, KafkaConfig, KafkaMessage, ProduceInput, TopicPartition, Watermarks,
} from "../../types.ts";
import type {
  ClientEventHandlers, NativeAdmin, NativeConsumer, NativeDriver, NativeProducer,
} from "../types.ts";

type Addon = {
  version(): { number: number; string: string };
  err2str(code: number): string;
  producerNew(cfg: Record<string, string>): unknown;
  consumerNew(cfg: Record<string, string>): unknown;
  handleClose(h: unknown, isConsumer: number): void;
  producerSend(h: unknown, msg: any): void;
  producerFlush(h: unknown, timeoutMs: number): void;
  producerPoll(h: unknown, timeoutMs: number): number;
  producerOutq(h: unknown): number;
  consumerSubscribe(h: unknown, topics: string[]): void;
  consumerUnsubscribe(h: unknown): void;
  consumerSubscription(h: unknown): TopicPartition[];
  consumerAssign(h: unknown, parts: TopicPartition[]): void;
  consumerAssignment(h: unknown): TopicPartition[];
  consumerPoll(h: unknown, timeoutMs: number): any;
  consumerCommit(h: unknown, offsets: TopicPartition[] | null, async: number): void;
  consumerCommitted(h: unknown, parts: TopicPartition[], timeoutMs: number): TopicPartition[];
  consumerPosition(h: unknown, parts: TopicPartition[]): TopicPartition[];
  consumerSeek(h: unknown, parts: TopicPartition[], timeoutMs: number): void;
  consumerPause(h: unknown, parts: TopicPartition[]): void;
  consumerResume(h: unknown, parts: TopicPartition[]): void;
  consumerStore(h: unknown, parts: TopicPartition[]): void;
  queryWatermarkOffsets(h: unknown, topic: string, partition: number, timeoutMs: number): Watermarks;
  getWatermarkOffsets(h: unknown, topic: string, partition: number): Watermarks;
  offsetsForTimes(h: unknown, parts: TopicPartition[], timeoutMs: number): TopicPartition[];
  memberId(h: unknown): string | null;
  assignmentLost(h: unknown): number;
  rebalanceProtocol(h: unknown): string;
  fatalError?(h: unknown): { code: number; message: string } | null;
  messageDone(wrap: unknown): void;
  adminMetadata(h: unknown, allTopics: number, timeoutMs: number): ClusterMetadata;
  adminClusterId(h: unknown, timeoutMs: number): string | null;
};

function loadAddon(): Addon {
  const candidates = [
    process.env.BUN_KAFKA_NAPI,
    join(import.meta.dir, "../../../native/build/bun_kafka_native.node"),
    join(process.cwd(), "native/build/bun_kafka_native.node"),
  ].filter(Boolean) as string[];
  for (const p of candidates) {
    if (existsSync(p)) return require(p) as Addon;
  }
  throw new Error("NAPI addon not built. Run: bun run build:napi");
}

function flatten(cfg: KafkaConfig): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(cfg)) {
    if (k.startsWith("bun.kafka.")) continue;
    out[k] = typeof v === "boolean" ? (v ? "true" : "false") : String(v);
  }
  return out;
}

function mapErr(e: unknown): never {
  const msg = e instanceof Error ? e.message : String(e);
  const m = /^(-?\d+):(.*)$/.exec(msg);
  if (m) throw new KafkaError(Number(m[1]), m[2]);
  throw e instanceof Error ? e : new Error(msg);
}

function wrapMsg(a: Addon, raw: any): KafkaMessage {
  const wrap = raw._wrap;
  let done = false;
  return {
    topic: raw.topic,
    partition: raw.partition,
    offset: typeof raw.offset === "bigint" ? raw.offset : BigInt(raw.offset),
    key: raw.key ? new Uint8Array(raw.key) : null,
    value: raw.value ? new Uint8Array(raw.value) : null,
    timestamp: typeof raw.timestamp === "bigint" ? raw.timestamp : BigInt(raw.timestamp ?? 0),
    timestampType: raw.timestampType ?? 0,
    headers: Object.fromEntries(
      Object.entries(raw.headers ?? {}).map(([k, v]) => [k, v ? new Uint8Array(v as ArrayBuffer) : null]),
    ),
    brokerId: raw.brokerId ?? -1,
    done() {
      if (done) return;
      done = true;
      a.messageDone(wrap);
    },
  };
}

let addon: Addon | null = null;
function A(): Addon {
  return (addon ??= loadAddon());
}

function readFatal(h: unknown): KafkaError | null {
  const a = A();
  if (!a.fatalError) return null;
  const f = a.fatalError(h);
  if (!f) return null;
  return new KafkaError(f.code, f.message, { fatal: true });
}

class NapiProducer implements NativeProducer {
  #h: unknown;
  #closed = false;
  constructor(cfg: KafkaConfig, _handlers: ClientEventHandlers = {}) {
    // Delivery/error callbacks require C-side support; flush remains durability boundary.
    // Handlers.onDelivery is reserved for a future NAPI callback bridge.
    try {
      this.#h = A().producerNew(flatten({
        "bootstrap.servers": "localhost:9092",
        "allow.auto.create.topics": "true",
        "linger.ms": 5,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 100000,
        "socket.nagle.disable": true,
        ...cfg,
      }));
    } catch (e) { mapErr(e); }
  }
  send(msg: ProduceInput) {
    this.#e();
    try { A().producerSend(this.#h, msg); } catch (e) { mapErr(e); }
  }
  poll(t: number) { this.#e(); return A().producerPoll(this.#h, t); }
  flush(t: number) { this.#e(); try { A().producerFlush(this.#h, t); } catch (e) { mapErr(e); } }
  outQueueLength() { this.#e(); return A().producerOutq(this.#h); }
  fatalError() { return this.#closed ? null : readFatal(this.#h); }
  close() { if (this.#closed) return; this.#closed = true; A().handleClose(this.#h, 0); }
  #e() { if (this.#closed) throw new Error("Producer is closed"); }
}

class NapiConsumer implements NativeConsumer {
  #h: unknown;
  #closed = false;
  constructor(cfg: KafkaConfig, _handlers: ClientEventHandlers = {}) {
    // No custom rebalance_cb => librdkafka automatic assign/revoke (production default).
    try {
      this.#h = A().consumerNew(flatten({
        "bootstrap.servers": "localhost:9092",
        "group.id": cfg["group.id"] ?? `bun-kafka-${crypto.randomUUID()}`,
        "enable.auto.commit": cfg["enable.auto.commit"] ?? false,
        "auto.offset.reset": cfg["auto.offset.reset"] ?? "earliest",
        "allow.auto.create.topics": "true",
        "fetch.wait.max.ms": 50,
        "fetch.min.bytes": 1,
        ...cfg,
      }));
    } catch (e) { mapErr(e); }
  }
  subscribe(topics: string[]) { this.#e(); try { A().consumerSubscribe(this.#h, topics); } catch (e) { mapErr(e); } }
  unsubscribe() { this.#e(); try { A().consumerUnsubscribe(this.#h); } catch (e) { mapErr(e); } }
  subscription() { this.#e(); try { return A().consumerSubscription(this.#h); } catch (e) { mapErr(e); } }
  assign(p: TopicPartition[]) { this.#e(); try { A().consumerAssign(this.#h, p); } catch (e) { mapErr(e); } }
  assignment() { this.#e(); try { return A().consumerAssignment(this.#h); } catch (e) { mapErr(e); } }
  assignmentLost() { return !!A().assignmentLost(this.#h); }
  rebalanceProtocol() { return A().rebalanceProtocol(this.#h) ?? "NONE"; }
  poll(timeoutMs: number) {
    this.#e();
    try {
      const raw = A().consumerPoll(this.#h, timeoutMs);
      if (!raw) return null;
      return wrapMsg(A(), raw);
    } catch (e) { mapErr(e); }
  }
  pollBatch(timeoutMs: number, max: number) {
    this.#e();
    const out: KafkaMessage[] = [];
    try {
      const first = A().consumerPoll(this.#h, timeoutMs);
      if (!first) return out;
      out.push(wrapMsg(A(), first));
      while (out.length < max) {
        const raw = A().consumerPoll(this.#h, 0);
        if (!raw) break;
        out.push(wrapMsg(A(), raw));
      }
    } catch (e) { mapErr(e); }
    return out;
  }
  commit(offsets: TopicPartition[] | null, async: boolean) {
    this.#e();
    try { A().consumerCommit(this.#h, offsets, async ? 1 : 0); } catch (e) { mapErr(e); }
  }
  committed(p: TopicPartition[], t: number) { this.#e(); try { return A().consumerCommitted(this.#h, p, t); } catch (e) { mapErr(e); } }
  position(p: TopicPartition[]) { this.#e(); try { return A().consumerPosition(this.#h, p); } catch (e) { mapErr(e); } }
  seek(p: TopicPartition[], t: number) { this.#e(); try { A().consumerSeek(this.#h, p, t); } catch (e) { mapErr(e); } }
  pause(p: TopicPartition[]) { this.#e(); try { A().consumerPause(this.#h, p); } catch (e) { mapErr(e); } }
  resume(p: TopicPartition[]) { this.#e(); try { A().consumerResume(this.#h, p); } catch (e) { mapErr(e); } }
  storeOffsets(p: TopicPartition[]) { this.#e(); try { A().consumerStore(this.#h, p); } catch (e) { mapErr(e); } }
  queryWatermarkOffsets(topic: string, partition: number, t: number) {
    this.#e(); try { return A().queryWatermarkOffsets(this.#h, topic, partition, t); } catch (e) { mapErr(e); }
  }
  getWatermarkOffsets(topic: string, partition: number) {
    this.#e(); try { return A().getWatermarkOffsets(this.#h, topic, partition); } catch (e) { mapErr(e); }
  }
  offsetsForTimes(q: TopicPartition[], t: number) {
    this.#e(); try { return A().offsetsForTimes(this.#h, q, t); } catch (e) { mapErr(e); }
  }
  memberId() { this.#e(); return A().memberId(this.#h); }
  fatalError() { return this.#closed ? null : readFatal(this.#h); }
  close() { if (this.#closed) return; this.#closed = true; A().handleClose(this.#h, 1); }
  #e() { if (this.#closed) throw new Error("Consumer is closed"); }
}

class NapiAdmin implements NativeAdmin {
  #h: unknown;
  #closed = false;
  constructor(cfg: KafkaConfig, _handlers: ClientEventHandlers = {}) {
    try {
      this.#h = A().producerNew(flatten({ "bootstrap.servers": "localhost:9092", ...cfg }));
    } catch (e) { mapErr(e); }
  }
  metadata(allTopics: boolean, timeoutMs: number) {
    this.#e();
    try { return A().adminMetadata(this.#h, allTopics ? 1 : 0, timeoutMs); } catch (e) { mapErr(e); }
  }
  clusterId(timeoutMs: number) {
    this.#e();
    return A().adminClusterId(this.#h, timeoutMs);
  }
  fatalError() { return this.#closed ? null : readFatal(this.#h); }
  close() { if (this.#closed) return; this.#closed = true; A().handleClose(this.#h, 0); }
  #e() { if (this.#closed) throw new Error("Admin is closed"); }
}

export const napiDriver: NativeDriver = {
  kind: "napi",
  version: () => A().version(),
  err2str: (c) => A().err2str(c),
  producer: (c, h) => new NapiProducer(c, h),
  consumer: (c, h) => new NapiConsumer(c, h),
  admin: (c, h) => new NapiAdmin(c, h),
};

export function napiAvailable(): boolean {
  try { loadAddon(); return true; } catch { return false; }
}
