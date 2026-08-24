import {
  rk, ptr, read, toArrayBuffer, CString, C, type Pointer,
} from "./bindings.ts";
import { KafkaError } from "../../errors.ts";
import type {
  ClusterMetadata, KafkaConfig, KafkaMessage, ProduceInput, TopicPartition, Watermarks,
} from "../../types.ts";
import type { NativeAdmin, NativeConsumer, NativeDriver, NativeProducer } from "../types.ts";

const enc = (s: string) => new TextEncoder().encode(s);

function toBytes(v: ProduceInput["value"]): Uint8Array | null {
  if (v == null) return null;
  if (typeof v === "string") return enc(v);
  if (v instanceof Uint8Array) return v;
  return new Uint8Array(v);
}

function check(code: number, what?: string) {
  if (code === 0) return;
  const m = rk.rd_kafka_err2str(code) || `err ${code}`;
  throw new KafkaError(code, what ? `${what}: ${m}` : m);
}

function fromErr(e: any): never {
  const code = rk.rd_kafka_error_code(e);
  const msg = rk.rd_kafka_error_string(e);
  const fatal = !!rk.rd_kafka_error_is_fatal(e);
  const retriable = !!rk.rd_kafka_error_is_retriable(e);
  rk.rd_kafka_error_destroy(e);
  throw new KafkaError(code, msg, { fatal, retriable });
}

function confSet(conf: Pointer, cfg: KafkaConfig) {
  const errbuf = new Uint8Array(512);
  for (const [k, raw] of Object.entries(cfg)) {
    const val = typeof raw === "boolean" ? (raw ? "true" : "false") : String(raw);
    const res = rk.rd_kafka_conf_set(conf, k, val, ptr(errbuf), BigInt(errbuf.byteLength));
    if (res !== C.CONF_OK) {
      const msg = new TextDecoder().decode(errbuf).replace(/\0.*$/, "");
      throw new KafkaError(res, `config ${k}: ${msg}`);
    }
  }
}

function newRk(type: number, cfg: KafkaConfig): Pointer {
  const conf = rk.rd_kafka_conf_new()!;
  confSet(conf, cfg);
  const errbuf = new Uint8Array(512);
  const h = rk.rd_kafka_new(type, conf, ptr(errbuf), BigInt(errbuf.byteLength));
  if (!h) throw new Error(new TextDecoder().decode(errbuf).replace(/\0.*$/, "") || "rd_kafka_new failed");
  return h;
}

function makeList(parts: Array<string | TopicPartition>): Pointer {
  const list = rk.rd_kafka_topic_partition_list_new(parts.length)!;
  for (const p of parts) {
    if (typeof p === "string") rk.rd_kafka_topic_partition_list_add(list, p, -1);
    else {
      rk.rd_kafka_topic_partition_list_add(list, p.topic, p.partition);
      if (p.offset != null) {
        check(
          rk.rd_kafka_topic_partition_list_set_offset(list, p.topic, p.partition, BigInt(p.offset)),
          "set_offset",
        );
      }
    }
  }
  return list;
}

function readList(list: Pointer): TopicPartition[] {
  const cnt = read.i32(list as any, C.TPL.cnt);
  const elems = read.ptr(list as any, C.TPL.elems);
  const out: TopicPartition[] = [];
  if (!elems || cnt <= 0) return out;
  for (let i = 0; i < cnt; i++) {
    const base = (elems as number) + i * C.TP.size;
    const topicPtr = read.ptr(base as any, C.TP.topic);
    out.push({
      topic: topicPtr ? new CString(topicPtr as any) : "",
      partition: read.i32(base as any, C.TP.partition),
      offset: read.i64(base as any, C.TP.offset),
      err: read.i32(base as any, C.TP.err),
    });
  }
  return out;
}

function viewAt(p: any, len: any): Uint8Array | null {
  if (!p || !len) return null;
  const n = Number(len);
  if (n <= 0) return null;
  return new Uint8Array(toArrayBuffer(p, 0, n));
}

// reusable header out-slots
const hdrSlot = new BigUint64Array(1);
const nameSlot = new BigUint64Array(1);
const valSlot = new BigUint64Array(1);
const sizeSlot = new BigUint64Array(1);
const tsTypeBuf = new Int32Array(1);

function readHeaders(msg: Pointer): Record<string, Uint8Array | null> {
  if (rk.rd_kafka_message_headers(msg, ptr(hdrSlot)) !== 0 || !hdrSlot[0]) return {};
  const hdrs = Number(hdrSlot[0]) as any;
  const out: Record<string, Uint8Array | null> = {};
  for (let i = 0; ; i++) {
    if (rk.rd_kafka_header_get_all(hdrs, BigInt(i), ptr(nameSlot), ptr(valSlot), ptr(sizeSlot)) !== 0) break;
    const name = nameSlot[0] ? new CString(Number(nameSlot[0]) as any) : "";
    const size = Number(sizeSlot[0] ?? 0n);
    out[name] = valSlot[0] && size > 0
      ? new Uint8Array(toArrayBuffer(Number(valSlot[0]) as any, 0, size)).slice()
      : null;
  }
  return out;
}

function readMsg(msg: Pointer, withHeaders = false): KafkaMessage {
  const err = read.i32(msg as any, C.MSG.err);
  if (err !== 0) {
    const estr = rk.rd_kafka_message_errstr(msg) || rk.rd_kafka_err2str(err);
    rk.rd_kafka_message_destroy(msg);
    throw new KafkaError(err, estr);
  }
  const rkt = read.ptr(msg as any, C.MSG.rkt);
  const timestamp = rk.rd_kafka_message_timestamp(msg, ptr(tsTypeBuf));
  let freed = false;
  let headers: Record<string, Uint8Array | null> | null = withHeaders ? readHeaders(msg) : null;
  return {
    topic: rkt ? (rk.rd_kafka_topic_name(rkt as any) ?? "") : "",
    partition: read.i32(msg as any, C.MSG.partition),
    offset: read.i64(msg as any, C.MSG.offset),
    key: viewAt(read.ptr(msg as any, C.MSG.key), read.u64(msg as any, C.MSG.key_len)),
    value: viewAt(read.ptr(msg as any, C.MSG.payload), read.u64(msg as any, C.MSG.len)),
    timestamp,
    timestampType: tsTypeBuf[0]!,
    get headers() {
      return (headers ??= readHeaders(msg));
    },
    brokerId: -1, // filled lazily via getter below would need more; set on demand cheap enough
    done() {
      if (!freed) {
        freed = true;
        rk.rd_kafka_message_destroy(msg);
      }
    },
  } as KafkaMessage;
}

// ---- produce ----
const VU_MAX = 8;
const vuScratch = new ArrayBuffer(VU_MAX * C.VU_SIZE);
const vuView = new Uint8Array(vuScratch);
const vuDv = new DataView(vuScratch);

function writeVu(index: number, vtype: number, write: (u: number) => void) {
  const base = index * C.VU_SIZE;
  vuDv.setInt32(base, vtype, true);
  write(base + 8);
}

function produceFast(handle: Pointer, topics: Map<string, Pointer>, msg: ProduceInput) {
  const value = toBytes(msg.value);
  const key = toBytes(msg.key);
  const hasHeaders = !!(msg.headers && Object.keys(msg.headers).length);
  const needsRich = hasHeaders || msg.timestamp != null;

  if (!needsRich) {
    let rkt = topics.get(msg.topic);
    if (!rkt) {
      rkt = rk.rd_kafka_topic_new(handle, msg.topic, null) as Pointer;
      if (!rkt) throw new KafkaError(rk.rd_kafka_last_error(), "topic_new failed");
      topics.set(msg.topic, rkt);
    }
    const part = msg.partition ?? C.PARTITION_UA;
    // retry on queue full without MSG_F_BLOCK (avoids hard stalls on the JS thread)
    for (let attempt = 0; ; attempt++) {
      const r = rk.rd_kafka_produce(
        rkt,
        part,
        C.MSG_F_COPY,
        value ? ptr(value) : null,
        BigInt(value?.byteLength ?? 0),
        key ? ptr(key) : null,
        BigInt(key?.byteLength ?? 0),
        null,
      );
      if (r === 0) break;
      if (attempt > 1000) check(rk.rd_kafka_last_error(), "produce");
      rk.rd_kafka_poll(handle, 1);
    }
    return;
  }

  const keep: Uint8Array[] = [];
  if (value) keep.push(value);
  if (key) keep.push(key);
  let headers: Pointer | null = null;
  if (hasHeaders) {
    const names = Object.keys(msg.headers!);
    headers = rk.rd_kafka_headers_new(BigInt(names.length));
    for (const name of names) {
      const b = toBytes(msg.headers![name]);
      check(
        b
          ? rk.rd_kafka_header_add(headers, name, -1n, ptr(b), BigInt(b.byteLength))
          : rk.rd_kafka_header_add(headers, name, -1n, null, 0n),
        `header ${name}`,
      );
      if (b) keep.push(b);
    }
  }

  let n = 2;
  if (msg.partition != null) n++;
  if (value) n++;
  if (key) n++;
  if (msg.timestamp != null) n++;
  if (headers) n++;
  vuView.fill(0);
  let i = 0;
  const topicBytes = enc(msg.topic + "\0");
  keep.push(topicBytes);
  writeVu(i++, C.VTYPE_TOPIC, (u) => vuDv.setBigUint64(u, BigInt(ptr(topicBytes)), true));
  if (msg.partition != null) writeVu(i++, C.VTYPE_PARTITION, (u) => vuDv.setInt32(u, msg.partition!, true));
  if (value) writeVu(i++, C.VTYPE_VALUE, (u) => {
    vuDv.setBigUint64(u, BigInt(ptr(value)), true);
    vuDv.setBigUint64(u + 8, BigInt(value.byteLength), true);
  });
  if (key) writeVu(i++, C.VTYPE_KEY, (u) => {
    vuDv.setBigUint64(u, BigInt(ptr(key)), true);
    vuDv.setBigUint64(u + 8, BigInt(key.byteLength), true);
  });
  writeVu(i++, C.VTYPE_MSGFLAGS, (u) => vuDv.setInt32(u, C.MSG_F_COPY, true));
  if (msg.timestamp != null) writeVu(i++, C.VTYPE_TIMESTAMP, (u) => vuDv.setBigInt64(u, BigInt(msg.timestamp!), true));
  if (headers) writeVu(i++, C.VTYPE_HEADERS, (u) => vuDv.setBigUint64(u, BigInt(headers as any), true));

  const err = rk.rd_kafka_produceva(handle, ptr(vuView), BigInt(n));
  if (err) {
    if (headers) rk.rd_kafka_headers_destroy(headers);
    fromErr(err);
  }
  void keep;
}

function readMetadata(meta: Pointer): ClusterMetadata {
  const brokerCnt = read.i32(meta as any, C.META.broker_cnt);
  const brokersPtr = read.ptr(meta as any, C.META.brokers) as number;
  const topicCnt = read.i32(meta as any, C.META.topic_cnt);
  const topicsPtr = read.ptr(meta as any, C.META.topics) as number;
  const brokers = [];
  for (let i = 0; i < brokerCnt; i++) {
    const b = brokersPtr + i * C.META_BROKER.size;
    const hostPtr = read.ptr(b as any, C.META_BROKER.host);
    brokers.push({
      id: read.i32(b as any, C.META_BROKER.id),
      host: hostPtr ? new CString(hostPtr as any) : "",
      port: read.i32(b as any, C.META_BROKER.port),
    });
  }
  const topics = [];
  for (let i = 0; i < topicCnt; i++) {
    const t = topicsPtr + i * C.META_TOPIC.size;
    const namePtr = read.ptr(t as any, C.META_TOPIC.topic);
    const pcnt = read.i32(t as any, C.META_TOPIC.partition_cnt);
    const partsPtr = read.ptr(t as any, C.META_TOPIC.partitions) as number;
    const partitions = [];
    for (let j = 0; j < pcnt; j++) {
      const p = partsPtr + j * C.META_PART.size;
      partitions.push({
        id: read.i32(p as any, C.META_PART.id),
        err: read.i32(p as any, C.META_PART.err),
        leader: read.i32(p as any, C.META_PART.leader),
      });
    }
    topics.push({
      name: namePtr ? new CString(namePtr as any) : "",
      err: read.i32(t as any, C.META_TOPIC.err),
      partitions,
    });
  }
  return { brokers, topics };
}

class FfiProducer implements NativeProducer {
  #rk: Pointer;
  #closed = false;
  #topics = new Map<string, Pointer>();
  constructor(cfg: KafkaConfig) {
    this.#rk = newRk(C.PRODUCER, {
      "bootstrap.servers": "localhost:9092",
      "allow.auto.create.topics": "true",
      "linger.ms": 5,
      "batch.num.messages": 10000,
      "queue.buffering.max.messages": 100000,
      "socket.nagle.disable": true,
      ...cfg,
    });
  }
  send(msg: ProduceInput) {
    this.#e();
    produceFast(this.#rk, this.#topics, msg);
  }
  poll(t: number) {
    this.#e();
    return rk.rd_kafka_poll(this.#rk, t);
  }
  flush(t: number) {
    this.#e();
    check(rk.rd_kafka_flush(this.#rk, t), "flush");
  }
  outQueueLength() {
    this.#e();
    return rk.rd_kafka_outq_len(this.#rk);
  }
  close() {
    if (this.#closed) return;
    this.#closed = true;
    for (const t of this.#topics.values()) rk.rd_kafka_topic_destroy(t);
    this.#topics.clear();
    rk.rd_kafka_destroy(this.#rk);
  }
  #e() {
    if (this.#closed) throw new Error("Producer is closed");
  }
}

class FfiConsumer implements NativeConsumer {
  #rk: Pointer;
  #closed = false;
  constructor(cfg: KafkaConfig) {
    this.#rk = newRk(C.CONSUMER, {
      "bootstrap.servers": "localhost:9092",
      "group.id": cfg["group.id"] ?? `bun-kafka-${crypto.randomUUID()}`,
      "enable.auto.commit": cfg["enable.auto.commit"] ?? false,
      "auto.offset.reset": cfg["auto.offset.reset"] ?? "earliest",
      "allow.auto.create.topics": "true",
      "fetch.wait.max.ms": cfg["fetch.wait.max.ms"] ?? 50,
      "fetch.min.bytes": cfg["fetch.min.bytes"] ?? 1,
      ...cfg,
    });
    check(rk.rd_kafka_poll_set_consumer(this.#rk), "poll_set_consumer");
  }
  subscribe(topics: string[]) {
    this.#e();
    const list = makeList(topics);
    try { check(rk.rd_kafka_subscribe(this.#rk, list), "subscribe"); }
    finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  unsubscribe() { this.#e(); check(rk.rd_kafka_unsubscribe(this.#rk), "unsubscribe"); }
  subscription() {
    this.#e();
    const slot = new BigUint64Array(1);
    check(rk.rd_kafka_subscription(this.#rk, slot as any), "subscription");
    const list = Number(slot[0]!) as any;
    try { return readList(list); } finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  assign(partitions: TopicPartition[]) {
    this.#e();
    if (!partitions.length) { check(rk.rd_kafka_assign(this.#rk, null), "assign"); return; }
    const list = makeList(partitions);
    try { check(rk.rd_kafka_assign(this.#rk, list), "assign"); }
    finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  assignment() {
    this.#e();
    const slot = new BigUint64Array(1);
    check(rk.rd_kafka_assignment(this.#rk, slot as any), "assignment");
    const list = Number(slot[0]!) as any;
    try { return readList(list); } finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  assignmentLost() { return !!rk.rd_kafka_assignment_lost(this.#rk); }
  rebalanceProtocol() { return rk.rd_kafka_rebalance_protocol(this.#rk) ?? "NONE"; }

  #take(raw: Pointer | null): KafkaMessage | null {
    if (!raw) return null;
    const err = read.i32(raw as any, C.MSG.err);
    if (err === C.TIMED_OUT || err === C.PARTITION_EOF) {
      rk.rd_kafka_message_destroy(raw);
      return null;
    }
    if (err !== C.NO_ERROR) {
      const estr = rk.rd_kafka_message_errstr(raw) || rk.rd_kafka_err2str(err);
      rk.rd_kafka_message_destroy(raw);
      // transient while topic auto-creates / metadata catches up
      if (
        err === 3 /* UNKNOWN_TOPIC_OR_PART */ ||
        err === -190 /* UNKNOWN_PARTITION */ ||
        err === -188 /* UNKNOWN_TOPIC */
      ) {
        return null;
      }
      throw new KafkaError(err, estr);
    }
    return readMsg(raw);
  }

  poll(timeoutMs: number): KafkaMessage | null {
    this.#e();
    return this.#take(rk.rd_kafka_consumer_poll(this.#rk, timeoutMs) as Pointer | null);
  }

  pollBatch(timeoutMs: number, max: number): KafkaMessage[] {
    this.#e();
    const out: KafkaMessage[] = [];
    // first wait
    let m = this.#take(rk.rd_kafka_consumer_poll(this.#rk, timeoutMs) as Pointer | null);
    if (!m) return out;
    out.push(m);
    // drain ready queue without blocking
    while (out.length < max) {
      m = this.#take(rk.rd_kafka_consumer_poll(this.#rk, 0) as Pointer | null);
      if (!m) break;
      out.push(m);
    }
    return out;
  }

  commit(offsets: TopicPartition[] | null, async: boolean) {
    this.#e();
    if (!offsets) { check(rk.rd_kafka_commit(this.#rk, null, async ? 1 : 0), "commit"); return; }
    const list = makeList(offsets);
    try { check(rk.rd_kafka_commit(this.#rk, list, async ? 1 : 0), "commit"); }
    finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  committed(partitions: TopicPartition[], timeoutMs: number) {
    this.#e();
    const list = makeList(partitions);
    try { check(rk.rd_kafka_committed(this.#rk, list, timeoutMs), "committed"); return readList(list); }
    finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  position(partitions: TopicPartition[]) {
    this.#e();
    const list = makeList(partitions);
    try { check(rk.rd_kafka_position(this.#rk, list), "position"); return readList(list); }
    finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  seek(partitions: TopicPartition[], timeoutMs: number) {
    this.#e();
    const list = makeList(partitions);
    try { const e = rk.rd_kafka_seek_partitions(this.#rk, list, timeoutMs); if (e) fromErr(e); }
    finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  pause(partitions: TopicPartition[]) {
    this.#e();
    const list = makeList(partitions);
    try { check(rk.rd_kafka_pause_partitions(this.#rk, list), "pause"); }
    finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  resume(partitions: TopicPartition[]) {
    this.#e();
    const list = makeList(partitions);
    try { check(rk.rd_kafka_resume_partitions(this.#rk, list), "resume"); }
    finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  storeOffsets(offsets: TopicPartition[]) {
    this.#e();
    const list = makeList(offsets);
    try { check(rk.rd_kafka_offsets_store(this.#rk, list), "offsets_store"); }
    finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  queryWatermarkOffsets(topic: string, partition: number, timeoutMs: number): Watermarks {
    this.#e();
    const low = new BigInt64Array(1), high = new BigInt64Array(1);
    check(rk.rd_kafka_query_watermark_offsets(this.#rk, topic, partition, low as any, high as any, timeoutMs), "query_watermark_offsets");
    return { low: low[0]!, high: high[0]! };
  }
  getWatermarkOffsets(topic: string, partition: number): Watermarks {
    this.#e();
    const low = new BigInt64Array(1), high = new BigInt64Array(1);
    check(rk.rd_kafka_get_watermark_offsets(this.#rk, topic, partition, low as any, high as any), "get_watermark_offsets");
    return { low: low[0]!, high: high[0]! };
  }
  offsetsForTimes(queries: TopicPartition[], timeoutMs: number) {
    this.#e();
    const list = makeList(queries);
    try { check(rk.rd_kafka_offsets_for_times(this.#rk, list, timeoutMs), "offsets_for_times"); return readList(list); }
    finally { rk.rd_kafka_topic_partition_list_destroy(list); }
  }
  memberId() {
    this.#e();
    const p = rk.rd_kafka_memberid(this.#rk);
    if (!p) return null;
    const s = new CString(p as any);
    rk.rd_kafka_mem_free(this.#rk, p as any);
    return s;
  }
  close() {
    if (this.#closed) return;
    this.#closed = true;
    rk.rd_kafka_destroy(this.#rk);
  }
  #e() { if (this.#closed) throw new Error("Consumer is closed"); }
}

class FfiAdmin implements NativeAdmin {
  #rk: Pointer;
  #closed = false;
  constructor(cfg: KafkaConfig) {
    this.#rk = newRk(C.PRODUCER, { "bootstrap.servers": "localhost:9092", ...cfg });
  }
  metadata(allTopics: boolean, timeoutMs: number) {
    this.#e();
    const slot = new BigUint64Array(1);
    check(rk.rd_kafka_metadata(this.#rk, allTopics ? 1 : 0, null, slot as any, timeoutMs), "metadata");
    const meta = Number(slot[0]!) as any;
    try { return readMetadata(meta); } finally { rk.rd_kafka_metadata_destroy(meta); }
  }
  clusterId(timeoutMs: number) {
    this.#e();
    const p = rk.rd_kafka_clusterid(this.#rk, timeoutMs);
    if (!p) return null;
    const s = new CString(p as any);
    rk.rd_kafka_mem_free(this.#rk, p as any);
    return s;
  }
  close() { if (this.#closed) return; this.#closed = true; rk.rd_kafka_destroy(this.#rk); }
  #e() { if (this.#closed) throw new Error("Admin is closed"); }
}

export const ffiDriver: NativeDriver = {
  kind: "ffi",
  version: () => ({ number: rk.rd_kafka_version(), string: rk.rd_kafka_version_str() ?? "" }),
  err2str: (c) => rk.rd_kafka_err2str(c) ?? `err ${c}`,
  producer: (c) => new FfiProducer(c),
  consumer: (c) => new FfiConsumer(c),
  admin: (c) => new FfiAdmin(c),
};
