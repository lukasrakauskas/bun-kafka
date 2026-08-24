import { dlopen, FFIType, ptr, read, toArrayBuffer, CString, type Pointer } from "bun:ffi";
import { existsSync } from "node:fs";

const { i32, i64, u64, ptr: p, cstring, void: v } = FFIType;

function findLib(): string {
  const env = process.env.LIBRDKAFKA_PATH;
  if (env && existsSync(env)) return env;
  for (const c of [
    "/opt/homebrew/lib/librdkafka.dylib",
    "/usr/local/lib/librdkafka.dylib",
    "/usr/lib/x86_64-linux-gnu/librdkafka.so.1",
    "/usr/lib/aarch64-linux-gnu/librdkafka.so.1",
    "/usr/lib64/librdkafka.so.1",
    "librdkafka.dylib",
    "librdkafka.so.1",
    "librdkafka",
  ]) {
    if (c.includes("/") && !existsSync(c)) continue;
    return c;
  }
  return "librdkafka";
}

export const lib = dlopen(findLib(), {
  rd_kafka_version: { args: [], returns: i32 },
  rd_kafka_version_str: { args: [], returns: cstring },
  rd_kafka_err2str: { args: [i32], returns: cstring },
  rd_kafka_last_error: { args: [], returns: i32 },
  rd_kafka_conf_new: { args: [], returns: p },
  rd_kafka_conf_destroy: { args: [p], returns: v },
  rd_kafka_conf_set: { args: [p, cstring, cstring, p, u64], returns: i32 },
  rd_kafka_new: { args: [i32, p, p, u64], returns: p },
  rd_kafka_destroy: { args: [p], returns: v },
  rd_kafka_destroy_flags: { args: [p, i32], returns: v },
  rd_kafka_name: { args: [p], returns: cstring },
  rd_kafka_memberid: { args: [p], returns: p },
  rd_kafka_clusterid: { args: [p, i32], returns: p },
  rd_kafka_mem_free: { args: [p, p], returns: v },
  rd_kafka_outq_len: { args: [p], returns: i32 },
  rd_kafka_poll: { args: [p, i32], returns: i32 },
  rd_kafka_flush: { args: [p, i32], returns: i32 },
  rd_kafka_topic_new: { args: [p, cstring, p], returns: p },
  rd_kafka_topic_destroy: { args: [p], returns: v },
  rd_kafka_topic_name: { args: [p], returns: cstring },
  rd_kafka_topic_partition_list_new: { args: [i32], returns: p },
  rd_kafka_topic_partition_list_destroy: { args: [p], returns: v },
  rd_kafka_topic_partition_list_add: { args: [p, cstring, i32], returns: p },
  rd_kafka_topic_partition_list_set_offset: { args: [p, cstring, i32, i64], returns: i32 },
  rd_kafka_subscribe: { args: [p, p], returns: i32 },
  rd_kafka_unsubscribe: { args: [p], returns: i32 },
  rd_kafka_subscription: { args: [p, p], returns: i32 },
  rd_kafka_assign: { args: [p, p], returns: i32 },
  rd_kafka_assignment: { args: [p, p], returns: i32 },
  rd_kafka_assignment_lost: { args: [p], returns: i32 },
  rd_kafka_rebalance_protocol: { args: [p], returns: cstring },
  rd_kafka_poll_set_consumer: { args: [p], returns: i32 },
  rd_kafka_consumer_poll: { args: [p, i32], returns: p },
  rd_kafka_consumer_close: { args: [p], returns: i32 },
  rd_kafka_commit: { args: [p, p, i32], returns: i32 },
  rd_kafka_committed: { args: [p, p, i32], returns: i32 },
  rd_kafka_position: { args: [p, p], returns: i32 },
  rd_kafka_seek_partitions: { args: [p, p, i32], returns: p },
  rd_kafka_pause_partitions: { args: [p, p], returns: i32 },
  rd_kafka_resume_partitions: { args: [p, p], returns: i32 },
  rd_kafka_offsets_store: { args: [p, p], returns: i32 },
  rd_kafka_query_watermark_offsets: { args: [p, cstring, i32, p, p, i32], returns: i32 },
  rd_kafka_get_watermark_offsets: { args: [p, cstring, i32, p, p], returns: i32 },
  rd_kafka_offsets_for_times: { args: [p, p, i32], returns: i32 },
  rd_kafka_message_destroy: { args: [p], returns: v },
  rd_kafka_message_errstr: { args: [p], returns: cstring },
  rd_kafka_message_timestamp: { args: [p, p], returns: i64 },
  rd_kafka_message_broker_id: { args: [p], returns: i32 },
  rd_kafka_message_headers: { args: [p, p], returns: i32 },
  rd_kafka_headers_new: { args: [u64], returns: p },
  rd_kafka_headers_destroy: { args: [p], returns: v },
  rd_kafka_header_add: { args: [p, cstring, i64, p, i64], returns: i32 },
  rd_kafka_header_get_all: { args: [p, u64, p, p, p], returns: i32 },
  rd_kafka_produce: { args: [p, i32, i32, p, u64, p, u64, p], returns: i32 },
  rd_kafka_produceva: { args: [p, p, u64], returns: p },
  rd_kafka_metadata: { args: [p, i32, p, p, i32], returns: i32 },
  rd_kafka_metadata_destroy: { args: [p], returns: v },
  rd_kafka_error_code: { args: [p], returns: i32 },
  rd_kafka_error_string: { args: [p], returns: cstring },
  rd_kafka_error_is_fatal: { args: [p], returns: i32 },
  rd_kafka_error_is_retriable: { args: [p], returns: i32 },
  rd_kafka_error_destroy: { args: [p], returns: v },
});

export const rk = lib.symbols;
export { ptr, read, toArrayBuffer, CString, type Pointer };

export const C = {
  PRODUCER: 0,
  CONSUMER: 1,
  CONF_OK: 0,
  MSG_F_COPY: 0x2,
  MSG_F_BLOCK: 0x4,
  PARTITION_UA: -1,
  VTYPE_TOPIC: 1,
  VTYPE_PARTITION: 3,
  VTYPE_VALUE: 4,
  VTYPE_KEY: 5,
  VTYPE_MSGFLAGS: 7,
  VTYPE_TIMESTAMP: 8,
  VTYPE_HEADERS: 10,
  VU_SIZE: 72,
  NO_ERROR: 0,
  TIMED_OUT: -185,
  PARTITION_EOF: -191,
  MSG: { err: 0, rkt: 8, partition: 16, payload: 24, len: 32, key: 40, key_len: 48, offset: 56 },
  TPL: { cnt: 0, elems: 8 },
  TP: { size: 64, topic: 0, partition: 8, offset: 16, err: 48 },
  META: { broker_cnt: 0, brokers: 8, topic_cnt: 16, topics: 24 },
  META_BROKER: { size: 24, id: 0, host: 8, port: 16 },
  META_TOPIC: { size: 32, topic: 0, partition_cnt: 8, partitions: 16, err: 24 },
  META_PART: { size: 40, id: 0, err: 4, leader: 8 },
} as const;
