/* oxlint-disable anti-slop/no-known-value-widening */
import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest } from "../codec.ts";
import type { RecordCompression } from "../compression.ts";
import { encodeRecordBatch, type WireRecord } from "../wire/records/encoder.ts";

export type ProduceRequestPartition = {
  topic: string;
  partition: number;
  records: readonly WireRecord[];
  producer?: {
    id: bigint;
    epoch: number;
    sequence: number;
    control?: boolean;
    transactional?: boolean;
  };
};
export function writeProduceRequest(
  transactionalId: string | null,
  acks: number,
  timeoutMs: number,
  partitions: readonly ProduceRequestPartition[],
  compression: RecordCompression,
): RequestBody {
  const topics = Map.groupBy(partitions, (partition) => partition.topic);
  return encodeRequest((writer) => {
    writer.string(transactionalId).i16(acks).i32(timeoutMs);
    writer.array([...topics], (topicWriter, [topic, values]) =>
      topicWriter
        .string(topic)
        .array(values, (partitionWriter, value) =>
          partitionWriter
            .i32(value.partition)
            .bytes(encodeRecordBatch(value.records, Date.now(), compression, value.producer)),
        ),
    );
  });
}
export function readProduceResponse(body: ResponseBody): {
  throttleMs: number;
  results: Array<{
    topic: string;
    partition: number;
    error: number;
    baseOffset: bigint;
    logAppendTime: bigint;
  }>;
} {
  const reader = decodeResponse(body);
  const results = reader
    .array((topicReader) => {
      const topic = topicReader.string() ?? "";
      return topicReader.array((partitionReader) => ({
        topic,
        partition: partitionReader.i32(),
        error: partitionReader.i16(),
        baseOffset: partitionReader.i64(),
        logAppendTime: partitionReader.i64(),
      }));
    })
    .flat();
  return { throttleMs: reader.i32(), results };
}
