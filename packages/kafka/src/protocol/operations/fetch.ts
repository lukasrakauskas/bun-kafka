/* oxlint-disable anti-slop/no-known-value-widening */
import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest } from "../codec.ts";
import { RecordSetDecoder, type RecordDecoderOptions } from "../wire/records/decoder.ts";

export function createRecordSetDecoder(
  bytes: Uint8Array,
  topic: string,
  partition: number,
  brokerId: number,
  options: RecordDecoderOptions = {},
) {
  return new RecordSetDecoder(bytes, topic, partition, brokerId, options);
}

export type FetchRequestPartition = {
  topic: string;
  partition: number;
  offset: bigint;
  maxPartitionBytes: number;
};
export function writeFetchRequest(
  maxWaitMs: number,
  minBytes: number,
  maxBytes: number,
  isolationLevel: number,
  sessionId: number,
  sessionEpoch: number,
  requested: readonly FetchRequestPartition[],
  forgotten: ReadonlyMap<string, readonly number[]>,
): RequestBody {
  const byTopic = Map.groupBy(requested, (item) => item.topic);
  return encodeRequest((writer) => {
    writer
      .i32(-1)
      .i32(maxWaitMs)
      .i32(minBytes)
      .i32(maxBytes)
      .i8(isolationLevel)
      .i32(sessionId)
      .i32(sessionEpoch);
    writer.array([...byTopic], (topicWriter, [topic, values]) =>
      topicWriter
        .string(topic)
        .array(values, (partitionWriter, value) =>
          partitionWriter
            .i32(value.partition)
            .i64(value.offset)
            .i64(-1n)
            .i32(value.maxPartitionBytes),
        ),
    );
    writer.array([...forgotten], (topicWriter, [topic, partitions]) =>
      topicWriter
        .string(topic)
        .array(partitions, (partitionWriter, partition) => partitionWriter.i32(partition)),
    );
  });
}
export type FetchResponsePartition = {
  topic: string;
  partition: number;
  error: number;
  abortedTransactions: Array<{ producerId: bigint; firstOffset: bigint }>;
  records: Uint8Array | null;
};
export function readFetchResponse(body: ResponseBody): {
  throttleMs: number;
  topError: number;
  sessionId: number;
  partitions: FetchResponsePartition[];
} {
  const reader = decodeResponse(body);
  const throttleMs = reader.i32();
  const topError = reader.i16();
  const sessionId = reader.i32();
  const partitions = reader
    .array((topicReader) => {
      const topic = topicReader.string() ?? "";
      return topicReader.array((partitionReader) => {
        const partition = partitionReader.i32();
        const error = partitionReader.i16();
        partitionReader.i64();
        partitionReader.i64();
        partitionReader.i64();
        const abortedTransactions = partitionReader.array((item) => ({
          producerId: item.i64(),
          firstOffset: item.i64(),
        }));
        const records = partitionReader.bytes();
        return { topic, partition, error, abortedTransactions, records };
      });
    })
    .flat();
  return { throttleMs, topError, sessionId, partitions };
}
