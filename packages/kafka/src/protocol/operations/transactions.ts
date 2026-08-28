/* oxlint-disable anti-slop/no-known-value-widening */
import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest } from "../codec.ts";

export function writeEndTransactionRequest(
  transactionalId: string,
  producerId: bigint,
  producerEpoch: number,
  committed: boolean,
): RequestBody {
  return encodeRequest((writer) =>
    writer.string(transactionalId).i64(producerId).i16(producerEpoch).bool(committed),
  );
}
export function readEndTransactionResponse(body: ResponseBody): {
  throttleMs: number;
  error: number;
} {
  const reader = decodeResponse(body);
  return { throttleMs: reader.i32(), error: reader.i16() };
}
export function writeAddOffsetsToTransactionRequest(
  transactionalId: string,
  producerId: bigint,
  producerEpoch: number,
  groupId: string,
): RequestBody {
  return encodeRequest((writer) =>
    writer.string(transactionalId).i64(producerId).i16(producerEpoch).string(groupId),
  );
}
export function writeTransactionOffsetCommitRequest(
  transactionalId: string,
  groupId: string,
  producerId: bigint,
  producerEpoch: number,
  topics: ReadonlyMap<string, readonly { partition: number; offset: bigint }[]>,
): RequestBody {
  return encodeRequest((writer) => {
    writer.string(transactionalId).string(groupId).i64(producerId).i16(producerEpoch);
    writer.array([...topics], (topicWriter, [topic, values]) =>
      topicWriter
        .string(topic)
        .array(values, (partitionWriter, value) =>
          partitionWriter.i32(value.partition).i64(value.offset).string(null),
        ),
    );
  });
}
export function readTransactionOffsetCommitResponse(body: ResponseBody): {
  throttleMs: number;
  topics: Array<{ topic: string; partitions: Array<{ index: number; error: number }> }>;
} {
  const reader = decodeResponse(body);
  return {
    throttleMs: reader.i32(),
    topics: reader.array((topicReader) => ({
      topic: topicReader.string() ?? "",
      partitions: topicReader.array((partitionReader) => ({
        index: partitionReader.i32(),
        error: partitionReader.i16(),
      })),
    })),
  };
}
export function writeAddPartitionsToTransactionRequest(
  transactionalId: string,
  producerId: bigint,
  producerEpoch: number,
  topics: ReadonlyMap<string, readonly { partition: number }[]>,
): RequestBody {
  return encodeRequest((writer) => {
    writer.string(transactionalId).i64(producerId).i16(producerEpoch);
    writer.array([...topics], (topicWriter, [topic, values]) =>
      topicWriter
        .string(topic)
        .array(values, (partitionWriter, value) => partitionWriter.i32(value.partition)),
    );
  });
}
export function readTopicPartitionErrors(body: ResponseBody): {
  throttleMs: number;
  topics: Array<{ name: string; partitions: Array<{ index: number; error: number }> }>;
} {
  const reader = decodeResponse(body);
  return {
    throttleMs: reader.i32(),
    topics: reader.array((topicReader) => ({
      name: topicReader.string() ?? "",
      partitions: topicReader.array((partitionReader) => ({
        index: partitionReader.i32(),
        error: partitionReader.i16(),
      })),
    })),
  };
}
export function writeInitProducerIdRequest(
  transactionalId: string | null,
  transactionTimeoutMs: number,
): RequestBody {
  return encodeRequest((writer) => writer.string(transactionalId).i32(transactionTimeoutMs));
}
export function readInitProducerIdResponse(body: ResponseBody): {
  throttleMs: number;
  error: number;
  producerId: bigint;
  producerEpoch: number;
} {
  const reader = decodeResponse(body);
  const throttleMs = reader.i32();
  const error = reader.i16();
  return { throttleMs, error, producerId: reader.i64(), producerEpoch: reader.i16() };
}
