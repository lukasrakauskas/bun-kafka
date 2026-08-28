/* oxlint-disable anti-slop/no-known-value-widening */
import { KafkaError } from "../errors.ts";
import { RequestBody, ResponseBody } from "./body.ts";
import { decodeResponse, encodeRequest, decodeBytes, type KafkaDecoder } from "./codec.ts";
import type { RecordCompression } from "./compression.ts";
import { RecordSetDecoder, type RecordDecoderOptions } from "./wire/records/decoder.ts";
import { encodeRecordBatch, type WireRecord } from "./wire/records/encoder.ts";

export function createRecordSetDecoder(
  bytes: Uint8Array,
  topic: string,
  partition: number,
  brokerId: number,
  options: RecordDecoderOptions = {},
) {
  return new RecordSetDecoder(bytes, topic, partition, brokerId, options);
}

export function readApiVersionsResponse(
  body: ResponseBody,
): Map<number, { min: number; max: number }> {
  const reader = decodeResponse(body);
  const error = reader.i16();
  if (error) {
    throw new KafkaError(error, "ApiVersions negotiation failed");
  }
  return new Map(
    reader.array((item) => [item.i16(), { min: item.i16(), max: item.i16() }] as const),
  );
}
export function readSaslHandshakeResponse(body: ResponseBody): {
  error: number;
  mechanisms: string[];
} {
  const reader = decodeResponse(body);
  return { error: reader.i16(), mechanisms: reader.array((item) => item.string() ?? "") };
}
export function readSaslAuthenticateResponse(body: ResponseBody): {
  error: number;
  message: string | null;
  authBytes: Uint8Array;
  sessionLifetimeMs: bigint;
} {
  const reader = decodeResponse(body);
  return {
    error: reader.i16(),
    message: reader.string(),
    authBytes: reader.bytes() ?? new Uint8Array(),
    sessionLifetimeMs: reader.i64(),
  };
}
export function readFindCoordinatorResponse(body: ResponseBody): {
  throttleMs: number;
  error: number;
  message: string | null;
  coordinatorId: number;
  host: string | null;
  port: number;
} {
  const reader = decodeResponse(body);
  return {
    throttleMs: reader.i32(),
    error: reader.i16(),
    message: reader.string(),
    coordinatorId: reader.i32(),
    host: reader.string(),
    port: reader.i32(),
  };
}
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

function readConsumerGroupMemberFromDecoder(reader: KafkaDecoder): {
  memberId: string;
  topics: string[];
  owned: Array<{ topic: string; partition: number }>;
} {
  const memberId = reader.string() ?? "";
  const metadata = decodeBytes(reader.bytes() ?? new Uint8Array());
  const version = metadata.i16();
  const topics = metadata.array((item) => item.string() ?? "");
  const owned =
    version < 1
      ? []
      : metadata
          .array((ownedReader) => {
            const topic = ownedReader.string() ?? "";
            return ownedReader
              .array((item) => item.i32())
              .map((partition) => ({ topic, partition }));
          })
          .flat();
  metadata.bytes();
  return { memberId, topics, owned };
}
export function writeConsumerSubscription(
  topics: readonly string[],
  owned?: readonly { topic: string; partition: number }[],
): Uint8Array {
  return encodeRequest((writer) => {
    writer.i16(owned ? 1 : 0).array(topics, (topicWriter, topic) => topicWriter.string(topic));
    if (owned) {
      const byTopic = Map.groupBy(owned, (item) => item.topic);
      writer.array([...byTopic], (topicWriter, [topic, values]) =>
        topicWriter
          .string(topic)
          .array(values, (partitionWriter, item) => partitionWriter.i32(item.partition)),
      );
    }
    writer.bytes(null);
  }).toBytes();
}
export function writeConsumerJoinRequest(
  groupId: string,
  sessionTimeoutMs: number,
  rebalanceTimeoutMs: number,
  memberId: string,
  instanceId: string | undefined,
  protocolName: string,
  memberMetadata: Uint8Array,
): RequestBody {
  return encodeRequest((writer) => {
    writer.string(groupId).i32(sessionTimeoutMs).i32(rebalanceTimeoutMs).string(memberId);
    if (instanceId !== undefined) {
      writer.string(instanceId);
    }
    writer
      .string("consumer")
      .array([[protocolName, memberMetadata] as const], (itemWriter, [name, metadata]) =>
        itemWriter.string(name).bytes(metadata),
      );
  });
}
export function readConsumerJoinResponse(body: ResponseBody): {
  throttleMs: number;
  error: number;
  generationId: number;
  leader: string;
  memberId: string;
  members: Array<{
    memberId: string;
    topics: string[];
    owned: Array<{ topic: string; partition: number }>;
  }>;
} {
  const reader = decodeResponse(body);
  const throttleMs = reader.i32();
  const error = reader.i16();
  const generationId = reader.i32();
  reader.string();
  const leader = reader.string() ?? "";
  const memberId = reader.string() ?? "";
  return {
    throttleMs,
    error,
    generationId,
    leader,
    memberId,
    members: reader.array(readConsumerGroupMemberFromDecoder),
  };
}
export function writeConsumerSyncRequest(
  groupId: string,
  generationId: number,
  memberId: string,
  instanceId: string | undefined,
  assignments: ReadonlyMap<string, readonly { topic: string; partitions: readonly number[] }[]>,
): RequestBody {
  return encodeRequest((writer) => {
    writer.string(groupId).i32(generationId).string(memberId);
    if (instanceId !== undefined) {
      writer.string(instanceId);
    }
    writer.array([...assignments], (itemWriter, [assignmentMemberId, values]) => {
      const assignment = encodeRequest((assignmentWriter) =>
        assignmentWriter
          .i16(0)
          .array(values, (valueWriter, item) =>
            valueWriter
              .string(item.topic)
              .array(item.partitions, (partitionWriter, partition) =>
                partitionWriter.i32(partition),
              ),
          )
          .bytes(null),
      );
      itemWriter.string(assignmentMemberId).bytes(assignment.toBytes());
    });
  });
}
export function readConsumerSyncResponse(
  body: ResponseBody,
  staticMembership: boolean,
): { error: number; assignment: Array<{ topic: string; partition: number }> } {
  const reader = decodeResponse(body);
  if (staticMembership) {
    reader.i32();
  }
  const error = reader.i16();
  if (error) {
    return { error, assignment: [] };
  }
  const nested = decodeBytes(reader.bytes() ?? new Uint8Array());
  nested.i16();
  return {
    error,
    assignment: nested
      .array((item) => ({
        topic: item.string() ?? "",
        partitions: item.array((partition) => partition.i32()),
      }))
      .flatMap((item) => item.partitions.map((partition) => ({ topic: item.topic, partition }))),
  };
}
export function writeConsumerGroupRequest(
  groupId: string,
  generationId: number,
  memberId: string,
  offsets: ReadonlyMap<string, readonly { partition: number; offset: bigint }[]>,
): RequestBody {
  return encodeRequest((writer) => {
    writer.string(groupId).i32(generationId).string(memberId).i64(-1n);
    writer.array([...offsets], (topicWriter, [topic, values]) =>
      topicWriter
        .string(topic)
        .array(values, (partitionWriter, value) =>
          partitionWriter.i32(value.partition).i64(value.offset).string(null),
        ),
    );
  });
}
export function writeConsumerOffsetFetchRequest(
  groupId: string,
  topics: ReadonlyMap<string, readonly { partition: number }[]>,
): RequestBody {
  return encodeRequest((writer) =>
    writer
      .string(groupId)
      .array([...topics], (topicWriter, [topic, values]) =>
        topicWriter
          .string(topic)
          .array(values, (partitionWriter, value) => partitionWriter.i32(value.partition)),
      ),
  );
}
export function readConsumerOffsetFetchResponse(body: ResponseBody): {
  topics: Array<{
    topic: string;
    partitions: Array<{
      partition: number;
      offset: bigint;
      metadata: string | null;
      error: number;
    }>;
  }>;
  error: number;
} {
  const reader = decodeResponse(body);
  const topics = reader.array((topicReader) => ({
    topic: topicReader.string() ?? "",
    partitions: topicReader.array((partitionReader) => ({
      partition: partitionReader.i32(),
      offset: partitionReader.i64(),
      metadata: partitionReader.string(),
      error: partitionReader.i16(),
    })),
  }));
  return { topics, error: reader.i16() };
}
export function writeListOffsetsRequest(
  topics: ReadonlyMap<string, readonly { partition: number; timestamp: bigint }[]>,
): RequestBody {
  return encodeRequest((writer) =>
    writer
      .i32(-1)
      .array([...topics], (topicWriter, [topic, values]) =>
        topicWriter
          .string(topic)
          .array(values, (partitionWriter, value) =>
            partitionWriter.i32(value.partition).i64(value.timestamp),
          ),
      ),
  );
}
export function readListOffsetsResponse(
  body: ResponseBody,
): Array<{ topic: string; partition: number; error: number; offset: bigint }> {
  const reader = decodeResponse(body);
  return reader
    .array((topicReader) => {
      const topic = topicReader.string() ?? "";
      return topicReader.array((partitionReader) => {
        const partition = partitionReader.i32();
        const error = partitionReader.i16();
        partitionReader.i64();
        const offset = partitionReader.i64();
        return { topic, partition, error, offset };
      });
    })
    .flat();
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
export function writeConsumerHeartbeatRequest(
  groupId: string,
  generationId: number,
  memberId: string,
  instanceId: string | undefined,
): RequestBody {
  return encodeRequest((writer) => {
    writer.string(groupId).i32(generationId).string(memberId);
    if (instanceId !== undefined) {
      writer.string(instanceId);
    }
  });
}
export function readConsumerHeartbeatResponse(
  body: ResponseBody,
  staticMembership: boolean,
): number {
  const reader = decodeResponse(body);
  if (staticMembership) {
    reader.i32();
  }
  return reader.i16();
}
export function readGroupCoordinatorResponse(body: ResponseBody): {
  error: number;
  coordinatorId: number;
  host: string | null;
  port: number;
} {
  const reader = decodeResponse(body);
  return {
    error: reader.i16(),
    coordinatorId: reader.i32(),
    host: reader.string(),
    port: reader.i32(),
  };
}

export function writeLeaveGroupRequest(
  groupId: string,
  memberId: string,
  instanceId?: string,
): RequestBody {
  return instanceId === undefined
    ? encodeRequest((writer) => writer.string(groupId).string(memberId))
    : encodeRequest((writer) =>
        writer
          .string(groupId)
          .array([{ memberId, instanceId }], (itemWriter, item) =>
            itemWriter.string(item.memberId).string(item.instanceId),
          ),
      );
}

export function readConsumerOffsetCommitResponse(body: ResponseBody) {
  return decodeResponse(body).array((topicReader) => ({
    topic: topicReader.string() ?? "",
    partitions: topicReader.array((partitionReader) => ({
      partition: partitionReader.i32(),
      error: partitionReader.i16(),
    })),
  }));
}
