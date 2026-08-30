/* oxlint-disable anti-slop/no-known-value-widening */
import { RequestBody, ResponseBody } from "../body.ts";
import { decodeBytes, decodeResponse, encodeRequest, type KafkaDecoder } from "../codec.ts";

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
