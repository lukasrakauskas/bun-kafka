import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest } from "../codec.ts";

export interface TopicPartition {
  topic: string;
  partition: number;
}

export interface PartitionReassignment extends TopicPartition {
  replicas: readonly number[] | null;
}

const byTopic = <T extends TopicPartition>(items: readonly T[]) =>
  Map.groupBy(items, (item) => item.topic);

export function writeAlterPartitionReassignmentsRequest(
  reassignments: readonly PartitionReassignment[],
  timeoutMs: number,
): RequestBody {
  return encodeRequest((writer) =>
    writer
      .i32(timeoutMs)
      .compactArray([...byTopic(reassignments)], (topicWriter, [topic, partitions]) =>
        topicWriter
          .compactString(topic)
          .compactArray(partitions, (partitionWriter, item) =>
            partitionWriter
              .i32(item.partition)
              .compactArray(item.replicas, (replicaWriter, replica) => replicaWriter.i32(replica))
              .tags(),
          )
          .tags(),
      )
      .tags(),
  );
}

export function readAlterPartitionReassignmentsResponse(body: ResponseBody) {
  const reader = decodeResponse(body);
  const throttleMs = reader.i32();
  const error = reader.i16();
  const message = reader.compactString();
  const results = reader.compactArray((topicReader) => {
    const topic = topicReader.compactString() ?? "";
    const partitions = topicReader.compactArray((partitionReader) => {
      const result = {
        topic,
        partition: partitionReader.i32(),
        error: partitionReader.i16(),
        message: partitionReader.compactString(),
      };
      partitionReader.skipTags();
      return result;
    });
    topicReader.skipTags();
    return partitions;
  });
  reader.skipTags();
  return { throttleMs, error, message, results: results.flat() };
}

export function writeListPartitionReassignmentsRequest(
  partitions: readonly TopicPartition[] | null,
  timeoutMs: number,
): RequestBody {
  return encodeRequest((writer) =>
    writer
      .i32(timeoutMs)
      .compactArray(
        partitions === null ? null : [...byTopic(partitions)],
        (topicWriter, [topic, values]) =>
          topicWriter
            .compactString(topic)
            .compactArray(values, (partitionWriter, item) => partitionWriter.i32(item.partition))
            .tags(),
      )
      .tags(),
  );
}

export function readListPartitionReassignmentsResponse(body: ResponseBody) {
  const reader = decodeResponse(body);
  const throttleMs = reader.i32();
  const error = reader.i16();
  const message = reader.compactString();
  const reassignments = reader.compactArray((topicReader) => {
    const topic = topicReader.compactString() ?? "";
    const partitions = topicReader.compactArray((partitionReader) => {
      const result = {
        topic,
        partition: partitionReader.i32(),
        replicas: partitionReader.compactArray((replica) => replica.i32()),
        addingReplicas: partitionReader.compactArray((replica) => replica.i32()),
        removingReplicas: partitionReader.compactArray((replica) => replica.i32()),
      };
      partitionReader.skipTags();
      return result;
    });
    topicReader.skipTags();
    return partitions;
  });
  reader.skipTags();
  return { throttleMs, error, message, reassignments: reassignments.flat() };
}

export function writeElectLeadersRequest(
  electionType: "preferred" | "unclean",
  partitions: readonly TopicPartition[] | null,
  timeoutMs: number,
): RequestBody {
  return encodeRequest((writer) =>
    writer
      .i8(electionType === "preferred" ? 0 : 1)
      .array(
        partitions === null ? null : [...byTopic(partitions)],
        (topicWriter, [topic, values]) =>
          topicWriter
            .string(topic)
            .array(values, (partitionWriter, item) => partitionWriter.i32(item.partition)),
      )
      .i32(timeoutMs),
  );
}

export function readElectLeadersResponse(body: ResponseBody) {
  const reader = decodeResponse(body);
  const throttleMs = reader.i32();
  const error = reader.i16();
  const results = reader.array((topicReader) => {
    const topic = topicReader.string() ?? "";
    return topicReader.array((partitionReader) => ({
      topic,
      partition: partitionReader.i32(),
      error: partitionReader.i16(),
      message: partitionReader.string(),
    }));
  });
  return { throttleMs, error, results: results.flat() };
}
