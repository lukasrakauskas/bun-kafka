/* oxlint-disable anti-slop/no-known-value-widening */
import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest } from "../codec.ts";

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

export function readConsumerOffsetCommitResponse(body: ResponseBody) {
  return decodeResponse(body).array((topicReader) => ({
    topic: topicReader.string() ?? "",
    partitions: topicReader.array((partitionReader) => ({
      partition: partitionReader.i32(),
      error: partitionReader.i16(),
    })),
  }));
}
