import type { ClusterMetadata } from "../types.ts";
import { ResponseBody } from "./body.ts";
import { decodeResponse, emptyRequestBody, encodeRequest, type KafkaEncoder } from "./codec.ts";

const UUID_BYTES = 16;

type TopicIdsMetadataResponse = {
  throttleMs: number;
  topics: Array<{ error: number; name: string; id: Uint8Array }>;
};

export { emptyRequestBody, encodeRequest, decodeResponse };

export function writeEmptyRequest() {
  return emptyRequestBody();
}

export function writeSaslHandshakeRequest(mechanism: string) {
  return encodeRequest((w) => {
    w.string(mechanism);
  });
}

export function writeSaslAuthenticateRequest(payload: Uint8Array) {
  return encodeRequest((w) => {
    w.bytes(payload);
  });
}

export function writeMetadataRequest(topics: string[] | null) {
  return encodeRequest((w) => {
    w.array(topics, (encoder, topic) => encoder.string(topic));
  });
}

export function writeTopicIdsMetadataRequest(topics: readonly string[]) {
  return encodeRequest((writer) =>
    writer
      .compactArray(topics, (topicWriter, topic) =>
        topicWriter.raw(new Uint8Array(UUID_BYTES)).compactString(topic).tags(),
      )
      .bool(true)
      .bool(false)
      .bool(false)
      .tags(),
  );
}

export function readTopicIdsMetadataResponse(body: ResponseBody): TopicIdsMetadataResponse {
  const reader = decodeResponse(body);
  const throttleMs = reader.i32();
  reader.compactArray((broker) => {
    broker.i32();
    broker.compactString();
    broker.i32();
    broker.compactString();
    broker.skipTags();
  });
  reader.compactString();
  reader.i32();
  const topics = reader.compactArray((topic) => {
    const error = topic.i16();
    const name = topic.compactString() ?? "";
    const id = topic.raw(UUID_BYTES);
    topic.bool();
    topic.compactArray((partition) => {
      partition.i16();
      partition.i32();
      partition.i32();
      partition.i32();
      partition.compactArray((item) => item.i32());
      partition.compactArray((item) => item.i32());
      partition.compactArray((item) => item.i32());
      partition.skipTags();
    });
    topic.i32();
    topic.skipTags();
    return { error, name, id };
  });
  reader.i32();
  reader.skipTags();
  return { throttleMs, topics };
}

export function readMetadataResponse(
  body: ResponseBody,
): ClusterMetadata & { controllerId: number } {
  const reader = decodeResponse(body);
  const brokers = reader.array((item) => {
    const broker = { id: item.i32(), host: item.string() ?? "", port: item.i32() };
    item.string();
    return broker;
  });
  const clusterId = reader.string();
  const controllerId = reader.i32();
  const topics = reader.array((item) => ({
    err: item.i16(),
    name: item.string() ?? "",
    internal: item.bool(),
    partitions: item.array((partitionReader) => {
      const err = partitionReader.i16();
      const id = partitionReader.i32();
      const leader = partitionReader.i32();
      partitionReader.array((x) => x.i32());
      partitionReader.array((x) => x.i32());
      return { err, id, leader };
    }),
  }));
  return { brokers, topics, controllerId, clusterId };
}

export function writeFindCoordinatorRequestV1(key: string, keyType: number) {
  return encodeRequest((w) => {
    w.string(key).i8(keyType);
  });
}

export function writeFindCoordinatorRequest(key: string) {
  return encodeRequest((w) => {
    w.string(key);
  });
}

export type { KafkaEncoder };
