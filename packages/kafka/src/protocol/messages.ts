import type { ClusterMetadata } from "../types.ts";
import { ResponseBody } from "./body.ts";
import { decodeResponse, emptyRequestBody, encodeRequest, type KafkaEncoder } from "./codec.ts";

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
