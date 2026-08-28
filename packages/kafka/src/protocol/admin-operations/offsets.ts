import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest } from "../codec.ts";

export function writeOffsetFetchRequest(
  groupId: string,
  topic: string,
  partitions: readonly number[],
): RequestBody {
  return encodeRequest((w) =>
    w
      .string(groupId)
      .array([topic], (x, name) => x.string(name).array(partitions, (p, n) => p.i32(n))),
  );
}
export function readOffsetFetchAdminResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    topics = r
      .array((t) => {
        const name = t.string() ?? "";
        return t.array((p) => ({
          name,
          partition: p.i32(),
          offset: p.i64(),
          metadata: p.string(),
          error: p.i16(),
        }));
      })
      .flat();
  return { topics, topError: r.i16() };
}
export function writeOffsetCommitRequest(
  groupId: string,
  topics: readonly {
    topic: string;
    partitions: readonly { partition: number; offset: bigint; metadata?: string }[];
  }[],
): RequestBody {
  return encodeRequest((w) =>
    w
      .string(groupId)
      .i32(-1)
      .string("")
      .i64(-1n)
      .array(topics, (t, v) =>
        t.string(v.topic).array(v.partitions, (p, x) =>
          p
            .i32(x.partition)
            .i64(x.offset)
            .string(x.metadata ?? null),
        ),
      ),
  );
}
export function readOffsetCommitResponse(body: ResponseBody) {
  return decodeResponse(body).array((x) => ({
    topic: x.string() ?? "",
    partitions: x.array((p) => ({ partition: p.i32(), error: p.i16() })),
  }));
}
export function writeAdminListOffsetsRequest(
  topic: string,
  partition: number,
  timestamp: bigint,
): RequestBody {
  return encodeRequest((w) =>
    w
      .i32(-1)
      .array([topic], (topicWriter, name) =>
        topicWriter
          .string(name)
          .array([partition], (partitionWriter, numberValue) =>
            partitionWriter.i32(numberValue).i64(timestamp),
          ),
      ),
  );
}
export function readAdminListOffsetsResponse(body: ResponseBody) {
  return decodeResponse(body)
    .array((t) => {
      t.string();
      return t.array((p) => {
        p.i32();
        const error = p.i16();
        p.i64();
        return { error, offset: p.i64() };
      });
    })
    .flat();
}
