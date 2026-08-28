import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest } from "../codec.ts";

const readTopicResults = (body: ResponseBody) => {
  const reader = decodeResponse(body);
  const throttleMs = reader.i32();
  const values = reader.array((item) => ({
    name: item.string() ?? "",
    error: item.i16(),
    message: item.string(),
  }));
  return { throttleMs, results: values };
};
export function writeCreateTopicsRequest(
  topics: readonly {
    name: string;
    numPartitions: number;
    replicationFactor?: number;
    assignments?: readonly { partition: number; brokers: readonly number[] }[];
    configs?: Record<string, string | null>;
  }[],
  timeoutMs: number,
  validateOnly: boolean,
): RequestBody {
  return encodeRequest((w) =>
    w
      .array(topics, (x, t) =>
        x
          .string(t.name)
          .i32(t.numPartitions)
          .i16(t.replicationFactor ?? -1)
          .array(t.assignments ?? [], (a, assignment) =>
            a.i32(assignment.partition).array(assignment.brokers, (b, n) => b.i32(n)),
          )
          .array(Object.entries(t.configs ?? {}), (c, [n, v]) => c.string(n).string(v)),
      )
      .i32(timeoutMs)
      .bool(validateOnly),
  );
}
export function writeDeleteTopicsRequest(
  topics: readonly string[],
  timeoutMs: number,
): RequestBody {
  return encodeRequest((w) => w.array(topics, (x, t) => x.string(t)).i32(timeoutMs));
}
export function writeCreatePartitionsRequest(
  topics: readonly { name: string; count: number; assignments?: number[][] }[],
  timeoutMs: number,
  validateOnly: boolean,
): RequestBody {
  return encodeRequest((w) =>
    w.array(topics, (x, t) =>
      x
        .string(t.name)
        .i32(t.count)
        .array(t.assignments ?? [], (a, bs) => a.array(bs, (b, n) => b.i32(n)))
        .i32(timeoutMs)
        .bool(validateOnly),
    ),
  );
}
export function readCreateTopicsResponse(body: ResponseBody) {
  return readTopicResults(body);
}
export function readDeleteTopicsResponse(body: ResponseBody) {
  const reader = decodeResponse(body);
  const throttleMs = reader.i32();
  const values = reader.array((item) => ({
    name: item.string() ?? "",
    error: item.i16(),
    message: null,
  }));
  return { throttleMs, results: values };
}
export function readCreatePartitionsResponse(body: ResponseBody) {
  return readTopicResults(body);
}
