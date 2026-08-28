import { RequestBody, ResponseBody } from "../body.ts";
import { decodeBytes, decodeResponse, encodeRequest } from "../codec.ts";

export function writeListGroupsRequest(): RequestBody {
  return encodeRequest((w) => w.array([], () => {}));
}
export function readListGroupsResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    error = r.i16(),
    groups = r.array((x) => ({
      groupId: x.string() ?? "",
      protocolType: x.string() ?? "",
      state: "",
    }));
  return { throttleMs, error, groups };
}
export function writeGroupIdsRequest(ids: readonly string[]): RequestBody {
  return encodeRequest((w) => w.array(ids, (x, id) => x.string(id)));
}
export function readDeleteGroupsResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    values = r.array((x) => ({ name: x.string() ?? "", error: x.i16(), message: null }));
  return { throttleMs, results: values };
}
export function readDescribeGroupsResponse(body: ResponseBody) {
  const data = decodeResponse(body).data;
  for (const withMessage of [true, false]) {
    try {
      const r = decodeBytes(data),
        throttleMs = r.i32(),
        groups = r.array((x) => {
          const error = x.i16(),
            message = withMessage ? x.string() : null,
            groupId = x.string() ?? "",
            state = x.string() ?? "",
            protocolType = x.string() ?? "",
            protocol = x.string(),
            members = x.array((m) => ({
              memberId: m.string() ?? "",
              clientId: m.string() ?? "",
              clientHost: m.string() ?? "",
              memberMetadata: m.bytes(),
              memberAssignment: m.bytes(),
            }));
          return { error, message, groupId, state, protocolType, protocol, members };
        });
      if (!r.remaining) {
        return { throttleMs, groups };
      }
    } catch {}
  }
  throw new Error("Malformed DescribeGroups response");
}
export function writeDeleteRecordsRequest(
  topics: readonly { name: string; partitions: readonly { index: number; offset: bigint }[] }[],
  timeoutMs: number,
): RequestBody {
  return encodeRequest((w) =>
    w
      .array(topics, (x, t) =>
        x.string(t.name).array(t.partitions, (p, v) => p.i32(v.index).i64(v.offset)),
      )
      .i32(timeoutMs),
  );
}
export function readDeleteRecordsResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    values = r
      .array((t) => {
        const name = t.string() ?? "";
        return t.array((p) => ({ name, index: p.i32(), lowWatermark: p.i64(), error: p.i16() }));
      })
      .flat();
  return { throttleMs, results: values };
}
