import { RequestBody, ResponseBody } from "./body.ts";
import { decodeResponse, encodeRequest, decodeBytes, type KafkaDecoder } from "./codec.ts";

const CONFIG_SOURCE_DEFAULT = 5;
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
export function writeDescribeConfigsRequest(
  resources: readonly {
    resourceType: number;
    resourceName: string;
    configNames?: readonly string[] | null;
  }[],
): RequestBody {
  return encodeRequest((w) =>
    w.array(resources, (x, r) =>
      x
        .i8(r.resourceType)
        .string(r.resourceName)
        .array(r.configNames ?? null, (configName, v) => configName.string(v)),
    ),
  );
}
export function readDescribeConfigsResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32();
  const values = r.array((x) => {
    const error = x.i16(),
      message = x.string(),
      resourceType = x.i8(),
      resourceName = x.string() ?? "",
      configs = x.array((c) => {
        const name = c.string() ?? "",
          value = c.string(),
          readOnly = c.bool(),
          isDefault = c.bool(),
          sensitive = c.bool();
        return { name, value, source: isDefault ? CONFIG_SOURCE_DEFAULT : 0, sensitive, readOnly };
      });
    return { resourceType, resourceName, error, message, configs };
  });
  return { throttleMs, results: values };
}
export function writeAlterConfigsRequest(
  resources: readonly {
    resourceType: number;
    resourceName: string;
    configs: Record<string, string | null>;
  }[],
): RequestBody {
  return encodeRequest((w) =>
    w.array(resources, (x, r) =>
      x
        .i8(r.resourceType)
        .string(r.resourceName)
        .array(Object.entries(r.configs), (c, [n, v]) => c.string(n).string(v)),
    ),
  );
}
export function readAlterConfigsResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32();
  const values = r.array((x) => {
    const error = x.i16(),
      message = x.string();
    x.i8();
    return { name: x.string() ?? "", error, message };
  });
  return { throttleMs, results: values };
}
export function writeIncrementalAlterConfigsRequest(
  resources: readonly {
    resourceType: number;
    resourceName: string;
    ops: readonly {
      name: string;
      operation: "set" | "delete" | "append" | "subtract";
      value?: string | null;
    }[];
  }[],
  validateOnly: boolean,
): RequestBody {
  const ops = { set: 0, delete: 1, append: 2, subtract: 3 } as const;
  return encodeRequest((w) =>
    w
      .compactArray(resources, (x, r) =>
        x
          .i8(r.resourceType)
          .compactString(r.resourceName)
          .compactArray(r.ops, (o, p) =>
            o
              .compactString(p.name)
              .i8(ops[p.operation])
              .compactString(p.value ?? null)
              .tags(),
          )
          .tags(),
      )
      .bool(validateOnly)
      .tags(),
  );
}
export function readIncrementalAlterConfigsResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    values = r.compactArray((x) => {
      const error = x.i16(),
        message = x.compactString();
      x.i8();
      const name = x.compactString() ?? "";
      x.skipTags();
      return { name, error, message };
    });
  return { throttleMs, results: values };
}
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
export function writeDescribeClientQuotasRequest(
  c: readonly {
    entityType: string;
    matchType?: "exact" | "defaulted" | "any";
    match: string | null;
  }[],
  strict: boolean,
): RequestBody {
  const types = { exact: 0, defaulted: 1, any: 2 } as const;
  return encodeRequest((w) =>
    w
      .compactArray(c, (x, v) =>
        x
          .compactString(v.entityType)
          .i8(types[v.matchType ?? "exact"])
          .compactString(v.match),
      )
      .bool(strict)
      .tags(),
  );
}
export function readDescribeClientQuotasResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    error = r.i16(),
    message = r.compactString(),
    entries = r.compactArray((e) => {
      const entities = e.compactArray((x) => {
          const v = { entityType: x.compactString() ?? "", entityName: x.compactString() };
          x.skipTags();
          return v;
        }),
        values = e.compactArray((x) => {
          const v = { name: x.compactString() ?? "", value: x.f64() };
          x.skipTags();
          return v;
        });
      e.skipTags();
      return { entities, values };
    });
  return { throttleMs, error, message, entries };
}
export function writeAlterClientQuotasRequest(
  entries: readonly {
    entity: readonly { entityType: string; entityName: string | null }[];
    ops: readonly { key: string; value?: number; remove?: boolean }[];
    validateOnly?: boolean;
  }[],
): RequestBody {
  return encodeRequest((w) =>
    w
      .compactArray(entries, (e, v) => {
        e.compactArray(v.entity, (x, i) =>
          x.compactString(i.entityType).compactString(i.entityName).tags(),
        );
        e.compactArray(v.ops, (x, o) =>
          x
            .compactString(o.key)
            .f64(o.value ?? Number.NaN)
            .bool(o.remove ?? false)
            .tags(),
        );
        e.tags();
      })
      .bool(entries.some((e) => e.validateOnly))
      .tags(),
  );
}
export function readAlterClientQuotasResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    entries = r.compactArray((e) => {
      const error = e.i16(),
        message = e.compactString(),
        entity = e.compactArray((x) => {
          const v = { entityType: x.compactString() ?? "", entityName: x.compactString() };
          x.skipTags();
          return v;
        });
      e.skipTags();
      return { error, message, entity };
    });
  return { throttleMs, entries };
}
type Token = {
  principalType: string;
  principalName: string;
  tokenRequester: string;
  issueTimestampMs: bigint;
  expiryTimestampMs: bigint;
  maxTimestampMs: bigint;
  tokenId: string;
  hmac: Uint8Array | null;
};
function token(r: KafkaDecoder): Token {
  const v = {
    principalType: r.compactString() ?? "",
    principalName: r.compactString() ?? "",
    tokenRequester: r.compactString() ?? "",
    issueTimestampMs: r.i64(),
    expiryTimestampMs: r.i64(),
    maxTimestampMs: r.i64(),
    tokenId: r.compactString() ?? "",
    hmac: r.compactBytes(),
  };
  r.skipTags();
  return v;
}
export function writeCreateDelegationTokenRequest(
  a: string | null,
  b: string | null,
  p: number | bigint,
): RequestBody {
  return encodeRequest((w) => w.compactString(a).compactString(b).i64(BigInt(p)).tags());
}
export function readCreateDelegationTokenResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    error = r.i16(),
    message = r.compactString();
  return { throttleMs, error, message, token: token(r) };
}
export function writeDescribeDelegationTokenRequest(
  o: readonly { principalType: string; principalName: string }[],
): RequestBody {
  return encodeRequest((w) =>
    w
      .compactArray(o.length ? o : null, (x, v) =>
        x.compactString(v.principalType).compactString(v.principalName).tags(),
      )
      .tags(),
  );
}
export function readDescribeDelegationTokenResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    error = r.i16(),
    message = r.compactString();
  return { throttleMs, error, message, tokens: r.compactArray(token) };
}
export function writeTokenPeriodRequest(h: Uint8Array, p: number | bigint): RequestBody {
  return encodeRequest((w) => w.compactBytes(h).i64(BigInt(p)).tags());
}
export function readTokenPeriodResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    error = r.i16(),
    message = r.compactString();
  return { throttleMs, error, message, expiryTimestampMs: r.i64() };
}

export function writeCreateAclsRequest(
  b: readonly {
    resourceType: number;
    resourceName: string;
    principal: string;
    host: string;
    operation: number;
    permissionType: number;
  }[],
  timeoutMs: number,
): RequestBody {
  return encodeRequest((w) =>
    w
      .array(b, (x, a) =>
        x
          .i8(a.resourceType)
          .string(a.resourceName)
          .string(a.principal)
          .string(a.host)
          .i8(a.operation)
          .i8(a.permissionType),
      )
      .i32(timeoutMs),
  );
}
export function readCreateAclsResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    results = r.array((x) => ({ error: x.i16(), message: x.string() }));
  return { throttleMs, results };
}
type Acl = {
  resourceType: number;
  resourceName: string | null;
  principal: string | null;
  host: string | null;
  operation: number;
  permissionType: number;
};
export function writeDescribeAclsRequest(f: Acl): RequestBody {
  return encodeRequest((w) =>
    w
      .i8(f.resourceType)
      .string(f.resourceName)
      .string(f.principal)
      .string(f.host)
      .i8(f.operation)
      .i8(f.permissionType),
  );
}
export function readDescribeAclsResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    error = r.i16(),
    message = r.string(),
    acls = r
      .array((x) => {
        const resourceType = x.i8(),
          resourceName = x.string() ?? "";
        return x.array((a) => ({
          resourceType,
          resourceName,
          principal: a.string() ?? "",
          host: a.string() ?? "",
          operation: a.i8(),
          permissionType: a.i8(),
        }));
      })
      .flat();
  return { throttleMs, error, message, acls };
}
export function writeDeleteAclsRequest(f: readonly Acl[], timeoutMs: number): RequestBody {
  return encodeRequest((w) =>
    w
      .array(f, (x, a) =>
        x
          .i8(a.resourceType)
          .string(a.resourceName)
          .string(a.principal)
          .string(a.host)
          .i8(a.operation)
          .i8(a.permissionType),
      )
      .i32(timeoutMs),
  );
}
export function readDeleteAclsResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    results = r.array((x) => {
      const error = x.i16(),
        message = x.string(),
        acls = x.array((a) => ({
          error: a.i16(),
          message: a.string(),
          resourceType: a.i8(),
          resourceName: a.string() ?? "",
          principal: a.string() ?? "",
          host: a.string() ?? "",
          operation: a.i8(),
          permissionType: a.i8(),
        }));
      return { error, message, acls };
    });
  return { throttleMs, results };
}
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
