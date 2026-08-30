import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest } from "../codec.ts";

const CONFIG_SOURCE_DEFAULT = 5;
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
