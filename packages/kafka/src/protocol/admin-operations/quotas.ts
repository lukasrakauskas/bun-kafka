import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest } from "../codec.ts";

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
