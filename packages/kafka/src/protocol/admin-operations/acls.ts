import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest } from "../codec.ts";

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
