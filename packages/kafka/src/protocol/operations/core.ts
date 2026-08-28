/* oxlint-disable anti-slop/no-known-value-widening */
import { KafkaError } from "../../errors.ts";
import { ResponseBody } from "../body.ts";
import { decodeResponse } from "../codec.ts";

export function readApiVersionsResponse(
  body: ResponseBody,
): Map<number, { min: number; max: number }> {
  const reader = decodeResponse(body);
  const error = reader.i16();
  if (error) {
    throw new KafkaError(error, "ApiVersions negotiation failed");
  }
  return new Map(
    reader.array((item) => [item.i16(), { min: item.i16(), max: item.i16() }] as const),
  );
}
export function readSaslHandshakeResponse(body: ResponseBody): {
  error: number;
  mechanisms: string[];
} {
  const reader = decodeResponse(body);
  return { error: reader.i16(), mechanisms: reader.array((item) => item.string() ?? "") };
}
export function readSaslAuthenticateResponse(body: ResponseBody): {
  error: number;
  message: string | null;
  authBytes: Uint8Array;
  sessionLifetimeMs: bigint;
} {
  const reader = decodeResponse(body);
  return {
    error: reader.i16(),
    message: reader.string(),
    authBytes: reader.bytes() ?? new Uint8Array(),
    sessionLifetimeMs: reader.i64(),
  };
}
export function readFindCoordinatorResponse(body: ResponseBody): {
  throttleMs: number;
  error: number;
  message: string | null;
  coordinatorId: number;
  host: string | null;
  port: number;
} {
  const reader = decodeResponse(body);
  return {
    throttleMs: reader.i32(),
    error: reader.i16(),
    message: reader.string(),
    coordinatorId: reader.i32(),
    host: reader.string(),
    port: reader.i32(),
  };
}
