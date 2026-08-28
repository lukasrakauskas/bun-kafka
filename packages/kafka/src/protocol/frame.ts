import { SIZE_I32 } from "../bun/shared.ts";
import { RequestBody, ResponseBody, requestFromWriter, responseFromReader } from "./body.ts";
import { Reader } from "./wire/reader.ts";
import { Writer } from "./wire/writer.ts";

export type RequestFrameInput = {
  apiKey: number;
  apiVersion: number;
  correlationId: number;
  clientId: string;
  body: RequestBody;
  flexible?: boolean;
};

export function writeRequestFrame(input: RequestFrameInput): Uint8Array {
  const frame = new Writer();
  frame
    .i32(0)
    .i16(input.apiKey)
    .i16(input.apiVersion)
    .i32(input.correlationId)
    .string(input.clientId);
  if (input.flexible) {
    frame.uvarint(0);
  }
  frame.raw(input.body.toBytes());
  frame.patchI32(0, frame.length - SIZE_I32);
  return frame.result();
}

export function writeResponseFrame(correlationId: number, bodyBytes: Uint8Array): Uint8Array {
  const frame = new Writer().i32(0).i32(correlationId).raw(bodyBytes);
  frame.patchI32(0, frame.length - SIZE_I32);
  return frame.result();
}

export type ResponsePayload = {
  correlationId: number;
  body: ResponseBody;
};

export function readResponsePayload(payload: Uint8Array, flexible = false): ResponsePayload {
  const header = new Reader(payload);
  const correlationId = header.i32();
  let bodyStart = header.offset;
  if (flexible) {
    header.skipTags();
    bodyStart = header.offset;
  }
  // Body-only view so response.data matches the historic Reader(frame.subarray(4)) shape.
  return {
    correlationId,
    body: responseFromReader(new Reader(payload.subarray(bodyStart))),
  };
}

export function requestFromBuilder(build: (writer: Writer) => Writer | void): RequestBody {
  const writer = new Writer();
  const out = build(writer);
  return requestFromWriter(out instanceof Writer ? out : writer);
}
