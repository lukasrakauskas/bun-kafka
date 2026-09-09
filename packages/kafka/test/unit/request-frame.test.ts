import { expect, test } from "bun:test";
import { RequestBody, encoder, writeRequestFrame } from "../../src/protocol/index.ts";

test("request frames preserve wire bytes and own exact-sized buffers", () => {
  for (const size of [0, 1, 255, 256, 257, 65536]) {
    for (const clientId of ["", "client", "klientas-ą🚀"]) {
      for (const flexible of [false, true]) {
        const input = new Uint8Array(size + 2).fill(0x78);
        const payload = input.subarray(1, size + 1);
        const body = new RequestBody(payload);
        const args = { apiKey: 18, apiVersion: 3, correlationId: 123, clientId, body, flexible };
        const expected = encoder()
          .i32(0)
          .i16(args.apiKey)
          .i16(args.apiVersion)
          .i32(args.correlationId)
          .string(clientId);
        if (flexible) expected.uvarint(0);
        expected.raw(payload).patchI32(0, expected.length - 4);
        const snapshot = expected.result();
        const frame = writeRequestFrame(args);
        expect(frame).toEqual(snapshot);
        expect(frame.byteOffset).toBe(0);
        expect(frame.buffer.byteLength).toBe(frame.byteLength);
        expect(frame.buffer).not.toBe(input.buffer);
        input.fill(0x55);
        const next = writeRequestFrame(args);
        next.fill(0);
        expect(frame).toEqual(snapshot);
        expect(input.every((byte) => byte === 0x55)).toBe(true);
      }
    }
  }
});

test("request client IDs enforce the signed 16-bit UTF-8 byte limit", () => {
  const args = {
    apiKey: 18,
    apiVersion: 0,
    correlationId: 1,
    body: new RequestBody(new Uint8Array()),
  };
  for (const clientId of ["x".repeat(32767), "ą".repeat(16383) + "x"]) {
    const frame = writeRequestFrame({ ...args, clientId });
    expect(new DataView(frame.buffer).getInt16(12)).toBe(32767);
    expect(frame.byteLength).toBe(14 + 32767);
  }
  for (const clientId of ["x".repeat(32768), "🚀".repeat(8192)]) {
    expect(() => writeRequestFrame({ ...args, clientId })).toThrow("Kafka string is too long");
  }
});
