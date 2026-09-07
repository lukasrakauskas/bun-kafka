import { expect, test } from "bun:test";
import { ResponseFramer } from "../../src/bun/connection/response-framer.ts";
import {
  decodeRecordSet,
  encodeRecordBatch,
  readResponsePayload,
  writeResponseFrame,
} from "../../src/protocol/index.ts";

const frames = [0, 9, 257].map((length, i) =>
  writeResponseFrame(i, new Uint8Array(length).fill(i)),
);
const wire = new Uint8Array(frames.reduce((sum, frame) => sum + frame.length, 0));
let at = 0;
for (const frame of frames) {
  wire.set(frame, at);
  at += frame.length;
}
const expected = frames.map((frame) => frame.subarray(4));

test("frames survive every split point and byte-at-a-time delivery", () => {
  for (let split = 0; split <= wire.length; split++) {
    const framer = new ResponseFramer(1024);
    expect([...framer.push(wire.subarray(0, split)), ...framer.push(wire.subarray(split))]).toEqual(
      expected,
    );
  }
  for (let size = 1; size <= 17; size++) {
    const framer = new ResponseFramer(1024);
    const actual = [];
    for (let i = 0; i < wire.length; i += size)
      actual.push(...framer.push(wire.subarray(i, i + size)));
    expect(actual).toEqual(expected);
  }
});

test("complete frames borrow nonzero-offset chunks without inheriting Buffer slice semantics", () => {
  const batch = encodeRecordBatch([{ value: "safe" }]);
  const source = Buffer.concat([
    Buffer.alloc(3),
    Buffer.from(writeResponseFrame(42, batch)),
    Buffer.alloc(5),
  ]);
  const chunk = source.subarray(3, source.length - 5);
  const [frame] = new ResponseFramer(1024).push(chunk);
  expect(frame.buffer).toBe(chunk.buffer);
  expect(Buffer.isBuffer(frame)).toBe(false);
  const { body } = readResponsePayload(frame);
  const [message] = decodeRecordSet(body.data, "t", 0, 1);
  frame.fill(0);
  expect(new TextDecoder().decode(message.value)).toBe("safe");
});

test("invalid sizes are rejected in complete and fragmented headers", () => {
  for (const size of [-1, 0, 3, 1025]) {
    const header = new Uint8Array(4);
    new DataView(header.buffer).setInt32(0, size);
    expect(() => [...new ResponseFramer(1024).push(header)]).toThrow(/Invalid Kafka response size/);
    const framer = new ResponseFramer(1024);
    expect([...framer.push(header.subarray(0, 2))]).toEqual([]);
    expect(() => [...framer.push(header.subarray(2))]).toThrow(/Invalid Kafka response size/);
  }
});

test("reset discards partial headers and bodies without changing previously returned frames", () => {
  for (const split of [2, 7]) {
    const framer = new ResponseFramer(1024);
    const [retained] = framer.push(frames[1]);
    expect([...framer.push(frames[2].subarray(0, split))]).toEqual([]);
    framer.reset();
    expect([...framer.push(frames[2])]).toEqual([expected[2]]);
    expect(retained).toEqual(expected[1]);
  }
});
