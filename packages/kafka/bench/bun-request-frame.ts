import assert from "node:assert/strict";
import { RequestBody, writeRequestFrame } from "../src/protocol/index.ts";

// Isolated request framing, not broker throughput. The body is prepared outside the timer.
const size = Number(process.argv[2] ?? 524288);
const count = Number(process.argv[3] ?? 2000);
assert(Number.isSafeInteger(size) && size >= 1 && size <= 1024 * 1024);
assert(Number.isSafeInteger(count) && count >= 1 && count <= 1_000_000);
const payload = new Uint8Array(size).fill(0x78);
const input = {
  apiKey: 0,
  apiVersion: 3,
  correlationId: 0,
  clientId: "frame-bench",
  body: new RequestBody(payload),
};
let frame = writeRequestFrame(input);
for (let i = 0; i < 1000; i++) frame = writeRequestFrame(input);
let checksum = 0;
const started = performance.now();
for (let i = 0; i < count; i++) {
  input.correlationId = i;
  frame = writeRequestFrame(input);
  checksum += frame.length;
}
const elapsedMs = performance.now() - started;
assert.equal(frame.byteLength, 25 + size);
assert.equal(frame.buffer.byteLength, frame.byteLength);
assert.equal(new DataView(frame.buffer).getInt32(8), count - 1);
assert.equal(Buffer.compare(frame.subarray(25), payload), 0);
assert.equal(checksum, (25 + size) * count);
console.log(JSON.stringify({ size, count, elapsedMs, checksum }));
