import { Connection } from "../src/bun/connection/connection.ts";
import { ResponseFramer } from "../src/bun/connection/response-framer.ts";
import { writeEmptyRequest, writeResponseFrame } from "../src/protocol/index.ts";

// Synthetic, sequential TCP request/reply benchmark, not a Kafka broker benchmark.
const size = Number(process.argv[2] ?? 1024);
const count = Number(process.argv[3] ?? 10000);
if (!Number.isSafeInteger(size) || size < 1 || !Number.isSafeInteger(count) || count < 1) {
  throw new RangeError("Expected positive response size and request count");
}
const framer = new ResponseFramer(1024);
const response = writeResponseFrame(0, new Uint8Array(size).fill(0x78));
let pending: Uint8Array | undefined;
function drain(socket: Bun.Socket): void {
  if (!pending) return;
  const written = socket.write(pending);
  if (written < 0) throw new Error("Socket closed during benchmark");
  pending = written === pending.length ? undefined : pending.subarray(written);
}
const listener = Bun.listen({
  hostname: "127.0.0.1",
  port: 0,
  socket: {
    data(socket, chunk) {
      for (const frame of framer.push(chunk)) {
        new DataView(response.buffer).setInt32(
          4,
          new DataView(frame.buffer, frame.byteOffset).getInt32(4),
        );
        pending = response;
        drain(socket);
      }
    },
    drain,
  },
});
const connection = new Connection(`127.0.0.1:${listener.port}`, {
  clientId: "transport-bench",
  requestTimeoutMs: 10000,
  connectTimeoutMs: 1000,
  maxResponseBytes: size + 4,
});
const request = writeEmptyRequest();
let checksum = 0;
async function roundtrip(): Promise<void> {
  const body = await connection.request(18, 0, request);
  if (body.data.length !== size || body.data.at(-1) !== 0x78) throw new Error("Corrupt response");
  checksum += body.data.length;
}
try {
  for (let i = 0; i < 100; i++) await roundtrip();
  checksum = 0;
  const start = performance.now();
  for (let i = 0; i < count; i++) await roundtrip();
  console.log(JSON.stringify({ size, count, transportMs: performance.now() - start, checksum }));
} finally {
  connection.close();
  listener.stop(true);
}
