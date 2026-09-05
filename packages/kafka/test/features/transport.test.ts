import { expect, test } from "bun:test";
import { Connection } from "../../src/bun/connection/connection.ts";
import { ResponseFramer } from "../../src/bun/connection/response-framer.ts";
import { writeEmptyRequest, writeResponseFrame } from "../../src/protocol/index.ts";

test("TCP response data stays valid across later socket reads and close", async () => {
  const framer = new ResponseFramer(1024);
  let sequence = 0;
  const listener = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      data(socket, chunk) {
        for (const frame of framer.push(chunk)) {
          const correlation = new DataView(frame.buffer, frame.byteOffset).getInt32(4);
          socket.write(writeResponseFrame(correlation, new Uint8Array(1024).fill(sequence++)));
        }
      },
    },
  });
  const connection = new Connection(`127.0.0.1:${listener.port}`, {
    clientId: "transport-test",
    requestTimeoutMs: 1000,
    connectTimeoutMs: 1000,
    maxResponseBytes: 2048,
  });
  try {
    const retained = [];
    for (let i = 0; i < 100; i++)
      retained.push((await connection.request(18, 0, writeEmptyRequest())).data);
    connection.close();
    for (let i = 0; i < retained.length; i++) {
      expect(retained[i]).toEqual(new Uint8Array(1024).fill(i));
      expect(Buffer.isBuffer(retained[i])).toBe(false);
    }
  } finally {
    connection.close();
    listener.stop(true);
  }
});
