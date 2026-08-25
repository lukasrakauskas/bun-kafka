import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Reader, Writer } from "../../src/bun/protocol.ts";

const apiVersionsBody = new Writer()
  .i16(0)
  .array([18, 32, 33, 44], (writer, key) => writer.i16(key).i16(0).i16(1))
  .i32(0);

function requestBodyBytes(buffer: ArrayBuffer, byteOffset: number, byteLength: number): Uint8Array {
  const clientIdLen = new DataView(buffer, byteOffset, byteLength).getInt16(12);
  const start = byteOffset + 14 + clientIdLen + 1; // +1 flexible header tag byte
  return new Uint8Array(buffer, start, byteLength - (14 + clientIdLen + 1));
}

function reply(socket: Bun.TcpSocket, correlation: number, body: Writer) {
  const response = new Writer().i32(correlation).uvarint(0).raw(body.result());
  socket.write(new Writer().i32(0).patchI32(0, response.length).raw(response.result()).result());
}

describe("IncrementalAlterConfigs (mock broker)", () => {
  test("sends v1 flexible resource ops and parses per-resource results", async () => {
    let decoded: unknown;
    let sawValidateOnly = false;
    let sawKey = false;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key === 18) return reply(socket, correlation, apiVersionsBody);
          if (key !== 44) return;
          sawKey = true;
          const req = new Reader(
            requestBodyBytes(request.buffer, request.byteOffset, request.byteLength),
          );
          const resources = req.compactArray((r) => {
            const resource = {
              resourceType: r.i8(),
              resourceName: r.compactString(),
              configs: r.compactArray((c) => {
                const config = {
                  name: c.compactString(),
                  operation: c.i8(),
                  value: c.compactString(),
                };
                c.skipTags();
                return config;
              }),
            };
            r.skipTags();
            return resource;
          });
          sawValidateOnly = req.bool();
          decoded = resources;
          reply(
            socket,
            correlation,
            new Writer()
              .i32(7) // throttle
              .compactArray(
                [
                  { error: 0, message: null as string | null, type: 2, name: "t1" },
                  { error: 82, message: "invalid config", type: 4, name: "broker-1" },
                ],
                (writer, item) =>
                  writer
                    .i16(item.error)
                    .compactString(item.message)
                    .i8(item.type)
                    .compactString(item.name)
                    .tags(),
              )
              .tags(),
          );
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const results = await kafka.admin().incrementalAlterConfigs([
        {
          resourceType: 2,
          resourceName: "t1",
          ops: [
            { name: "retention.ms", operation: "set", value: "604800000" },
            { name: "cleanup.policy", operation: "append", value: "compact" },
            { name: "follower.replication.throttled.replicas", operation: "delete" },
          ],
        },
        {
          resourceType: 4,
          resourceName: "broker-1",
          ops: [{ name: "log.cleaner.threads", operation: "subtract", value: "1" }],
        },
      ]);
      expect(sawKey).toBe(true);
      expect(sawValidateOnly).toBe(false);
      expect(decoded).toEqual([
        {
          resourceType: 2,
          resourceName: "t1",
          configs: [
            { name: "retention.ms", operation: 0, value: "604800000" },
            { name: "cleanup.policy", operation: 2, value: "compact" },
            { name: "follower.replication.throttled.replicas", operation: 1, value: null },
          ],
        },
        {
          resourceType: 4,
          resourceName: "broker-1",
          configs: [{ name: "log.cleaner.threads", operation: 3, value: "1" }],
        },
      ]);
      // Throttle surfaced before the body; per-resource errors and names round-trip.
      expect(results).toEqual([
        { name: "t1", error: 0, message: null },
        { name: "broker-1", error: 82, message: "invalid config" },
      ]);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("empty input skips the wire entirely", async () => {
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data() {
          throw new Error("no requests expected");
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      expect(await kafka.admin().incrementalAlterConfigs([])).toEqual([]);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
