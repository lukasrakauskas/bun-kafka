import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Writer } from "../../src/bun/protocol.ts";

const apiVersions = (produceMin = 0) =>
  new Writer().i16(0).array(
    Array.from({ length: 64 }, (_, key) => key),
    (writer, key) =>
      writer
        .i16(key)
        .i16(key === 0 ? produceMin : 0)
        .i16(20),
  );

function metadataBody(listenerPort: number) {
  return new Writer()
    .array([{ id: 1, host: "127.0.0.1", port: listenerPort }], (writer, b) =>
      writer.i32(b.id).string(b.host).i32(b.port).string(null),
    )
    .string("test-cluster-id")
    .i32(1)
    .array([{ name: "events" }], (writer, item) =>
      writer
        .i16(0)
        .string(item.name)
        .bool(false)
        .array([0], (pw) =>
          pw
            .i16(0)
            .i32(0)
            .i32(1)
            .array([1], (w) => w.i32(1))
            .array([1], (w) => w.i32(1)),
        ),
    );
}

describe("API version pinning", () => {
  test("requests stay at pinned versions even when brokers advertise newer ones", async () => {
    // api-key -> requested version; silent version drift shows up here first.
    const seen = new Map<number, number>();
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          let offset = 0;
          while (offset < request.byteLength) {
            const size = new DataView(request.buffer, request.byteOffset + offset).getInt32(0);
            const frameView = new DataView(request.buffer, request.byteOffset + offset + 4, 8);
            const key = frameView.getInt16(0);
            const version = frameView.getInt16(2);
            const correlation = new DataView(
              request.buffer,
              request.byteOffset + offset + 8,
              4,
            ).getInt32(0);
            seen.set(key, version);
            let body: Writer;
            if (key === 18) {
              body = apiVersions();
            } else if (key === 3) {
              body = metadataBody(listener.port);
            } else if (key === 10) {
              body = new Writer().i16(0).i32(1).string("127.0.0.1").i32(listener.port);
            } else if (key === 2) {
              body = new Writer().array(["events"], (writer, name) =>
                writer.string(name).array([0], (pWriter, p) => pWriter.i32(p).i16(0).i64(0).i64(7)),
              );
            } else if (key === 9) {
              body = new Writer()
                .array(["events"], (writer, name) =>
                  writer
                    .string(name)
                    .array([0], (pWriter, p) => pWriter.i32(p).i64(3).string(null).i16(0)),
                )
                .i16(0);
            } else if (key === 8) {
              body = new Writer().array(["events"], (writer, name) =>
                writer.string(name).array([0], (pWriter, p) => pWriter.i32(p).i16(0)),
              );
            } else if (key === 0) {
              body = new Writer()
                .array(["events"], (writer, name) =>
                  writer
                    .string(name)
                    .array([0], (pWriter, p) => pWriter.i32(p).i16(0).i64(100).i64(-1)),
                )
                .i32(0);
            } else {
              body = new Writer().i16(0);
            }
            const response = new Writer().i32(0).i32(correlation).raw(body.result());
            response.patchI32(0, response.length - 4);
            socket.write(response.result());
            offset += 4 + size;
          }
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const producer = kafka.producer({ lingerMs: 0 });
      await producer.send({ topic: "events", messages: [{ value: "v" }] });
      await producer.close();

      const a = kafka.admin();
      await a.topicOffsets("events");
      await a.groupOffsets("workers", ["events"]);
      await a.setGroupOffsets("workers", [
        { topic: "events", partitions: [{ partition: 0, offset: 3n }] },
      ]);
      await a.close();

      expect(seen.get(18)).toBe(0); // ApiVersions
      expect(seen.get(3)).toBe(2); // Metadata
      expect(seen.get(0)).toBe(3); // Produce
      expect(seen.get(2)).toBe(1); // ListOffsets
      expect(seen.get(10)).toBe(0); // FindCoordinator
      expect(seen.get(9)).toBe(2); // OffsetFetch
      expect(seen.get(8)).toBe(2); // OffsetCommit
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("refuses to send when a pinned version is outside the broker's advertised range", async () => {
    let sawProduce = false;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          let offset = 0;
          while (offset < request.byteLength) {
            const size = new DataView(request.buffer, request.byteOffset + offset).getInt32(0);
            const frameView = new DataView(request.buffer, request.byteOffset + offset + 4, 8);
            const key = frameView.getInt16(0);
            const correlation = new DataView(
              request.buffer,
              request.byteOffset + offset + 8,
              4,
            ).getInt32(0);
            if (key === 0) {
              sawProduce = true;
            }
            let body: Writer;
            if (key === 18) {
              body = apiVersions(4);
            } // Produce supported only from v4 up
            else if (key === 3) {
              body = metadataBody(listener.port);
            } else {
              body = new Writer().i16(0);
            }
            const response = new Writer().i32(0).i32(correlation).raw(body.result());
            response.patchI32(0, response.length - 4);
            socket.write(response.result());
            offset += 4 + size;
          }
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const producer = kafka.producer({ lingerMs: 0 });
      await expect(
        producer.send({ topic: "events", messages: [{ value: "v" }] }),
      ).rejects.toMatchObject({
        code: 35,
        message: expect.stringContaining("does not support API 0 version 3"),
      });
      expect(sawProduce).toBe(false);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("DescribeConfigs maps isDefault to the config source heuristic", async () => {
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          let offset = 0;
          while (offset < request.byteLength) {
            const size = new DataView(request.buffer, request.byteOffset + offset).getInt32(0);
            const frameView = new DataView(request.buffer, request.byteOffset + offset + 4, 8);
            const key = frameView.getInt16(0);
            const correlation = new DataView(
              request.buffer,
              request.byteOffset + offset + 8,
              4,
            ).getInt32(0);
            let body: Writer;
            if (key === 18) {
              body = apiVersions();
            } else if (key === 3) {
              body = metadataBody(listener.port);
            } else if (key === 32) {
              body = new Writer().i32(0).array([{ type: 2, name: "events" }], (writer, r) =>
                writer
                  .i16(0)
                  .string(null)
                  .i8(r.type)
                  .string(r.name)
                  .array(
                    [
                      { name: "retention.ms", value: "60000", isDefault: true },
                      { name: "cleanup.policy", value: "delete", isDefault: false },
                    ],
                    (cWriter, c) =>
                      cWriter
                        .string(c.name)
                        .string(c.value)
                        .bool(false)
                        .bool(c.isDefault)
                        .bool(false),
                  ),
              );
            } else {
              body = new Writer().i16(0);
            }
            const response = new Writer().i32(0).i32(correlation).raw(body.result());
            response.patchI32(0, response.length - 4);
            socket.write(response.result());
            offset += 4 + size;
          }
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const [resource] = await kafka
        .admin()
        .describeConfigs([{ resourceType: 2, resourceName: "events" }]);
      expect(resource?.error).toBe(0);
      expect(resource?.configs.find((config) => config.name === "retention.ms")).toMatchObject({
        value: "60000",
        source: 5,
        sensitive: false,
        readOnly: false,
      });
      expect(resource?.configs.find((config) => config.name === "cleanup.policy")).toMatchObject({
        value: "delete",
        source: 0,
      });
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("ApiVersions negotiation failures surface as UNSUPPORTED_VERSION instead of hanging", async () => {
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          // Pre-0.10 broker shape: reject every ApiVersions attempt outright.
          const correlation = new DataView(request.buffer, request.byteOffset + 8, 4).getInt32(0);
          const response = new Writer().i32(0).i32(correlation).raw(new Writer().i16(35).result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      await expect(kafka.admin().metadata()).rejects.toMatchObject({ code: 35 });
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
