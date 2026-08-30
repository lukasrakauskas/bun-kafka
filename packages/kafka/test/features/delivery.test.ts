import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { encoder, type KafkaEncoder } from "../../src/protocol/index.ts";

const apiVersions = () =>
  encoder()
    .i16(0)
    .array(
      Array.from({ length: 64 }, (_, key) => key),
      (writer, key) => writer.i16(key).i16(0).i16(20),
    );

describe("Producer delivery options", () => {
  test("acks=0 returns without a broker response", async () => {
    let produceRequests = 0;
    let answered = false;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          // Frames can coalesce inside one data event; parse all of them.
          let offset = 0;
          while (offset < request.byteLength) {
            const view = new DataView(
              request.buffer,
              request.byteOffset + offset,
              Math.min(request.byteLength - offset, 12),
            );
            const size = view.getInt32(0);
            const frame = request.subarray(offset, offset + 4 + size);
            offset += 4 + size;
            const frameView = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
            const key = frameView.getInt16(4);
            if (key === 0) {
              produceRequests++;
              continue; // acks=0: brokers never answer Produce requests.
            }
            const correlation = frameView.getInt32(8);
            const metadata = encoder()
              .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) =>
                writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
              )
              .string(null)
              .i32(1)
              .array([{ name: "events" }], (writer, item) => {
                writer
                  .i16(0)
                  .string(item.name)
                  .bool(false)
                  .array([0], (partitionWriter) =>
                    partitionWriter
                      .i16(0)
                      .i32(0)
                      .i32(1)
                      .array([1], (w) => w.i32(1))
                      .array([1], (w) => w.i32(1)),
                  );
              });
            const body = key === 18 ? apiVersions() : metadata;
            const response = encoder().i32(0).i32(correlation).raw(body.result());
            response.patchI32(0, response.length - 4);
            socket.write(response.result());
            answered = true;
          }
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`], requestTimeoutMs: 500 });
    try {
      const producer = kafka.producer();
      const results = await producer.send({
        topic: "events",
        acks: 0,
        messages: [{ value: "one" }, { value: "two" }],
      });
      // Both messages land on the single mocked partition; results are partition-level.
      expect(results).toHaveLength(1);
      expect(results[0].baseOffset).toBe(-1n);
      expect(results[0].logAppendTime).toBe(-1n);
      for (let i = 0; i < 50 && produceRequests === 0; i++) {
        await Bun.sleep(10);
      }
      expect(produceRequests).toBeGreaterThan(0);
      expect(answered).toBe(true); // Metadata was still answered; no hang on Produce.
      await producer.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("idempotent producers reject acks=0", () => {
    const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
    const producer = kafka.producer({ idempotent: true });
    expect(() => producer.send({ topic: "events", acks: 0, messages: [{ value: "x" }] })).toThrow(
      TypeError,
    );
  });

  test("delivery callbacks fire with an error when producing fails for good", async () => {
    const errors: unknown[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          if (key === 0) {
            socket.end(); // Kill the connection on Produce so retries exhaust.
            return;
          }
          const correlation = view.getInt32(8);
          const body =
            key === 18
              ? apiVersions()
              : encoder()
                  .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) =>
                    writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
                  )
                  .string(null)
                  .i32(1)
                  .array([{ name: "events" }], (writer, item) => {
                    writer
                      .i16(0)
                      .string(item.name)
                      .bool(false)
                      .array([0], (partitionWriter) =>
                        partitionWriter
                          .i16(0)
                          .i32(0)
                          .i32(1)
                          .array([1], (w) => w.i32(1))
                          .array([1], (w) => w.i32(1)),
                      );
                  });
          const response = encoder().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      retry: { maxRetries: 1, initialBackoffMs: 1, maxBackoffMs: 2 },
      requestTimeoutMs: 300,
    });
    try {
      const producer = kafka.producer();
      await expect(
        producer.send({
          topic: "events",
          messages: [
            {
              value: "lost",
              onDelivery: (error) => errors.push(error),
            },
          ],
        }),
      ).rejects.toBeDefined();
      expect(errors).toHaveLength(1);
      expect(errors[0]).toBeInstanceOf(Object);
      await producer.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
