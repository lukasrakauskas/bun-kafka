import { describe, expect, test } from "bun:test";
import { Kafka, KafkaError, kafkaErrorName, type KafkaEvent } from "../../index.ts";
import { encoder, type KafkaEncoder } from "../../src/protocol/index.ts";

const apiVersions = () =>
  encoder()
    .i16(0)
    .array(
      Array.from({ length: 64 }, (_, key) => key),
      (writer, key) => writer.i16(key).i16(0).i16(20),
    );

describe("Observability", () => {
  test("error catalog names every documented Kafka error code", () => {
    expect(kafkaErrorName(1)).toBe("OFFSET_OUT_OF_RANGE");
    expect(kafkaErrorName(19)).toBe("NOT_ENOUGH_REPLICAS");
    expect(kafkaErrorName(56)).toBe("KAFKA_STORAGE_ERROR");
    expect(kafkaErrorName(90)).toBe("PRODUCER_FENCED");
    expect(kafkaErrorName(999)).toBe("UNKNOWN_ERROR_999");
    new KafkaError(19, "context");
    expect(new KafkaError(19, null, { retriable: true }).retriable).toBe(true);
    expect(new KafkaError(58).fatal).toBe(false); // fatal flags are opt-in on the error object
  });

  test("healthCheck flags unreachable brokers", async () => {
    const kafka = new Kafka({ brokers: ["127.0.0.1:1"], connectTimeoutMs: 300 });
    try {
      const report = await kafka.healthCheck(500);
      expect(report.brokers[0]?.ok).toBe(false);
      expect(report.brokers[0]?.latencyMs).toBeLessThan(5_000);
    } finally {
      await kafka.disconnect();
    }
  }, 15_000);

  test("statsIntervalMs emits stats events and logger hooks fire", async () => {
    const events: unknown[] = [];
    const warnings: string[] = [];
    let offsetAttempts = 0;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key === 2 && offsetAttempts++ === 0) {
            socket.end();
            return;
          }
          let body: KafkaEncoder;
          if (key === 18) {
            body = apiVersions();
          } else if (key === 3) {
            body = encoder()
              .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, b) =>
                writer.i32(b.id).string(b.host).i32(b.port).string(null),
              )
              .string(null)
              .i32(1)
              .array(["events"], (writer, topic) =>
                writer
                  .i16(0)
                  .string(topic)
                  .bool(false)
                  .array([0], (p) =>
                    p
                      .i16(0)
                      .i32(0)
                      .i32(1)
                      .array([1], (w) => w.i32(1))
                      .array([1], (w) => w.i32(1)),
                  ),
              );
          } else if (key === 10) {
            body = encoder().i16(0).i32(1).string("127.0.0.1").i32(listener.port);
          } else {
            body = encoder().array(["events"], (writer, t) =>
              writer
                .string(t)
                .array([0], (partitions, p) => partitions.i32(p).i16(0).i64(0).i64(3)),
            );
          }
          const response = encoder().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      retry: { initialBackoffMs: 0, maxBackoffMs: 0 },
      statsIntervalMs: 25,
      onEvent: (event) => events.push(event),
      logger: { warn: (message) => warnings.push(message), debug: () => {} },
    });
    try {
      const consumer = kafka.consumer();
      await consumer.assign([{ topic: "events", partition: 0, offset: "earliest" }]);
      expect(consumer.position("events", 0)).toBe(3n);
      await Bun.sleep(80);
      const statsEvents = events.filter(
        (e): e is Extract<KafkaEvent, { type: "stats" }> => e.type === "stats",
      );
      expect(statsEvents.length).toBeGreaterThanOrEqual(2);
      expect(warnings.some((w) => w.includes("retry"))).toBe(true);
      await consumer.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
