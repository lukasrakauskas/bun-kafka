import { describe, expect, test } from "bun:test";
import { Kafka, KafkaError, kafkaErrorName } from "../index.ts";
import { Writer } from "../src/bun/protocol.ts";
import { BROKERS } from "./helpers.ts";

const apiVersions = () => new Writer().i16(0).array(Array.from({ length: 64 }, (_, key) => key), (writer, key) => writer.i16(key).i16(0).i16(20));

describe("Observability", () => {
  test("error catalog names every documented Kafka error code", () => {
    expect(kafkaErrorName(1)).toBe("OFFSET_OUT_OF_RANGE");
    expect(kafkaErrorName(19)).toBe("NOT_ENOUGH_REPLICAS");
    expect(kafkaErrorName(56)).toBe("KAFKA_STORAGE_ERROR");
    expect(kafkaErrorName(90)).toBe("PRODUCER_FENCED");
    expect(kafkaErrorName(999)).toBe("UNKNOWN_ERROR_999");
    const e = new KafkaError(19, "context");
    expect(new KafkaError(19, null, { retriable: true }).retriable).toBe(true);
    expect(new KafkaError(58).fatal).toBe(false); // fatal flags are opt-in on the error object
  });

  test("stats() counts requests and bytes against a live broker", async () => {
    const kafka = new Kafka({ brokers: BROKERS.split(",") });
    try {
      await kafka.admin().metadata(["__consumer_offsets"]);
      const stats = kafka.stats();
      expect(stats.requests).toBeGreaterThanOrEqual(2); // ApiVersions + Metadata
      expect(stats.bytesSent).toBeGreaterThan(0);
      expect(stats.bytesReceived).toBeGreaterThan(0);
      expect(stats.retries).toBe(0);
      expect(stats.throttles).toBe(0);
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);

  test("healthCheck reports every broker with latency", async () => {
    const kafka = new Kafka({ brokers: BROKERS.split(",") });
    try {
      await kafka.admin().metadata(); // discover brokers
      const report = await kafka.healthCheck(5_000);
      expect(report.brokers.length).toBeGreaterThanOrEqual(1);
      for (const broker of report.brokers) {
        expect(broker.ok).toBe(true);
        expect(broker.latencyMs).toBeGreaterThanOrEqual(0);
        expect(broker.address).toBeTruthy();
      }
    } finally {
      await kafka.disconnect();
    }
  }, 30_000);

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
          const body = key === 18 ? apiVersions()
            : key === 3 ? new Writer()
              .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, b) => writer.i32(b.id).string(b.host).i32(b.port).string(null))
              .string(null)
              .i32(1)
              .array(["events"], (writer, topic) => writer.i16(0).string(topic).bool(false).array([0], (p) => p.i16(0).i32(0).i32(1).array([1], (w) => w.i32(1)).array([1], (w) => w.i32(1))))
              : key === 10 ? new Writer().i16(0).i32(1).string("127.0.0.1").i32(listener.port)
                : new Writer().array(["events"], (writer, t) => writer.string(t).array([0], (partitions, p) => partitions.i32(p).i16(0).i64(0).i64(3)));
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
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
      const statsEvents = events.filter((e): e is { type: "stats"; stats: Record<string, number> } =>
        (e as { type: string }).type === "stats");
      expect(statsEvents.length).toBeGreaterThanOrEqual(2);
      expect(warnings.some((w) => w.includes("retry"))).toBe(true);
      await consumer.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
