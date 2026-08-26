import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { BROKERS } from "../helpers.ts";

describe("Observability (real broker)", () => {
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
});
