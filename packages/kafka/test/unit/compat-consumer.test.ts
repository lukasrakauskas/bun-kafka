import { expect, test } from "bun:test";
import type { ClusterGetter } from "../../src/kafkajs-compat/config.ts";
import { Logger } from "../../src/kafkajs-compat/logger.ts";
import { CompatConsumer } from "../../src/kafkajs-compat/consumer.ts";
import { Cluster } from "../../src/bun/cluster.ts";

class FailingCluster extends Cluster {
  override async anyRequest(): Promise<never> {
    throw new Error("describe failed");
  }
}

test("describeGroup releases its temporary admin after a failed request", async () => {
  const cluster = new FailingCluster({ brokers: ["127.0.0.1:1"] });
  let releases = 0;
  const getter: ClusterGetter = {
    ready: async () => cluster,
    sync: () => cluster,
    acquire: () => cluster,
    release: () => releases++,
  };
  const consumer = new CompatConsumer(() => getter, new Logger(0, "test"), { groupId: "workers" });

  await expect(consumer.describeGroup()).rejects.toMatchObject({
    name: "KafkaJSError",
    message: "describe failed",
  });
  expect(releases).toBe(1);
});
