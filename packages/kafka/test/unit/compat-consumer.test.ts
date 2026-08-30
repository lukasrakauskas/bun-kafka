import { expect, test } from "bun:test";
import type { ClusterGetter } from "../../src/kafkajs-compat/config.ts";
import { Logger } from "../../src/kafkajs-compat/logger.ts";
import { CompatConsumer, type RunOptions } from "../../src/kafkajs-compat/consumer.ts";
import { Cluster } from "../../src/bun/cluster.ts";

class FailingCluster extends Cluster {
  override async anyRequest(): Promise<never> {
    throw new Error("describe failed");
  }
}

class HandlerFailingConsumer extends CompatConsumer {
  protected override async loop(options: RunOptions): Promise<void> {
    await options.eachMessage?.({} as never);
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

test("run reports a handler crash once", async () => {
  const cluster = new FailingCluster({ brokers: ["127.0.0.1:1"] });
  const getter: ClusterGetter = {
    ready: async () => cluster,
    sync: () => cluster,
    acquire: () => cluster,
    release: () => {},
  };
  const logs: string[] = [];
  const consumer = new HandlerFailingConsumer(
    () => getter,
    new Logger(1, "test", ({ log }) => logs.push(String(log.message))),
    { groupId: "workers" },
  );
  const eventErrors: Error[] = [];
  const callbackErrors: Error[] = [];
  const crashed = Promise.withResolvers<void>();
  consumer.on(consumer.events.CRASH, ({ error }) => {
    if (error instanceof Error) eventErrors.push(error);
  });

  await consumer.run({
    eachMessage: async () => {
      throw new Error("handler failed");
    },
    onCrash: (error) => {
      callbackErrors.push(error);
      crashed.resolve();
    },
  });
  await crashed.promise;

  expect(logs).toEqual(["consumer crashed: handler failed"]);
  expect(eventErrors).toHaveLength(1);
  expect(callbackErrors).toHaveLength(1);
  expect(callbackErrors[0]).toMatchObject({ name: "KafkaJSError", message: "handler failed" });
  expect(eventErrors[0]).toBe(callbackErrors[0]);
});
