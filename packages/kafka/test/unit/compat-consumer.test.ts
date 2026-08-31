import { expect, test } from "bun:test";
import type { ClusterGetter } from "../../src/kafkajs-compat/config.ts";
import { Logger } from "../../src/kafkajs-compat/logger.ts";
import { CompatConsumer, type RunOptions } from "../../src/kafkajs-compat/consumer.ts";
import type { Consumer } from "../../src/consumer/index.ts";
import type { TopicPartition } from "../../src/types.ts";
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

class PartitionTrackingConsumer extends CompatConsumer {
  readonly corePaused = new Set<number>();

  protected override underlying(): Consumer {
    return {
      assignment: () => [
        { topic: "events", partition: 0 },
        { topic: "events", partition: 1 },
      ],
      pause: (partitions: TopicPartition[]) => {
        for (const { partition } of partitions) this.corePaused.add(partition);
      },
      resume: (partitions: TopicPartition[]) => {
        for (const { partition } of partitions) this.corePaused.delete(partition);
      },
    } as unknown as Consumer;
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

test("pause and resume pass resolved partitions to the core consumer", () => {
  const consumer = new PartitionTrackingConsumer(
    () => {
      throw new Error("unexpected cluster access");
    },
    new Logger(0, "test"),
    { groupId: "workers" },
  );

  expect(consumer.pause([{ topic: "events", partitions: [1] }])).toEqual([
    { topic: "events", partitions: [1] },
  ]);
  expect([...consumer.corePaused]).toEqual([1]);

  expect(consumer.resume([{ topic: "events", partitions: [1] }])).toEqual([]);
  expect([...consumer.corePaused]).toEqual([]);

  expect(consumer.pause([{ topic: "events" }])).toEqual([{ topic: "events", partitions: [0, 1] }]);
  expect([...consumer.corePaused]).toEqual([0, 1]);

  expect(consumer.resume([{ topic: "events", partitions: [1] }])).toEqual([
    { topic: "events", partitions: [0] },
  ]);
  expect([...consumer.corePaused]).toEqual([0]);
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
