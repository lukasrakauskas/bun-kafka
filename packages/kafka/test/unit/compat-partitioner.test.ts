import { expect, test } from "bun:test";
import { Partitioners, toBunPartitioner } from "../../src/kafkajs-compat/index.ts";
import type { KafkaJsPartitioner } from "../../src/kafkajs-compat/messages.ts";

const key = new TextEncoder().encode("same-key");

test("JavaCompatiblePartitioner keeps Murmur2 key affinity", () => {
  const partition = toBunPartitioner(Partitioners.JavaCompatiblePartitioner);

  expect(partition?.({ topic: "events", partitionCount: 12, key })).toBe(6);
  expect(partition?.({ topic: "events", partitionCount: 12, key })).toBe(6);
});

test("built-in partitioner factories return valid keyless partitions", () => {
  for (const factory of Object.values(Partitioners)) {
    const partition = toBunPartitioner(factory);
    const selected = partition?.({ topic: "events", partitionCount: 3, key: null });
    expect(Number.isInteger(selected)).toBe(true);
    expect(selected).toBeGreaterThanOrEqual(0);
    expect(selected).toBeLessThan(3);
  }
});

test("custom KafkaJS partitioner factories receive KafkaJS callback context", () => {
  let factoryCalls = 0;
  const factory: KafkaJsPartitioner = () => {
    factoryCalls++;
    return ({ topic, partitionMetadata, message }) => {
      expect(topic).toBe("events");
      expect(partitionMetadata.map(({ partitionId }) => partitionId)).toEqual([0, 1, 2]);
      expect(message.key).toEqual(key);
      return 2;
    };
  };

  const partition = toBunPartitioner(factory);

  expect(factoryCalls).toBe(1);
  expect(partition?.({ topic: "events", partitionCount: 3, key })).toBe(2);
  expect(factoryCalls).toBe(1);
});
