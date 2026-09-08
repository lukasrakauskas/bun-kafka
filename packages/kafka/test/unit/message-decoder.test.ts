import { expect, spyOn, test } from "bun:test";
import { MessageDecoder } from "../../src/consumer/message-decoder.ts";
import { partitionKey } from "../../src/bun/shared.ts";
import { encodeRecordBatch, RecordSetDecoder } from "../../src/protocol/index.ts";

const topic = "events";
const key = partitionKey(topic, 0);
const base = 9007199254740993n;

function batch(partition: number, offset: bigint, values = ["a", "b", "c"]) {
  const bytes = encodeRecordBatch(
    values.map((value) => ({ value })),
    1000,
    "none",
    undefined,
    offset,
  );
  return { bytes, decoder: new RecordSetDecoder(bytes, topic, partition, 1) };
}

test("raw draining updates positions once per partition fragment and preserves payload ownership", () => {
  const positions = new Map<string, bigint>();
  const updates = spyOn(positions, "set");
  const decoder = new MessageDecoder<Uint8Array | null, Uint8Array | null>({}, positions);
  const first = batch(0, base);
  const second = batch(1, 10n);
  decoder.add([first.decoder, second.decoder]);
  try {
    const head = decoder.drain(2);
    expect(head.map((message) => message.offset)).toEqual([base, base + 1n]);
    expect(positions.get(key)).toBe(base + 2n);
    expect(updates).toHaveBeenCalledTimes(1);
    expect(decoder.pending).toBe(true);
    const tail = decoder.drain(10);
    expect(tail.map(({ partition, offset }) => [partition, offset])).toEqual([
      [0, base + 2n],
      [1, 10n],
      [1, 11n],
      [1, 12n],
    ]);
    expect(updates).toHaveBeenCalledTimes(3);
    expect(positions.get(key)).toBe(base + 3n);
    expect(positions.get(partitionKey(topic, 1))).toBe(13n);
    expect(decoder.pending).toBe(false);
    expect(head[0].value?.buffer).toBe(first.bytes.buffer);
    decoder.clear();
    expect(new TextDecoder().decode(head[0].value ?? undefined)).toBe("a");
    expect(decoder.drain(1)).toEqual([]);
    expect(updates).toHaveBeenCalledTimes(3);
  } finally {
    updates.mockRestore();
  }
});

test("filtered and corrupt record sets do not advance positions", () => {
  const positions = new Map([[key, base + 10n]]);
  const decoder = new MessageDecoder({}, positions);
  const { bytes } = batch(0, base);
  decoder.add([new RecordSetDecoder(bytes, topic, 0, 1, { minOffset: base + 10n })]);
  expect(decoder.drain(10)).toEqual([]);
  expect(positions.get(key)).toBe(base + 10n);
  expect(decoder.pending).toBe(false);
  bytes[bytes.length - 1] ^= 1;
  decoder.add([new RecordSetDecoder(bytes, topic, 0, 1)]);
  expect(() => decoder.drain(10)).toThrow(/CRC mismatch/u);
  expect(positions.get(key)).toBe(base + 10n);
});

test("deserializers still observe each position update, including the record that throws", () => {
  const positions = new Map<string, bigint>();
  const observed: Array<bigint | undefined> = [];
  const decoder = new MessageDecoder(
    {
      valueDeserializer: (value, context) => {
        observed.push(positions.get(key));
        if (context.offset === base + 1n) throw new Error("bad value");
        return value;
      },
    },
    positions,
  );
  decoder.add([batch(0, base).decoder]);
  expect(() => decoder.drain(10)).toThrow("bad value");
  expect(observed).toEqual([base + 1n, base + 2n]);
  expect(positions.get(key)).toBe(base + 2n);
});
