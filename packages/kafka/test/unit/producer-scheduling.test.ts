import { expect, spyOn, test } from "bun:test";
import { Producer } from "../../index.ts";
import { ProducerSender } from "../../src/bun/producer/sender.ts";

const result = { topic: "events", partition: 0, baseOffset: 0n, logAppendTime: 0n };
const response = { results: [result], routedPartitions: [] };

for (const lingerMs of [0, 1000]) {
  test(`one automatic flush per send burst with lingerMs=${lingerMs}`, async () => {
    const sender = spyOn(ProducerSender.prototype, "producePendingGroup").mockResolvedValue(
      response,
    );
    const producer = new Producer({ brokers: ["127.0.0.1:1"] }, { lingerMs, batchMaxMessages: 10 });
    const flush = spyOn(producer, "flush");
    try {
      for (let burst = 0; burst < 2; burst++) {
        const sends = Array.from({ length: 100 }, (_, i) =>
          producer.send({ topic: "events", messages: [{ value: String(i) }] }),
        );
        expect(producer.queuedMessages).toBe(100);
        expect(await Promise.all(sends)).toEqual(Array.from({ length: 100 }, () => [result]));
        expect(flush).toHaveBeenCalledTimes(burst + 1);
        expect(sender).toHaveBeenCalledTimes(burst + 1);
        expect(sender.mock.calls[burst][0].map(({ input }) => input.messages[0].value)).toEqual(
          Array.from({ length: 100 }, (_, i) => String(i)),
        );
        await Bun.sleep(0);
      }
    } finally {
      await producer.close();
      flush.mockRestore();
      sender.mockRestore();
    }
  });
}

test("linger and explicit flush still drain a below-threshold burst", async () => {
  const sender = spyOn(ProducerSender.prototype, "producePendingGroup").mockResolvedValue(response);
  const producer = new Producer(
    { brokers: ["127.0.0.1:1"] },
    { lingerMs: 10, batchMaxMessages: 100 },
  );
  try {
    const first = producer.send({ topic: "events", messages: [{ value: "timer" }] });
    expect(sender).not.toHaveBeenCalled();
    expect(await first).toEqual([result]);
    await Bun.sleep(0);
    const second = producer.send({ topic: "events", messages: [{ value: "explicit" }] });
    await producer.flush();
    expect(await second).toEqual([result]);
    expect(sender).toHaveBeenCalledTimes(2);
  } finally {
    await producer.close();
    sender.mockRestore();
  }
});

test("a failed burst does not prevent scheduling the next one", async () => {
  const sender = spyOn(ProducerSender.prototype, "producePendingGroup")
    .mockRejectedValueOnce(new Error("failed burst"))
    .mockResolvedValue(response);
  const producer = new Producer({ brokers: ["127.0.0.1:1"] }, { lingerMs: 0 });
  try {
    const failed = await Promise.allSettled(
      Array.from({ length: 100 }, () =>
        producer.send({ topic: "events", messages: [{ value: "failed" }] }),
      ),
    );
    expect(failed.every((item) => item.status === "rejected")).toBe(true);
    await Bun.sleep(0);
    expect(await producer.send({ topic: "events", messages: [{ value: "retry" }] })).toEqual([
      result,
    ]);
    expect(sender).toHaveBeenCalledTimes(2);
  } finally {
    await producer.close();
    sender.mockRestore();
  }
});
