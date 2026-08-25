import { describe, expect, test } from "bun:test";
import { Kafka } from "../index.ts";
import { Writer } from "../src/bun/protocol.ts";
import { BROKERS, dec, topic } from "./helpers.ts";

const apiVersions = () => new Writer().i16(0).array(Array.from({ length: 64 }, (_, key) => key), (writer, key) => writer.i16(key).i16(0).i16(20));

describe("Transactions (mock broker)", () => {
  test("commit flow sends InitProducerId, AddPartitionsToTxn, Produce, EndTxn", async () => {
    const apiCalls: Array<{ key: number; flags?: number[] }> = [];
    let txnPartitionAdded = false;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          const metadata = new Writer()
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, b) => writer.i32(b.id).string(b.host).i32(b.port).string(null))
            .string(null)
            .i32(1)
            .array([{ name: "events" }], (writer, t) => writer.i16(0).string(t.name).bool(false).array([0], (pw) => pw.i16(0).i32(0).i32(1).array([1], (w) => w.i32(1)).array([1], (w) => w.i32(1))));
          let body: Writer;
          if (key === 18) body = apiVersions();
          else if (key === 3) body = metadata;
          else if (key === 22) body = new Writer().i32(0).i16(0).i64(500).i16(1); // pid=500 epoch=1
          else if (key === 24) body = new Writer().i32(0).array(["events"], (writer, t) => writer.string(t).array([0], (p) => p.i32(p.index ?? 0).i16(0)));
          else if (key === 28) {
            // EndTxn body starts after header + client_id string; commit flag follows txn_id string + pid i64 + epoch i16.
            const clientIdLen = view.getInt16(12);
            const bodyStart = 14 + clientIdLen;
            const txnIdLen = new DataView(request.buffer, request.byteOffset + bodyStart, 2).getInt16(0);
            const at = bodyStart + 2 + txnIdLen + 8 + 2;
            apiCalls.push({ key, flags: [new DataView(request.buffer, request.byteOffset + at, 1).getInt8(0)] });
            body = new Writer().i32(0).i16(0);
          } else if (key === 0) {
            body = new Writer().i32(0).array(["events"], (writer, t) => writer.string(t).array([0], (p) => p.i32(0).i16(0).i64(10).i64(-1)));
          } else body = new Writer().i32(0).i16(0);
          if (!apiCalls.some((c) => c.key === key)) apiCalls.push({ key });
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    void txnPartitionAdded;
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const producer = kafka.producer({ transactionalId: "txn-producer-1" });
      await producer.beginTransaction();
      await producer.send({ topic: "events", messages: [{ value: "one" }] });
      await producer.commitTransaction();
      expect(apiCalls.map((call) => call.key)).toEqual(expect.arrayContaining([22, 24, 0, 28]));
      expect(apiCalls.find((call) => call.key === 28)?.flags).toEqual([1]);
      await producer.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("abort sends EndTxn with commit=false", async () => {
    const endTxnFlags: number[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          const metadata = new Writer()
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, b) => writer.i32(b.id).string(b.host).i32(b.port).string(null))
            .string(null)
            .i32(1)
            .array([{ name: "events" }], (writer, t) => writer.i16(0).string(t.name).bool(false).array([0], (pw) => pw.i16(0).i32(0).i32(1).array([1], (w) => w.i32(1)).array([1], (w) => w.i32(1))));
          let body: Writer;
          if (key === 18) body = apiVersions();
          else if (key === 3) body = metadata;
          else if (key === 22) body = new Writer().i32(0).i16(0).i64(500).i16(1);
          else if (key === 24) body = new Writer().i32(0).array(["events"], (writer, t) => writer.string(t).array([0], (p) => p.i32(0).i16(0)));
          else if (key === 0) body = new Writer().i32(0).array(["events"], (writer, t) => writer.string(t).array([0], (p) => p.i32(0).i16(0).i64(10).i64(-1)));
          else if (key === 28) {
            const clientIdLen = view.getInt16(12);
            const bodyStart = 14 + clientIdLen;
            const txnIdLen = new DataView(request.buffer, request.byteOffset + bodyStart, 2).getInt16(0);
            const at = bodyStart + 2 + txnIdLen + 8 + 2;
            endTxnFlags.push(new DataView(request.buffer, request.byteOffset + at, 1).getInt8(0));
            body = new Writer().i32(0).i16(0);
          } else body = new Writer().i32(0).i16(0);
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const producer = kafka.producer({ transactionalId: "txn-producer-2" });
      await producer.beginTransaction();
      await producer.send({ topic: "events", messages: [{ value: "lost" }] });
      await producer.abortTransaction();
      expect(endTxnFlags).toEqual([0]);
      await producer.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("sendOffsetsToTransaction registers the group and commits offsets", async () => {
    const seenKeys: number[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          const metadata = new Writer()
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, b) => writer.i32(b.id).string(b.host).i32(b.port).string(null))
            .string(null)
            .i32(1)
            .array([{ name: "events" }], (writer, t) => writer.i16(0).string(t.name).bool(false).array([0], (pw) => pw.i16(0).i32(0).i32(1).array([1], (w) => w.i32(1)).array([1], (w) => w.i32(1))));
          let body: Writer;
          if (key === 18) body = apiVersions();
          else if (key === 3) body = metadata;
          else if (key === 22) body = new Writer().i32(0).i16(0).i64(9).i16(2);
          else if (key === 25 || key === 26 || key === 28) {
            seenKeys.push(key);
            body = key === 26
              ? new Writer().i32(0).array(["events"], (w, t) => w.string(t).array([0], (p) => p.i32(0).i16(0)))
              : new Writer().i32(0).i16(0);
          } else body = new Writer().i32(0).i16(0);
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const producer = kafka.producer({ transactionalId: "txn-producer-3" });
      await producer.beginTransaction();
      await producer.sendOffsetsToTransaction([{ topic: "events", partition: 0, offset: 42n }], "workers");
      expect(seenKeys).toEqual([25, 26]);
      await producer.commitTransaction();
      expect(seenKeys).toContain(28);
      await producer.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("transaction methods reject without a transactionalId", () => {
    const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
    const producer = kafka.producer();
    expect(() => producer.beginTransaction()).toThrow(/transactionalId/);
  });

  test("acks=0 is rejected for transactional producers", () => {
    const kafka = new Kafka({ brokers: ["127.0.0.1:9092"] });
    const producer = kafka.producer({ transactionalId: "t" });
    expect(() => producer.send({ topic: "x", acks: 0, messages: [{ value: "y" }] })).toThrow(TypeError);
  });
});

describe("Transactions (real broker)", () => {
  test("committed transactions are visible under read_committed", async () => {
    const name = topic("txn");
    const client = kafka(BROKERS.split(","));
    try {
      const admin = client.admin();
      await admin.createTopics([{ name, numPartitions: 1 }]);
      await admin.close();

      const producer = client.producer({ transactionalId: `txn-${name}` });
      await producer.beginTransaction();
      await producer.send({ topic: name, messages: [{ key: "k1", value: "v1" }, { key: "k2", value: "v2" }] });
      await producer.commitTransaction();

      const consumer = client.consumer({ isolationLevel: "read_committed" });
      await consumer.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const messages = await consumer.fetch({ maxWaitMs: 5_000, maxMessages: 5, copy: true });
      expect(messages.map((m) => dec(m.value))).toEqual(["v1", "v2"]);
      await consumer.close();
      await producer.close();
    } finally {
      await client.disconnect();
    }
  }, 60_000);

  test("aborted transactions are invisible under read_committed but visible uncommitted", async () => {
    const name = topic("txna");
    const committedName = topic("txnc");
    const client = kafka(BROKERS.split(","));
    try {
      const admin = client.admin();
      await admin.createTopics([{ name, numPartitions: 1 }, { name: committedName, numPartitions: 1 }]);
      await admin.close();

      // First commit a baseline record so offsets advance.
      const producer = client.producer({ transactionalId: `txn-${committedName}` });
      await producer.beginTransaction();
      await producer.send({ topic: name, messages: [{ value: "committed-record" }] });
      await producer.commitTransaction();

      // Then send an aborted record.
      const aborter = client.producer({ transactionalId: `txna-${name}` });
      await aborter.beginTransaction();
      await aborter.send({ topic: name, messages: [{ value: "aborted-record" }] });
      await aborter.abortTransaction();

      const committed = client.consumer({ isolationLevel: "read_committed" });
      await committed.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const visible = await committed.fetch({ maxWaitMs: 5_000, maxMessages: 10, copy: true });
      expect(visible.map((m) => dec(m.value))).toEqual(["committed-record"]);

      const uncommitted = client.consumer({ isolationLevel: "read_uncommitted" });
      await uncommitted.assign([{ topic: name, partition: 0, offset: "earliest" }]);
      const all = await uncommitted.fetch({ maxWaitMs: 5_000, maxMessages: 10, copy: true });
      expect(all.some((m) => m.offset >= visible[visible.length - 1]!.offset + 1n)).toBe(true);

      await committed.close();
      await uncommitted.close();
      await producer.close();
      await aborter.close();
    } finally {
      await client.disconnect();
    }
  }, 60_000);
});

function kafka(brokers: string[]): Kafka {
  return new Kafka({ brokers });
}
