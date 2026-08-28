import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Writer } from "../../src/bun/protocol.ts";

const apiVersions = () =>
  new Writer().i16(0).array(
    Array.from({ length: 64 }, (_, key) => key),
    (writer, key) => writer.i16(key).i16(0).i16(20),
  );

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
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, b) =>
              writer.i32(b.id).string(b.host).i32(b.port).string(null),
            )
            .string(null)
            .i32(1)
            .array([{ name: "events" }], (writer, t) =>
              writer
                .i16(0)
                .string(t.name)
                .bool(false)
                .array([0], (pw) =>
                  pw
                    .i16(0)
                    .i32(0)
                    .i32(1)
                    .array([1], (w) => w.i32(1))
                    .array([1], (w) => w.i32(1)),
                ),
            );
          let body: Writer;
          if (key === 18) {
            body = apiVersions();
          } else if (key === 10) {
            body = new Writer()
              .i32(0)
              .i16(0)
              .string(null)
              .i32(1)
              .string("127.0.0.1")
              .i32(listener.port);
          } // FindCoordinator v2: txn coordinator is this mock broker
          else if (key === 3) {
            body = metadata;
          } else if (key === 22) {
            body = new Writer().i32(0).i16(0).i64(500).i16(1);
          } // pid=500 epoch=1
          else if (key === 24) {
            body = new Writer()
              .i32(0)
              .array(["events"], (writer, t) =>
                writer.string(t).array([0], (p) => p.i32(p.index ?? 0).i16(0)),
              );
          } else if (key === 26) {
            // EndTxn body starts after header + client_id string; commit flag follows txn_id string + pid i64 + epoch i16.
            const clientIdLen = view.getInt16(12);
            const bodyStart = 14 + clientIdLen;
            const txnIdLen = new DataView(
              request.buffer,
              request.byteOffset + bodyStart,
              2,
            ).getInt16(0);
            const at = bodyStart + 2 + txnIdLen + 8 + 2;
            apiCalls.push({
              key,
              flags: [new DataView(request.buffer, request.byteOffset + at, 1).getInt8(0)],
            });
            body = new Writer().i32(0).i16(0);
          } else if (key === 28) {
            body = new Writer().i32(0).i16(0);
          } // TxnOffsetCommit
          else if (key === 0) {
            body = new Writer()
              .i32(0)
              .array(["events"], (writer, t) =>
                writer.string(t).array([0], (p) => p.i32(0).i16(0).i64(10).i64(-1)),
              );
          } else {
            body = new Writer().i32(0).i16(0);
          }
          if (!apiCalls.some((c) => c.key === key)) {
            apiCalls.push({ key });
          }
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
      expect(apiCalls.map((call) => call.key)).toEqual(expect.arrayContaining([22, 24, 0, 26]));
      expect(apiCalls.find((call) => call.key === 26)?.flags).toEqual([1]);
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
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, b) =>
              writer.i32(b.id).string(b.host).i32(b.port).string(null),
            )
            .string(null)
            .i32(1)
            .array([{ name: "events" }], (writer, t) =>
              writer
                .i16(0)
                .string(t.name)
                .bool(false)
                .array([0], (pw) =>
                  pw
                    .i16(0)
                    .i32(0)
                    .i32(1)
                    .array([1], (w) => w.i32(1))
                    .array([1], (w) => w.i32(1)),
                ),
            );
          let body: Writer;
          if (key === 18) {
            body = apiVersions();
          } else if (key === 10) {
            body = new Writer()
              .i32(0)
              .i16(0)
              .string(null)
              .i32(1)
              .string("127.0.0.1")
              .i32(listener.port);
          } // FindCoordinator v2: txn coordinator is this mock broker
          else if (key === 3) {
            body = metadata;
          } else if (key === 22) {
            body = new Writer().i32(0).i16(0).i64(500).i16(1);
          } else if (key === 24) {
            body = new Writer()
              .i32(0)
              .array(["events"], (writer, t) =>
                writer.string(t).array([0], (p) => p.i32(0).i16(0)),
              );
          } else if (key === 0) {
            body = new Writer()
              .i32(0)
              .array(["events"], (writer, t) =>
                writer.string(t).array([0], (p) => p.i32(0).i16(0).i64(10).i64(-1)),
              );
          } else if (key === 26) {
            const clientIdLen = view.getInt16(12);
            const bodyStart = 14 + clientIdLen;
            const txnIdLen = new DataView(
              request.buffer,
              request.byteOffset + bodyStart,
              2,
            ).getInt16(0);
            const at = bodyStart + 2 + txnIdLen + 8 + 2;
            endTxnFlags.push(new DataView(request.buffer, request.byteOffset + at, 1).getInt8(0));
            body = new Writer().i32(0).i16(0);
          } else {
            body = new Writer().i32(0).i16(0);
          }
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
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, b) =>
              writer.i32(b.id).string(b.host).i32(b.port).string(null),
            )
            .string(null)
            .i32(1)
            .array([{ name: "events" }], (writer, t) =>
              writer
                .i16(0)
                .string(t.name)
                .bool(false)
                .array([0], (pw) =>
                  pw
                    .i16(0)
                    .i32(0)
                    .i32(1)
                    .array([1], (w) => w.i32(1))
                    .array([1], (w) => w.i32(1)),
                ),
            );
          let body: Writer;
          if (key === 18) {
            body = apiVersions();
          } else if (key === 10) {
            body = new Writer()
              .i32(0)
              .i16(0)
              .string(null)
              .i32(1)
              .string("127.0.0.1")
              .i32(listener.port);
          } // FindCoordinator v2: txn coordinator is this mock broker
          else if (key === 3) {
            body = metadata;
          } else if (key === 22) {
            body = new Writer().i32(0).i16(0).i64(9).i16(2);
          } else if (key === 25 || key === 26 || key === 28) {
            seenKeys.push(key);
            body =
              key === 28
                ? new Writer()
                    .i32(0)
                    .array(["events"], (w, t) => w.string(t).array([0], (p) => p.i32(0).i16(0)))
                : new Writer().i32(0).i16(0);
          } else {
            body = new Writer().i32(0).i16(0);
          }
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
      await producer.sendOffsetsToTransaction(
        [{ topic: "events", partition: 0, offset: 42n }],
        "workers",
      );
      expect(seenKeys).toEqual([25, 28]);
      await producer.commitTransaction();
      expect(seenKeys).toContain(26);
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
    expect(() => producer.send({ topic: "x", acks: 0, messages: [{ value: "y" }] })).toThrow(
      TypeError,
    );
  });
});
