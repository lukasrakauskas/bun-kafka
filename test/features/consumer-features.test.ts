import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Writer, decodeRecordSet, encodeRecordBatch } from "../../src/bun/protocol.ts";
import type { AbortedTransaction } from "../../src/types.ts";
import { RecordSetDecoder } from "../../src/bun/protocol.ts";

const apiVersions = () =>
  new Writer().i16(0).array(
    Array.from({ length: 64 }, (_, key) => key),
    (writer, key) => writer.i16(key).i16(0).i16(20),
  );

function decode(value: Uint8Array | null | unknown): string | null {
  return value == null
    ? null
    : typeof value === "string"
      ? value
      : new TextDecoder().decode(value as Uint8Array);
}

describe("Static group membership", () => {
  test("sends group.instance.id through JoinGroup, SyncGroup, Heartbeat, and LeaveGroup", async () => {
    const versions = new Map<number, number[]>();
    let heartbeats = 0;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          let offset = 0;
          while (offset < request.byteLength) {
            const size = new DataView(request.buffer, request.byteOffset + offset).getInt32(0);
            const frame = request.subarray(offset, offset + 4 + size);
            offset += 4 + size;
            const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
            const key = view.getInt16(4);
            const version = view.getInt16(6);
            const correlation = view.getInt32(8);
            if (key !== 18) versions.set(key, [version]);
            // Parse the static identity position for the group APIs.
            let instanceId: string | null = null;
            if (key === 11 || key === 14 || key === 13 || (key === 12 && version >= 3)) {
              const clientIdLen = new DataView(frame.buffer, frame.byteOffset + 12, 2).getInt16(0);
              const reader = new ReaderShim(frame.subarray(14 + clientIdLen));
              reader.string(); // groupId
              if (key === 11) {
                reader.i32(); // session timeout
                reader.i32(); // rebalance timeout
                reader.string(); // memberId
                instanceId = reader.string();
              } else {
                reader.i32(); // generation id
                reader.string(); // memberId
                instanceId = reader.string();
              }
            }
            void instanceId;
            if (key === 12) {
              heartbeats++;
            }
            const memberMetadata = new Writer()
              .i16(0)
              .array(["events"], (writer, topicName) => writer.string(topicName))
              .bytes(null)
              .result();
            const assignment = new Writer()
              .i16(0)
              .array(["events"], (writer, topicName) =>
                writer.string(topicName).array([0], (item, partition) => item.i32(partition)),
              )
              .bytes(null)
              .result();
            let body: Writer;
            if (key === 18) body = apiVersions();
            else if (key === 10)
              body = new Writer().i16(0).i32(1).string("127.0.0.1").i32(listener.port);
            else if (key === 3)
              body = new Writer()
                .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, b) =>
                  writer.i32(b.id).string(b.host).i32(b.port).string(null),
                )
                .string(null)
                .i32(1)
                .array([{ name: "events" }], (writer, item) =>
                  writer
                    .i16(0)
                    .string(item.name)
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
            else if (key === 11)
              body = new Writer()
                .i32(0)
                .i16(0)
                .i32(1)
                .string("range")
                .string("member-static")
                .string("member-static")
                .array(["member-static"], (writer, member) =>
                  writer.string(member).bytes(memberMetadata),
                );
            else if (key === 14) body = new Writer().i32(0).i16(0).bytes(assignment);
            else if (key === 9)
              body = new Writer()
                .array(["events"], (writer, topicName) =>
                  writer
                    .string(topicName)
                    .array([0], (item, partition) =>
                      item.i32(partition).i64(7).string(null).i16(0),
                    ),
                )
                .i16(0);
            else if (key === 8)
              body = new Writer().array(["events"], (writer, topicName) =>
                writer
                  .string(topicName)
                  .array([0], (item, partition) => item.i32(partition).i16(0)),
              );
            else body = new Writer().i16(0);
            const response = new Writer().i32(0).i32(correlation).raw(body.result());
            response.patchI32(0, response.length - 4);
            socket.write(response.result());
          }
        },
      },
    });
    class ReaderShim {
      #view: DataView;
      #bytes: Uint8Array;
      #at = 0;
      constructor(bytes: Uint8Array) {
        this.#bytes = bytes;
        this.#view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      }
      i32() {
        const v = this.#view.getInt32(this.#at);
        this.#at += 4;
        return v;
      }
      i16() {
        const v = this.#view.getInt16(this.#at);
        this.#at += 2;
        return v;
      }
      string(): string | null {
        const length = this.i16();
        if (length < 0) return null;
        const out = new TextDecoder().decode(this.#bytes.subarray(this.#at, this.#at + length));
        this.#at += length;
        return out;
      }
    }
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      retry: { initialBackoffMs: 0, maxBackoffMs: 0 },
    });
    try {
      const consumer = kafka.consumer({
        groupId: "workers",
        groupInstanceId: "worker-a",
        heartbeatIntervalMs: 30,
      });
      await consumer.subscribe("events");
      expect(consumer.assignment()).toEqual([{ topic: "events", partition: 0, offset: 7n }]);
      await consumer.commitOffsets();
      await Bun.sleep(120); // allow one heartbeat cycle
      await consumer.close();
      expect(versions.get(11)).toEqual([3]); // JoinGroup v3
      expect(versions.get(14)).toEqual([3]); // SyncGroup v3
      expect(heartbeats).toBeGreaterThan(0);
      expect(versions.get(12)).toEqual([3]); // Heartbeat v3
      expect(versions.get(13)).toEqual([3]); // LeaveGroup v3
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("dynamic members keep the legacy protocol versions", async () => {
    const versions = new Map<number, number[]>();
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key !== 18 && key !== 3) versions.set(key, [view.getInt16(6)]);
          const metadata = new Writer()
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) =>
              writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
            )
            .string(null)
            .i32(1)
            .array([{ name: "events" }], (writer, item) =>
              writer
                .i16(0)
                .string(item.name)
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
          const memberMetadata = new Writer()
            .i16(0)
            .array(["events"], (writer, t) => writer.string(t))
            .bytes(null)
            .result();
          const assignment = new Writer()
            .i16(0)
            .array(["events"], (writer, t) => writer.string(t).array([0], (item, p) => item.i32(p)))
            .bytes(null)
            .result();
          const body =
            key === 18
              ? apiVersions()
              : key === 10
                ? new Writer().i16(0).i32(1).string("127.0.0.1").i32(listener.port)
                : key === 3
                  ? metadata
                  : key === 11
                    ? new Writer()
                        .i32(0)
                        .i16(0)
                        .i32(1)
                        .string("range")
                        .string("m1")
                        .string("m1")
                        .array(["m1"], (w, m) => w.string(m).bytes(memberMetadata))
                    : key === 14
                      ? new Writer().i16(0).bytes(assignment)
                      : key === 9
                        ? new Writer()
                            .array(["events"], (w, t) =>
                              w
                                .string(t)
                                .array([0], (item, p) => item.i32(p).i64(7).string(null).i16(0)),
                            )
                            .i16(0)
                        : key === 8
                          ? new Writer().array(["events"], (w, t) =>
                              w.string(t).array([0], (item, p) => item.i32(p).i16(0)),
                            )
                          : new Writer().i16(0);
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const consumer = kafka.consumer({ groupId: "workers" });
      await consumer.subscribe("events");
      await consumer.close();
      expect(versions.get(11)).toEqual([2]);
      expect(versions.get(14)).toEqual([0]);
      expect(versions.get(13)).toEqual([0]);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});

describe("Read-committed isolation", () => {
  test("RecordSetDecoder filters aborted transaction ranges until the commit marker", () => {
    const aborted: AbortedTransaction[] = [{ producerId: 77n, firstOffset: 4n }];
    const batchA = encodeRecordBatch(Array.from({ length: 4 }, (_, i) => ({ value: `keep-${i}` })));
    const batchB = encodeRecordBatch(
      Array.from({ length: 4 }, (_, i) => ({ value: `drop-${i}` })),
      Date.now(),
      "none",
      { id: 77n, epoch: 0, sequence: 0 },
      4n,
    );
    const combined = new Uint8Array(batchA.byteLength + batchB.byteLength);
    combined.set(batchA);
    combined.set(batchB, batchA.byteLength);
    const decoder = new RecordSetDecoder(combined, "tx", 0, 1, {
      minOffset: 0n,
      abortedTransactions: aborted,
    });
    const messages = decoder.read();
    expect(messages.map((message) => decode(message.value))).toEqual([
      "keep-0",
      "keep-1",
      "keep-2",
      "keep-3",
    ]);
  });

  test("commit markers resume visibility for later batches of the same producer", () => {
    const aborted: AbortedTransaction[] = [{ producerId: 9n, firstOffset: 2n }];
    // Batch 1 (producer 9): offsets 0-1 visible, 2-3 aborted.
    const batch1 = encodeRecordBatch(
      [{ value: "a" }, { value: "b" }, { value: "c" }, { value: "d" }],
      Date.now(),
      "none",
      { id: 9n, epoch: 0, sequence: 0 },
    );
    // Control batch carrying a COMMIT marker (value first byte 1) at offset 4.
    const commitMarker = encodeRecordBatch(
      [{ value: new Uint8Array([1]) }],
      Date.now(),
      "none",
      { id: 9n, epoch: 0, sequence: 4, control: true },
      4n,
    );
    // Post-commit data is visible again.
    const batch3 = encodeRecordBatch(
      [{ value: "e" }, { value: "f" }],
      Date.now(),
      "none",
      { id: 9n, epoch: 0, sequence: 5 },
      5n,
    );
    const combined = new Uint8Array(
      batch1.byteLength + commitMarker.byteLength + batch3.byteLength,
    );
    let at = 0;
    for (const part of [batch1, commitMarker, batch3]) {
      combined.set(part, at);
      at += part.byteLength;
    }
    const messages = new RecordSetDecoder(combined, "tx", 0, 1, {
      minOffset: 0n,
      abortedTransactions: aborted,
    }).read();
    expect(messages.map((message) => decode(message.value))).toEqual(["a", "b", "e", "f"]);
  });

  test("other producers stay visible inside aborted ranges", () => {
    const aborted: AbortedTransaction[] = [{ producerId: 5n, firstOffset: 0n }];
    const batch = encodeRecordBatch([{ value: "clean" }], Date.now(), "none", {
      id: 6n,
      epoch: 0,
      sequence: 0,
    });
    const messages = new RecordSetDecoder(batch, "tx", 0, 1, {
      minOffset: 0n,
      abortedTransactions: aborted,
    }).read();
    expect(messages.map((message) => decode(message.value))).toEqual(["clean"]);
  });

  test("fetch requests declare isolation level 1 under read_committed", async () => {
    let isolationByte: number | undefined;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key === 1) {
            // Fetch v4 body starts after the request header incl. client_id string.
            let at = 12 + view.getInt16(12) + 2; // skip client_id string
            at += 16; // replica_id + max_wait_ms + min_bytes + max_bytes (isolation sits before session fields)
            isolationByte = new DataView(request.buffer, request.byteOffset + at, 1).getInt8(0);
          }
          const body =
            key === 18
              ? apiVersions()
              : key === 3
                ? new Writer()
                    .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) =>
                      writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
                    )
                    .string(null)
                    .i32(1)
                    .array([{ name: "events" }], (writer, item) =>
                      writer
                        .i16(0)
                        .string(item.name)
                        .bool(false)
                        .array([0], (pw) =>
                          pw
                            .i16(0)
                            .i32(0)
                            .i32(1)
                            .array([1], (w) => w.i32(1))
                            .array([1], (w) => w.i32(1)),
                        ),
                    )
                : key === 2
                  ? new Writer().array(["events"], (w, t) =>
                      w
                        .string(t)
                        .array([0], (p, partition) => p.i32(partition).i16(0).i64(0).i64(0)),
                    )
                  : new Writer()
                      .i32(0) // throttle_time_ms (Fetch v7 response)
                      .i16(0) // top-level error code
                      .i32(0) // session id
                      .array(["events"], (topicWriter) =>
                        topicWriter.string("events").array([0], (partitionWriter) =>
                          partitionWriter
                            .i32(0)
                            .i16(0)
                            .i64(0)
                            .i64(1)
                            .i64(0)
                            .array([], () => {})
                            .bytes(null),
                        ),
                      );
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const consumer = kafka.consumer({ isolationLevel: "read_committed" });
      await consumer.assign([{ topic: "events", partition: 0, offset: 0n }]);
      const messages = await consumer.fetch({ maxWaitMs: 50 });
      expect(messages).toHaveLength(0);
      expect(isolationByte).toBe(1);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});

describe("Deserializers", () => {
  test("decodeRecordSet stays independent of deserialization", () => {
    const batch = encodeRecordBatch([{ key: "k", value: "v" }]);
    expect(decode(decodeRecordSet(batch, "t", 0, 1)[0]!.value)).toBe("v");
  });

  test("value deserializer types fetched messages without casts", async () => {
    const records = encodeRecordBatch([{ value: JSON.stringify({ seq: 1 }) }]);
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          const body =
            key === 18
              ? apiVersions()
              : key === 3
                ? new Writer()
                    .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, broker) =>
                      writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
                    )
                    .string(null)
                    .i32(1)
                    .array([{ name: "events" }], (writer, item) =>
                      writer
                        .i16(0)
                        .string(item.name)
                        .bool(false)
                        .array([0], (pw) =>
                          pw
                            .i16(0)
                            .i32(0)
                            .i32(1)
                            .array([1], (w) => w.i32(1))
                            .array([1], (w) => w.i32(1)),
                        ),
                    )
                : key === 2
                  ? new Writer().array(["events"], (w, t) =>
                      w.string(t).array([0], (p, partition) => p.i32(partition).i64(0)),
                    )
                  : new Writer()
                      .i32(0) // Fetch v7
                      .i16(0)
                      .i32(0)
                      .array(["events"], (topicWriter) =>
                        topicWriter.string("events").array([0], (partitionWriter) =>
                          partitionWriter
                            .i32(0)
                            .i16(0)
                            .i64(1)
                            .i64(1)
                            .i64(0)
                            .array([], () => {})
                            .bytes(records),
                        ),
                      );
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const consumer = kafka.consumer({
        valueDeserializer: (data): { seq?: number } | null =>
          data === null ? null : JSON.parse(new TextDecoder().decode(data)),
      });
      await consumer.assign([{ topic: "events", partition: 0, offset: 0n }]);
      const [message] = await consumer.fetch({ maxWaitMs: 50 });
      expect(message).toBeDefined();
      const value: { seq?: number } | null = message!.value;
      expect(value?.seq).toBe(1);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
