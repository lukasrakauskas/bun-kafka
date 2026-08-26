import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { crc32c, Reader, Writer } from "../../src/bun/protocol.ts";
import { dec } from "../helpers.ts";

const apiVersions = () =>
  new Writer().i16(0).array(
    Array.from({ length: 64 }, (_, key) => key),
    (writer, key) => writer.i16(key).i16(0).i16(20),
  );

type FetchRequestView = {
  sessionId: number;
  sessionEpoch: number;
  topics: Array<{ name: string; partitions: number[] }>;
  forgotten: Array<{ name: string; partitions: number[] }>;
};

function parseFetchV7(
  buffer: ArrayBuffer,
  byteOffset: number,
  byteLength: number,
): FetchRequestView {
  const view = new DataView(buffer, byteOffset, byteLength);
  const clientIdLen = view.getInt16(12);
  const at = 14 + clientIdLen + 25; // header + replica/maxWait/min/max/isolation + session id/epoch
  const sid = view.getInt32(14 + clientIdLen + 17);
  const epoch = view.getInt32(14 + clientIdLen + 21);
  const reader = new Reader(new Uint8Array(buffer, byteOffset + at, byteLength - at));
  const topics = reader.array((r) => {
    const name = r.string() ?? "";
    const partitions = r.array((p) => {
      const index = p.i32(); // partition index (first field)
      p.i64(); // fetch offset
      p.i64(); // log start offset
      p.i32(); // partition max bytes
      return index;
    });
    return { name, partitions };
  });
  const forgotten = reader.array((r) => ({
    name: r.string() ?? "",
    partitions: r.array((p) => p.i32()),
  }));
  return { sessionId: sid, sessionEpoch: epoch, topics, forgotten };
}

/** Build one single-record magic-2 batch containing the given value. */
function singleRecordBatch(value: string, baseOffset: bigint): Uint8Array {
  const records = new Writer();
  const valueBytes = new TextEncoder().encode(value);
  // bodyLength varint + attributes + tsDelta + offsetDelta + key(null) + value + headers(0)
  records
    .varInt(1 + 1 + 1 + 1 + 1 + valueBytes.byteLength + 1)
    .i8(0)
    .varLong(0)
    .varInt(0)
    .varInt(-1)
    .varInt(valueBytes.byteLength)
    .raw(valueBytes)
    .varInt(0);
  const rawRecords = records.result();
  const w = new Writer(61 + rawRecords.byteLength);
  w.i64(baseOffset)
    .i32(0)
    .i32(-1)
    .i8(2)
    .u32(0)
    .i16(0)
    .i32(0)
    .i64(1700000000000n)
    .i64(1700000000000n)
    .i64(-1)
    .i16(-1)
    .i32(-1)
    .i32(1)
    .raw(rawRecords);
  w.patchI32(8, w.length - 12);
  w.patchU32(17, crc32c(w.view().subarray(21)));
  return w.result();
}

function fetchV7Response(
  sessionId: number,
  topics: Array<{ name: string; partitions: Array<{ index: number; records: Uint8Array | null }> }>,
): Writer {
  return new Writer()
    .i32(0) // throttle
    .i16(0) // top-level error
    .i32(sessionId)
    .array(topics, (writer, t) =>
      writer.string(t.name).array(t.partitions, (partitionWriter, p) =>
        partitionWriter
          .i32(p.index)
          .i16(0)
          .i64(10)
          .i64(10)
          .i64(0)
          .array([], () => {})
          .bytes(p.records),
      ),
    );
}

function respond(socket: Bun.Socket, correlation: number, body: Writer): void {
  const response = new Writer().i32(0).i32(correlation).raw(body.result());
  response.patchI32(0, response.length - 4);
  socket.write(response.result());
}

describe("Fetch sessions (mock broker)", () => {
  test("establishes a session, then prunes idle partitions incrementally", async () => {
    const requests: FetchRequestView[] = [];
    let call = 0;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key === 18) {
            respond(socket, correlation, apiVersions());
            return;
          }
          if (key === 3) {
            respond(
              socket,
              correlation,
              new Writer()
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
                ),
            );
            return;
          }
          if (key === 2) {
            respond(
              socket,
              correlation,
              new Writer().array(["events"], (w, t) =>
                w.string(t).array([0], (p, partition) => p.i32(partition).i16(0).i64(0).i64(10)),
              ),
            );
            return;
          }
          if (key === 1) {
            call += 1;
            requests.push(parseFetchV7(request.buffer, request.byteOffset, request.byteLength));
            if (call === 1) {
              respond(
                socket,
                correlation,
                fetchV7Response(42, [
                  {
                    name: "events",
                    partitions: [{ index: 0, records: singleRecordBatch("x", 0n) }],
                  },
                ]),
              );
            } else {
              respond(
                socket,
                correlation,
                fetchV7Response(42, [
                  { name: "events", partitions: [{ index: 0, records: null }] },
                ]),
              );
            }
            return;
          }
          respond(socket, correlation, new Writer().i32(0).i16(0));
        },
      },
    });

    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const consumer = kafka.consumer();
      await consumer.assign([{ topic: "events", partition: 0, offset: 0n }]);
      const first = await consumer.fetch({ maxWaitMs: 20 });
      expect(first.map((m) => dec(m.value))).toEqual(["x"]);
      await consumer.fetch({ maxWaitMs: 20 });
      await consumer.fetch({ maxWaitMs: 20 });

      expect(requests.length).toBeGreaterThanOrEqual(2);
      expect(requests[0]).toMatchObject({ sessionId: 0, sessionEpoch: 0 });
      expect(requests[0]!.topics[0]).toMatchObject({ name: "events" });
      expect(requests[1]).toMatchObject({ sessionId: 42, sessionEpoch: 1 });
      const third = requests[2];
      if (third) {
        // once idle, the partition is omitted entirely from the incremental request
        expect(third.sessionId).toBe(42);
        expect(third.topics.length).toBe(0);
      }
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("paused partitions are sent as forgotten topics", async () => {
    const requests: FetchRequestView[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key === 18) {
            respond(socket, correlation, apiVersions());
            return;
          }
          if (key === 3) {
            const metadataBody = new Writer()
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
                  .array([0, 1], (pw, pid) =>
                    pw
                      .i16(0)
                      .i32(pid)
                      .i32(1)
                      .array([1], (w) => w.i32(1))
                      .array([1], (w) => w.i32(1)),
                  ),
              );
            respond(socket, correlation, metadataBody);
            return;
          }
          if (key === 2) {
            respond(
              socket,
              correlation,
              new Writer().array(["events"], (w, t) =>
                w.string(t).array([0, 1], (p, partition) => p.i32(partition).i16(0).i64(0).i64(10)),
              ),
            );
            return;
          }
          if (key === 1) {
            requests.push(parseFetchV7(request.buffer, request.byteOffset, request.byteLength));
            respond(
              socket,
              correlation,
              fetchV7Response(7, [
                {
                  name: "events",
                  partitions: [
                    { index: 0, records: null },
                    { index: 1, records: null },
                  ],
                },
              ]),
            );
            return;
          }
          respond(socket, correlation, new Writer().i32(0).i16(0));
        },
      },
    });

    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const consumer = kafka.consumer();
      await consumer.assign([
        { topic: "events", partition: 0, offset: 0n },
        { topic: "events", partition: 1, offset: 0n },
      ]);
      await consumer.fetch({ maxWaitMs: 20 });
      consumer.pause([{ topic: "events", partition: 1 }]);
      await consumer.fetch({ maxWaitMs: 20 });
      expect(requests.length).toBe(2);
      expect(requests[1]!.sessionEpoch).toBe(1);
      const eventsForgotten = requests[1]!.forgotten.find((f) => f.name === "events");
      expect(eventsForgotten).toBeTruthy();
      expect(eventsForgotten!.partitions).toContain(1);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
