import { describe, expect, test } from "bun:test";
import { Kafka, KafkaError } from "../../index.ts";
import {
  Reader,
  RecordSetDecoder,
  Writer,
  crc32c,
  encodeRecordBatch,
} from "../../src/bun/protocol.ts";

type Request = { apiKey: number; correlation: number; count: number; socket: Bun.Socket };
type MockBroker = { address: string; close(): void; active(): number };
type Handler = (
  request: Request,
  reply: (body: Writer | Uint8Array, correlation?: number) => void,
) => boolean | void;

const topic = "chaos";
const apiVersions = () =>
  new Writer().i16(0).array(
    Array.from({ length: 64 }, (_, key) => key),
    (writer, key) => writer.i16(key).i16(0).i16(20),
  );
const recordBatch = (value: string, offset = 0n) => {
  const batch = encodeRecordBatch([{ value }]);
  new DataView(batch.buffer, batch.byteOffset, batch.byteLength).setBigInt64(0, offset);
  return batch;
};

function response(correlation: number, body: Writer | Uint8Array): Uint8Array {
  const frame = new Writer()
    .i32(0)
    .i32(correlation)
    .raw(body instanceof Writer ? body.result() : body);
  frame.patchI32(0, frame.length - 4);
  return frame.result();
}

function metadataBody(brokers: MockBroker[], leader = 1, error = 0): Writer {
  return new Writer()
    .array(brokers, (writer, broker) => {
      const url = new URL(`tcp://${broker.address}`);
      writer
        .i32(brokers.indexOf(broker) + 1)
        .string(url.hostname)
        .i32(Number(url.port))
        .string(null);
    })
    .string(null)
    .i32(1)
    .array([topic], (writer, name) =>
      writer
        .i16(error)
        .string(name)
        .bool(false)
        .array(error ? [] : [0], (partitionWriter) => {
          partitionWriter
            .i16(0)
            .i32(0)
            .i32(leader)
            .array(
              brokers.map((_, index) => index + 1),
              (item, id) => item.i32(id),
            )
            .array(
              brokers.map((_, index) => index + 1),
              (item, id) => item.i32(id),
            );
        }),
    );
}

function defaultBody(apiKey: number, broker: MockBroker, fetchOffset = 0n): Writer {
  if (apiKey === 18) return apiVersions();
  if (apiKey === 3) return metadataBody([broker]);
  if (apiKey === 2)
    return new Writer().array([topic], (writer, name) =>
      writer
        .string(name)
        .array([0], (partitionWriter) => partitionWriter.i32(0).i16(0).i64(0).i64(0)),
    );
  if (apiKey === 0)
    return new Writer()
      .array([topic], (writer, name) =>
        writer
          .string(name)
          .array([0], (partitionWriter) => partitionWriter.i32(0).i16(0).i64(fetchOffset).i64(-1)),
      )
      .i32(0);
  if (apiKey === 1)
    return new Writer()
      .i32(0)
      .i16(0)
      .i32(0)
      .array([topic], (writer, name) =>
        writer.string(name).array([0], (partitionWriter) => {
          partitionWriter
            .i32(0)
            .i16(0)
            .i64(fetchOffset + 1n)
            .i64(fetchOffset + 1n)
            .i64(0)
            .array([], () => {})
            .bytes(recordBatch(`value-${fetchOffset}`, fetchOffset));
        }),
      );
  if (apiKey === 22) return new Writer().i32(0).i16(0).i64(1).i16(0);
  return new Writer();
}

function mockBroker(handler: Handler = () => false): MockBroker {
  const buffers = new WeakMap<Bun.Socket, Uint8Array>();
  const counts = new Map<number, number>();
  let open = 0;
  let broker: MockBroker;
  const listener = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      open() {
        open++;
      },
      close() {
        open--;
      },
      data(socket, chunk) {
        const previous = buffers.get(socket) ?? new Uint8Array();
        const input = new Uint8Array(previous.byteLength + chunk.byteLength);
        input.set(previous);
        input.set(chunk, previous.byteLength);
        let at = 0;
        while (input.byteLength - at >= 4) {
          const size = new DataView(input.buffer, input.byteOffset + at, 4).getInt32(0);
          if (size < 0 || input.byteLength - at < size + 4) break;
          const frame = input.subarray(at, at + size + 4);
          const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
          const apiKey = view.getInt16(4);
          const correlation = view.getInt32(8);
          const count = (counts.get(apiKey) ?? 0) + 1;
          counts.set(apiKey, count);
          const reply = (body: Writer | Uint8Array, responseCorrelation = correlation) =>
            socket.write(response(responseCorrelation, body));
          if (handler({ apiKey, correlation, count, socket }, reply) === false)
            reply(defaultBody(apiKey, broker, BigInt(Math.max(0, count - 1))));
          at += size + 4;
        }
        buffers.set(socket, input.slice(at));
      },
    },
  });
  broker = {
    address: `127.0.0.1:${listener.port}`,
    close: () => listener.stop(true),
    active: () => open,
  };
  return broker;
}

const kafka = (
  brokers: MockBroker[],
  extra: Partial<ConstructorParameters<typeof Kafka>[0]> = {},
) =>
  new Kafka({
    requestTimeoutMs: 40,
    connectTimeoutMs: 40,
    retry: { maxRetries: 0, initialBackoffMs: 0, maxBackoffMs: 0 },
    ...extra,
    brokers: brokers.map(({ address }) => address),
  });

async function rejectsQuickly(promise: Promise<unknown>, limitMs = 500): Promise<void> {
  const started = performance.now();
  await expect(promise).rejects.toBeDefined();
  expect(performance.now() - started).toBeLessThan(limitMs);
}

function close(...brokers: MockBroker[]) {
  for (const broker of brokers) broker.close();
}

describe("deterministic Kafka chaos", () => {
  test("uses a healthy bootstrap broker when the first broker is unavailable", async () => {
    const broker = mockBroker();
    const client = new Kafka({ brokers: ["127.0.0.1:1", broker.address], connectTimeoutMs: 40 });
    try {
      expect((await client.admin().metadata([topic])).topics[0]?.name).toBe(topic);
    } finally {
      await client.disconnect();
      close(broker);
    }
  });

  test("rejects a reset Produce and reconnects with clean frame state", async () => {
    const broker = mockBroker(({ apiKey, count, socket }) => {
      if (apiKey === 0 && count === 1) {
        socket.terminate();
        return true;
      }
      return false;
    });
    const client = kafka([broker]);
    const producer = client.producer({ lingerMs: 0 });
    try {
      await rejectsQuickly(
        producer.send({
          topic,
          timeoutMs: 40,
          messages: [{ value: "unknown-result", partition: 0 }],
        }),
      );
      expect(
        (
          await producer.send({
            topic,
            timeoutMs: 40,
            messages: [{ value: "after-reset", partition: 0 }],
          })
        )[0]?.topic,
      ).toBe(topic);
    } finally {
      await client.disconnect();
      close(broker);
    }
  });

  test("discards a partial Fetch frame and keeps old zero-copy values valid", async () => {
    let partial = false;
    const broker = mockBroker(({ apiKey, count, correlation, socket }) => {
      if (apiKey === 1 && count === 2) {
        const frame = response(correlation, defaultBody(1, broker, 1n));
        socket.write(frame.subarray(0, Math.floor(frame.length / 2)));
        socket.terminate();
        partial = true;
        return true;
      }
      return false;
    });
    const client = kafka([broker]);
    const consumer = client.consumer({ fromBeginning: true });
    try {
      await consumer.assign([{ topic, partition: 0, offset: 0n }]);
      const held = (await consumer.fetch({ maxWaitMs: 1, maxMessages: 1 }))[0]!;
      await rejectsQuickly(consumer.fetch({ maxWaitMs: 1, maxMessages: 1 }));
      expect(partial).toBe(true);
      expect(new TextDecoder().decode(held.value! as Uint8Array)).toBe("value-0");
      expect(consumer.position(topic, 0)).toBe(1n);
    } finally {
      await client.disconnect();
      close(broker);
    }
  });

  test("bounds blackholed Metadata, ListOffsets, Produce, and Fetch requests", async () => {
    for (const apiKey of [3, 2, 0, 1]) {
      const broker = mockBroker((request) => request.apiKey === apiKey || false);
      const client = kafka([broker]);
      try {
        if (apiKey === 3) await rejectsQuickly(client.admin().metadata([topic]));
        if (apiKey === 2)
          await rejectsQuickly(
            client.consumer().assign([{ topic, partition: 0, offset: "earliest" }]),
          );
        if (apiKey === 0)
          await rejectsQuickly(
            client
              .producer({ lingerMs: 0 })
              .send({ topic, timeoutMs: 40, messages: [{ value: "x", partition: 0 }] }),
          );
        if (apiKey === 1) {
          const consumer = client.consumer();
          await consumer.assign([{ topic, partition: 0, offset: 0n }]);
          await rejectsQuickly(consumer.fetch({ maxWaitMs: 1 }));
        }
      } finally {
        await client.disconnect();
        close(broker);
      }
    }
  });

  test("retries a reset request with backoff and succeeds on a slow response", async () => {
    const events: unknown[] = [];
    const broker = mockBroker(({ apiKey, count, socket }, reply) => {
      if (apiKey !== 2) return false;
      if (count === 1) {
        socket.terminate();
        return true;
      }
      setTimeout(() => reply(defaultBody(2, broker)), 20);
      return true;
    });
    const client = kafka([broker], {
      requestTimeoutMs: 100,
      retry: { maxRetries: 1, initialBackoffMs: 5, maxBackoffMs: 5 },
      onEvent: (event) => events.push(event),
    });
    try {
      const started = performance.now();
      await client.consumer().assign([{ topic, partition: 0, offset: "earliest" }]);
      expect(performance.now() - started).toBeGreaterThanOrEqual(20);
      expect(events).toContainEqual(
        expect.objectContaining({ type: "retry", apiKey: 2, attempt: 1 }),
      );
    } finally {
      await client.disconnect();
      close(broker);
    }
  });

  test("refreshes metadata and follows a transferred leader", async () => {
    let leader = 1;
    let firstProduce = true;
    let a: MockBroker;
    let b: MockBroker;
    const handler =
      (id: number): Handler =>
      ({ apiKey }, reply) => {
        if (apiKey === 18) {
          reply(apiVersions());
          return true;
        }
        if (apiKey === 3) {
          reply(metadataBody([a, b], leader));
          return true;
        }
        if (apiKey === 0 && id === 1 && firstProduce) {
          firstProduce = false;
          leader = 2;
          reply(
            new Writer()
              .array([topic], (writer, name) =>
                writer.string(name).array([0], (part) => part.i32(0).i16(6).i64(-1).i64(-1)),
              )
              .i32(0),
          );
          return true;
        }
        return false;
      };
    a = mockBroker(handler(1));
    b = mockBroker(handler(2));
    const client = kafka([a], { retry: { maxRetries: 1, initialBackoffMs: 0, maxBackoffMs: 0 } });
    try {
      expect(
        (
          await client
            .producer({ lingerMs: 0 })
            .send({ topic, messages: [{ value: "moved", partition: 0 }] })
        )[0]?.topic,
      ).toBe(topic);
    } finally {
      await client.disconnect();
      close(a, b);
    }
  });

  test("handles broker pause, timeout, resume, and a late response", async () => {
    let metadataRequests = 0;
    const broker = mockBroker(({ apiKey }, reply) => {
      if (apiKey !== 3) return false;
      metadataRequests++;
      if (metadataRequests === 1) setTimeout(() => reply(metadataBody([broker])), 20);
      else if (metadataRequests === 2) setTimeout(() => reply(metadataBody([broker])), 80);
      else reply(metadataBody([broker]));
      return true;
    });
    const client = kafka([broker]);
    try {
      expect((await client.admin().metadata([topic])).topics[0]?.name).toBe(topic);
      await rejectsQuickly(client.admin().metadata([topic]));
      expect((await client.admin().metadata([topic])).topics[0]?.name).toBe(topic);
      await Bun.sleep(90);
      expect((await client.admin().metadata([topic])).topics[0]?.name).toBe(topic);
    } finally {
      await client.disconnect();
      close(broker);
    }
  });

  test("surfaces broker storage errors and topic deletion without false success", async () => {
    let deleted = false;
    const broker = mockBroker(({ apiKey }, reply) => {
      if (apiKey === 0) {
        reply(
          new Writer()
            .array([topic], (writer, name) =>
              writer.string(name).array([0], (part) => part.i32(0).i16(56).i64(-1).i64(-1)),
            )
            .i32(0),
        );
        return true;
      }
      if (apiKey === 3 && deleted) {
        reply(metadataBody([broker], 1, 3));
        return true;
      }
      return false;
    });
    const client = kafka([broker]);
    try {
      await rejectsQuickly(
        client
          .producer({ lingerMs: 0 })
          .send({ topic, messages: [{ value: "disk-full", partition: 0 }] }),
      );
      deleted = true;
      expect((await client.admin().metadata([topic])).topics[0]?.err).toBe(3);
      deleted = false;
      expect((await client.admin().metadata([topic])).topics[0]?.err).toBe(0);
    } finally {
      await client.disconnect();
      close(broker);
    }
  });

  test("rejects malformed frames and accepts a valid frame after an unknown correlation ID", async () => {
    for (const size of [-1, 1025]) {
      const broker = mockBroker(({ socket }) => {
        const header = new Writer().i32(size).result();
        socket.write(header);
        return true;
      });
      const client = kafka([broker], { maxResponseBytes: 1024 });
      try {
        await rejectsQuickly(client.admin().metadata([topic]));
      } finally {
        await client.disconnect();
        close(broker);
      }
    }

    const broker = mockBroker(({ apiKey, correlation }, reply) => {
      reply(defaultBody(apiKey, broker), correlation + 100);
      reply(defaultBody(apiKey, broker));
      return true;
    });
    const client = kafka([broker]);
    try {
      expect((await client.admin().metadata([topic])).topics[0]?.name).toBe(topic);
    } finally {
      await client.disconnect();
      close(broker);
    }
  });

  test("rejects truncated fields, invalid arrays, varints, records, CRCs, and compression", () => {
    expect(() => new Reader(new Uint8Array([0])).i32()).toThrow(KafkaError);
    expect(() => new Reader(new Uint8Array([0, 0, 0, 2])).array((reader) => reader.i8())).toThrow(
      KafkaError,
    );
    expect(() => new Reader(new Uint8Array([0x80, 0x80, 0x80, 0x80, 0x80])).varInt()).toThrow(
      KafkaError,
    );
    expect(() => new Reader(new Uint8Array(10).fill(0x80)).varLong()).toThrow(KafkaError);

    const invalidRecord = recordBatch("x");
    invalidRecord[61] = 0x7e;
    new DataView(invalidRecord.buffer).setUint32(17, crc32c(invalidRecord.subarray(21)));
    expect(() => new RecordSetDecoder(invalidRecord, topic, 0, 1).read()).toThrow(KafkaError);

    const badCrc = recordBatch("x");
    badCrc[badCrc.length - 1]! ^= 1;
    expect(() => new RecordSetDecoder(badCrc, topic, 0, 1).read()).toThrow(/CRC/);

    const badCompression = recordBatch("x");
    new DataView(badCompression.buffer).setInt16(21, 7);
    new DataView(badCompression.buffer).setUint32(17, crc32c(badCompression.subarray(21)));
    expect(() => new RecordSetDecoder(badCompression, topic, 0, 1).read()).toThrow(/compression/);
  });

  test("does not leak sockets through repeated fault cycles", async () => {
    const cycles = Number(process.env.CHAOS_FAULT_CYCLES ?? 1_000);
    const broker = mockBroker(({ apiKey, socket }, reply) => {
      if (apiKey !== 3) return false;
      reply(metadataBody([broker]));
      queueMicrotask(() => socket.terminate());
      return true;
    });
    const client = kafka([broker]);
    try {
      for (let i = 0; i < cycles; i++) {
        await client
          .admin()
          .metadata([topic])
          .catch((error) => expect(error).toBeInstanceOf(KafkaError));
        await Bun.sleep(0);
      }
    } finally {
      await client.disconnect();
    }
    await Bun.sleep(20);
    expect(broker.active()).toBe(0);
    close(broker);
  }, 60_000);
});

describe("Kafka TLS chaos", () => {
  test("rejects an untrusted certificate, a wrong server name, and plaintext fallback", async () => {
    const cert = await Bun.file(new URL("fixtures/chaos-cert.pem", import.meta.url)).text();
    const key = await Bun.file(new URL("fixtures/chaos-key.pem", import.meta.url)).text();
    let kafkaBytes = 0;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      tls: { cert, key },
      socket: {
        data(_socket, data) {
          kafkaBytes += data.byteLength;
        },
      },
    });
    const mutual = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      tls: { cert, key, ca: cert, requestCert: true, rejectUnauthorized: true },
      socket: {
        data() {
          kafkaBytes++;
        },
      },
    });
    const untrusted = new Kafka({
      brokers: [`localhost:${listener.port}`],
      tls: true,
      connectTimeoutMs: 100,
      requestTimeoutMs: 100,
    });
    const wrongName = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      tls: { ca: cert },
      connectTimeoutMs: 100,
      requestTimeoutMs: 100,
    });
    const noClientCertificate = new Kafka({
      brokers: [`localhost:${mutual.port}`],
      tls: { ca: cert },
      connectTimeoutMs: 100,
      requestTimeoutMs: 100,
    });
    try {
      await rejectsQuickly(untrusted.admin().metadata([topic]));
      await rejectsQuickly(wrongName.admin().metadata([topic]));
      await rejectsQuickly(noClientCertificate.admin().metadata([topic]));
      expect(kafkaBytes).toBe(0);
    } finally {
      await untrusted.disconnect();
      await wrongName.disconnect();
      await noClientCertificate.disconnect();
      listener.stop(true);
      mutual.stop(true);
    }
  });

  (process.env.CHAOS_TLS_EXTERNAL === "1" ? test : test.skip)(
    "rejects an expired certificate",
    async () => {
      const client = new Kafka({
        brokers: ["expired.badssl.com:443"],
        tls: true,
        connectTimeoutMs: 2_000,
        requestTimeoutMs: 2_000,
      });
      try {
        await rejectsQuickly(client.admin().metadata(), 3_000);
      } finally {
        await client.disconnect();
      }
    },
  );
});
