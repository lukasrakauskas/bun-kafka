import { describe, expect, test } from "bun:test";
import { Kafka } from "../../src/kafkajs-compat/index.ts";
import { Writer } from "../../src/bun/protocol.ts";

const apiVersions = () =>
  new Writer().i16(0).array(
    Array.from({ length: 64 }, (_, key) => key),
    (writer, key) => writer.i16(key).i16(0).i16(20),
  );

function metadataBody(port: number) {
  return new Writer()
    .array([{ id: 1, host: "127.0.0.1", port }], (writer, b) =>
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
}

/** Minimal SASL/OAUTHBEARER broker: records every token the client presents. */
function saslListener(port: () => number, tokens: string[]) {
  return Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      data(socket, request) {
        let offset = 0;
        while (offset < request.byteLength) {
          const size = new DataView(request.buffer, request.byteOffset + offset).getInt32(0);
          const frameView = new DataView(request.buffer, request.byteOffset + offset + 4, 8);
          const key = frameView.getInt16(0);
          const correlation = new DataView(
            request.buffer,
            request.byteOffset + offset + 8,
            4,
          ).getInt32(0);
          if (key === 36) {
            // Header: size(4) key(2) version(2) correlation(4) clientId(string); body is bytes(auth).
            const frame = new DataView(request.buffer, request.byteOffset + offset);
            const clientIdLength = frame.getInt16(12);
            const auth = frame.getInt32(14 + clientIdLength);
            tokens.push(
              new TextDecoder().decode(
                new Uint8Array(
                  request.buffer,
                  request.byteOffset + offset + 18 + clientIdLength,
                  auth,
                ),
              ),
            );
          }
          let body: Writer;
          if (key === 18) body = apiVersions();
          else if (key === 17)
            body = new Writer().i16(0).array(["OAUTHBEARER"], (writer, m) => writer.string(m));
          else if (key === 36)
            body = new Writer().i16(0).string(null).bytes(new Uint8Array()).i64(0);
          else body = metadataBody(port());
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
          offset += 4 + size;
        }
      },
    },
  });
}

describe("SASL OAUTHBEARER provider callbacks (kafkajs config shape)", () => {
  test("oauthBearerProvider resolving to {value} supplies the bearer token", async () => {
    const tokens: string[] = [];
    let providerCalls = 0;
    const listener = saslListener(() => listener.port, tokens);
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      sasl: {
        mechanism: "oauthbearer",
        oauthBearerProvider: async () => {
          providerCalls++;
          return { value: "token-from-provider", expiryTimeMs: Date.now() + 60_000 };
        },
      },
    });
    try {
      const admin = kafka.admin();
      await admin.connect();
      expect(await admin.describeCluster()).toMatchObject({ clusterId: null });
      await admin.disconnect();
      expect(providerCalls).toBe(1);
      expect(tokens[0]).toBe("n,,\u0001auth=Bearer token-from-provider\u0001\u0001");
    } finally {
      listener.stop(true);
    }
  }, 15_000);

  test("oauthBearerProvider returning a plain string is accepted too", async () => {
    const tokens: string[] = [];
    const listener = saslListener(() => listener.port, tokens);
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      sasl: {
        mechanism: "oauthbearer",
        oauthBearerProvider: async () => "plain-token",
      },
    });
    try {
      const admin = kafka.admin();
      await admin.connect();
      await admin.describeCluster();
      await admin.disconnect();
      expect(tokens[0]).toContain("Bearer plain-token");
    } finally {
      listener.stop(true);
    }
  }, 15_000);
});
