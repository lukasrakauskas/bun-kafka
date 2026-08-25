import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Writer } from "../../src/bun/protocol.ts";

const apiVersions = () => new Writer().i16(0).array(Array.from({ length: 64 }, (_, key) => key), (writer, key) => writer.i16(key).i16(0).i16(20));

describe("SASL OAUTHBEARER reauthentication", () => {
  test("re-authenticates before the advertised session lifetime elapses", async () => {
    const authEvents: number[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key === 36) authEvents.push(Date.now());
          let body: Writer;
          if (key === 18) body = apiVersions();
          else if (key === 17) body = new Writer().i16(0).array(["OAUTHBEARER"], (writer, m) => writer.string(m));
          else if (key === 36) body = new Writer().i16(0).string(null).bytes(new Uint8Array()).i64(500); // lifetime 500ms
          else body = metadataBody(listener.port);
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });
    function metadataBody(port: number) {
      return new Writer()
        .array([{ id: 1, host: "127.0.0.1", port }], (writer, b) => writer.i32(b.id).string(b.host).i32(b.port).string(null))
        .string(null)
        .i32(1)
        .array([{ name: "events" }], (writer, item) => writer.i16(0).string(item.name).bool(false).array([0], (pw) => pw.i16(0).i32(0).i32(1).array([1], (w) => w.i32(1)).array([1], (w) => w.i32(1))));
    }
    let tokenCounter = 0;
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      sasl: {
        mechanism: "oauthbearer",
        token: () => `token-${++tokenCounter}`,
      },
    });
    try {
      const admin = kafka.admin();
      await admin.metadata(["events"]);
      expect(authEvents.length).toBe(1); // initial authentication
      // Session lifetime is 500ms; re-authentication fires at ~400ms.
      await Bun.sleep(700);
      expect(authEvents.length).toBeGreaterThanOrEqual(2);
      expect(tokenCounter).toBeGreaterThanOrEqual(2);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
