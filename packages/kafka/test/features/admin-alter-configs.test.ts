import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Kafka as CompatKafka } from "../../src/kafkajs-compat/index.ts";
import { decoder, encoder, type KafkaEncoder } from "../../src/protocol/index.ts";

const apiVersionsBody = encoder()
  .i16(0)
  .array([18, 33], (writer, key) => writer.i16(key).i16(0).i16(0))
  .i32(0);

function reply(socket: Bun.TcpSocket, correlation: number, body: KafkaEncoder) {
  const response = encoder().i32(0).i32(correlation).raw(body.result());
  socket.write(response.patchI32(0, response.length - 4).result());
}

describe("AlterConfigs (mock broker)", () => {
  test("encodes validateOnly for native and KafkaJS-compatible calls", async () => {
    const requests: Array<{ validateOnly: boolean; remaining: number }> = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          let offset = 0;
          while (offset < request.byteLength) {
            const view = new DataView(request.buffer, request.byteOffset + offset);
            const size = view.getInt32(0);
            const key = view.getInt16(4);
            const correlation = view.getInt32(8);
            if (key === 18) {
              reply(socket, correlation, apiVersionsBody);
            } else if (key === 33) {
              const clientIdLength = view.getInt16(12);
              const bodyOffset = offset + 14 + clientIdLength;
              const body = decoder(
                new Uint8Array(
                  request.buffer,
                  request.byteOffset + bodyOffset,
                  4 + size - (14 + clientIdLength),
                ),
              );
              body.array((resource) => {
                resource.i8();
                resource.string();
                resource.array((config) => {
                  config.string();
                  config.string();
                });
              });
              const validateOnly = body.bool();
              requests.push({ validateOnly, remaining: body.remaining });
              reply(
                socket,
                correlation,
                encoder()
                  .i32(0)
                  .array(["events"], (writer, name) =>
                    writer.i16(0).string(null).i8(2).string(name),
                  ),
              );
            }
            offset += 4 + size;
          }
        },
      },
    });
    const brokers = [`127.0.0.1:${listener.port}`];
    const resources = [
      { resourceType: 2, resourceName: "events", configs: { "retention.ms": "60000" } },
    ];
    const kafka = new Kafka({ brokers });
    const compatAdmin = new CompatKafka({ brokers }).admin();
    try {
      await kafka.admin().alterConfigs(resources);
      await compatAdmin.connect();
      await compatAdmin.alterConfigs({
        validateOnly: true,
        resources: [{ type: 2, name: "events", configEntries: { "retention.ms": "120000" } }],
      });
      expect(requests).toEqual([
        { validateOnly: false, remaining: 0 },
        { validateOnly: true, remaining: 0 },
      ]);
    } finally {
      await kafka.disconnect();
      await compatAdmin.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
