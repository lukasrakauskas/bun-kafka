import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Reader, Writer } from "../../src/bun/protocol.ts";
import { admin } from "../helpers.ts";

const apiVersions = () =>
  new Writer().i16(0).array(
    Array.from({ length: 64 }, (_, key) => key),
    (writer, key) => writer.i16(key).i16(0).i16(20),
  );

const apiVersionsBody = apiVersions();
function requestBodyBytes(buffer: ArrayBuffer, byteOffset: number, byteLength: number): Uint8Array {
  const clientIdLen = new DataView(buffer, byteOffset, byteLength).getInt16(12);
  const start = byteOffset + 14 + clientIdLen + 1; // +1 flexible header tag byte
  return new Uint8Array(buffer, start, byteLength - (14 + clientIdLen + 1));
}

describe("Client quotas (mock broker)", () => {
  test("describeClientQuotas sends v1 flexible filters and parses entries", async () => {
    let sawDescribe = false;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          let body: Writer;
          let response: Writer;
          if (key === 18) {
            response = new Writer().i32(correlation).raw(apiVersionsBody.result());
            socket.write(
              new Writer().i32(0).patchI32(0, response.length).raw(response.result()).result(),
            );
            return;
          }
          {
            void key;
          }
          if (key === 48) {
            sawDescribe = true;
            const req = new Reader(
              requestBodyBytes(request.buffer, request.byteOffset, request.byteLength),
            );
            const components = req.compactArray((r) => ({
              entityType: r.compactString(),
              matchType: r.i8(),
              match: r.compactString(),
            }));
            expect(components.length).toBe(1);
            // compactString("client-id") => length+1 = 10; match type exact = 0.
            expect(components[0]).toEqual({
              entityType: "client-id",
              matchType: 0,
              match: "quota-app",
            });
            expect(req.bool()).toBe(false); // strict
          }
          // DescribeClientQuotas v1 response with one entry.
          response = new Writer()
            .i32(correlation)
            .uvarint(0) // header tags
            .i32(0) // throttle
            .i16(0) // error code
            .compactString(null) // error message
            .compactArray(
              [{ entity: "client-id", name: "quota-app", value: 1024 }],
              (writer, item) => {
                writer.compactArray([{ t: item.entity, n: item.name }], (entityWriter, e) =>
                  entityWriter.compactString(e.t).compactString(e.n).tags(),
                );
                writer.compactArray(
                  [{ k: "producer_byte_rate", v: item.value }],
                  (valueWriter, v) => valueWriter.compactString(v.k).f64(v.v).tags(),
                );
                writer.tags(); // entry tags
              },
            )
            .tags();
          socket.write(
            new Writer().i32(0).patchI32(0, response.length).raw(response.result()).result(),
          );
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const result = await kafka
        .admin()
        .describeClientQuotas([
          { entityType: "client-id", matchType: "exact", match: "quota-app" },
        ]);
      expect(sawDescribe).toBe(true);
      expect(result[0]!.entities).toEqual([{ entityType: "client-id", entityName: "quota-app" }]);
      expect(result[0]!.values).toEqual([{ name: "producer_byte_rate", value: 1024 }]);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("alterClientQuotas encodes per-struct tag sections and decodes results", async () => {
    let sawAlter = false;
    let sawValidateOnly = false;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key === 18) {
            const r2 = new Writer().i32(correlation).raw(apiVersionsBody.result());
            socket.write(new Writer().i32(0).patchI32(0, r2.length).raw(r2.result()).result());
            return;
          }
          if (key === 49) {
            sawAlter = true;
            const body = new Reader(
              requestBodyBytes(request.buffer, request.byteOffset, request.byteLength),
            );
            const entries = body.compactArray((entryReader) => {
              const entity = entryReader.compactArray((entityReader) => ({
                t: entityReader.compactString(),
                n: entityReader.compactString(),
                tags: entityReader.uvarint(),
              }));
              const ops = entryReader.compactArray((opsReader) => ({
                key: opsReader.compactString(),
                value: opsReader.f64(),
                remove: opsReader.bool(),
                tags: opsReader.uvarint(),
              }));
              const entryTags = entryReader.uvarint();
              return { entity, ops, entryTags };
            });
            sawValidateOnly = body.bool();
            expect(entries[0]!.entity[0]).toEqual({ t: "user", n: "alice", tags: 0 });
            expect(entries[0]!.ops[0]!.key).toBe("consumer_byte_rate");
            expect(entries[0]!.entryTags).toBe(0);
            var responseBody = new Writer()
              .i32(correlation)
              .uvarint(0)
              .i32(0)
              .compactArray(
                [{ code: 0, msg: null, entity: "user", name: "alice" }],
                (writer, item) =>
                  writer
                    .i16(item.code)
                    .compactString(item.msg)
                    .compactArray([{ t: item.entity, n: item.name }], (entityWriter, e) =>
                      entityWriter.compactString(e.t).compactString(e.n).tags(),
                    )
                    .tags(),
              )
              .tags();
          }
          const resp: Writer = responseBody!;
          socket.write(new Writer().i32(0).patchI32(0, resp.length).raw(resp.result()).result());
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const result = await kafka.admin().alterClientQuotas([
        {
          entity: [{ entityType: "user", entityName: "alice" }],
          ops: [{ key: "consumer_byte_rate", value: 4096 }],
          validateOnly: true,
        },
      ]);
      expect(sawAlter).toBe(true);
      expect(sawValidateOnly).toBe(true);
      expect(result[0]!.error).toBe(0);
      expect(result[0]!.entity).toEqual([{ entityType: "user", entityName: "alice" }]);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});

describe("Delegation tokens (mock broker)", () => {
  test("createDelegationToken sends v2 flexible body and parses token fields", async () => {
    let sawCreate = false;
    let hmacBytes: Uint8Array | null = null;
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key === 18) {
            const r3 = new Writer().i32(correlation).raw(apiVersionsBody.result());
            socket.write(new Writer().i32(0).patchI32(0, r3.length).raw(r3.result()).result());
            return;
          }
          if (key === 38) {
            sawCreate = true;
            const body = new Reader(
              requestBodyBytes(request.buffer, request.byteOffset, request.byteLength),
            );
            expect(body.compactString()).toBe(null); // owner principal type
            expect(body.compactString()).toBe(null); // owner principal name
            expect(body.i64()).toBe(-1n); // renew period: default
          }
          hmacBytes = new TextEncoder().encode("token-hmac-bytes");
          const response = new Writer()
            .i32(correlation)
            .uvarint(0)
            .i32(0)
            .i16(0)
            .compactString(null)
            .compactString("User")
            .compactString("alice")
            .compactString("alice")
            .i64(1700000000000n)
            .i64(1710000000000n)
            .i64(1760000000000n)
            .compactString("token-id-1")
            .compactBytes(hmacBytes)
            .tags();
          socket.write(
            new Writer().i32(0).patchI32(0, response.length).raw(response.result()).result(),
          );
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const token = await kafka.admin().createDelegationToken({ renewalPeriodMs: -1 });
      expect(sawCreate).toBe(true);
      expect(token.principalName).toBe("alice");
      expect(token.tokenId).toBe("token-id-1");
      expect(new TextDecoder().decode(token.hmac!)).toBe("token-hmac-bytes");
      expect(token.expiryTimestampMs).toBe(1710000000000n);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("renew/expire round trip wire shapes", async () => {
    const seenKeys: number[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (key !== 18) seenKeys.push(key);
          const response = new Writer()
            .i32(correlation)
            .uvarint(0)
            .i32(0)
            .i16(0)
            .compactString(null)
            .i64(1234567890n)
            .tags();
          socket.write(
            new Writer().i32(0).patchI32(0, response.length).raw(response.result()).result(),
          );
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const a = kafka.admin();
      const fakeHmac = new TextEncoder().encode("h");
      const renewed = await a.renewDelegationToken(fakeHmac, 3600_000);
      expect(renewed.expiryTimestampMs).toBe(1234567890n);
      const expired = await a.expireDelegationToken(fakeHmac, 60_000);
      expect(expired.expiryTimestampMs).toBe(1234567890n);
      expect(seenKeys).toEqual([39, 40]);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("describeDelegationTokens parses the token list", async () => {
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const correlation = view.getInt32(8);
          const response = new Writer()
            .i32(correlation)
            .uvarint(0)
            .i32(0)
            .i16(0)
            .compactString(null)
            .compactArray(
              [{ ownerType: "User", ownerName: "bob", requester: "bob", id: "t-9" }],
              (writer, t) =>
                writer
                  .compactString(t.ownerType)
                  .compactString(t.ownerName)
                  .compactString(t.requester)
                  .i64(100n)
                  .i64(200n)
                  .i64(300n)
                  .compactString(t.id)
                  .compactBytes(new Uint8Array([9, 9]))
                  .tags(),
            )
            .tags();
          socket.write(
            new Writer().i32(0).patchI32(0, response.length).raw(response.result()).result(),
          );
        },
      },
    });
    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const tokens = await kafka.admin().describeDelegationTokens();
      expect(tokens.length).toBe(1);
      expect(tokens[0]).toMatchObject({
        ownerPrincipalName: "bob",
        tokenId: "t-9",
        issueTimestampMs: 100n,
        maxTimestampMs: 300n,
      });
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);
});
