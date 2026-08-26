import { describe, expect, test } from "bun:test";
import { Kafka } from "../../index.ts";
import { Writer } from "../../src/bun/protocol.ts";
import { isString } from "../../src/type-guards.ts";

const apiVersions = () =>
  new Writer().i16(0).array(
    Array.from({ length: 64 }, (_, key) => key),
    (writer, key) => writer.i16(key).i16(0).i16(20),
  );

function metadataBody(port: number) {
  return new Writer()
    .array([{ id: 1, host: "127.0.0.1", port }], (writer, broker) =>
      writer.i32(broker.id).string(broker.host).i32(broker.port).string(null),
    )
    .string(null)
    .i32(1)
    .array([{ name: "events" }], (writer, topic) =>
      writer
        .i16(0)
        .string(topic.name)
        .bool(false)
        .array([0], (partitionWriter) =>
          partitionWriter
            .i16(0)
            .i32(0)
            .i32(1)
            .array([1], (itemWriter) => itemWriter.i32(1))
            .array([1], (itemWriter) => itemWriter.i32(1)),
        ),
    );
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

async function hmac(key: Uint8Array, message: string | Uint8Array): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    key,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const data = isString(message) ? encoder.encode(message) : message;
  return new Uint8Array(await crypto.subtle.sign("HMAC", cryptoKey, data));
}

async function sha256(value: Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", value));
}

function xor(left: Uint8Array, right: Uint8Array): Uint8Array {
  const result = new Uint8Array(left.length);
  for (let i = 0; i < left.length; i++) result[i] = left[i]! ^ right[i]!;
  return result;
}

const b64 = (value: Uint8Array) => btoa(String.fromCharCode(...value));

/**
 * Minimal SASL/SCRAM-SHA-256 broker: performs a real handshake so the client's
 * proof and the server signature are both cryptographically checked.
 */
function scramListener(options: {
  port: () => number;
  password: string;
  onProofVerified?: (verified: boolean) => void;
}) {
  let round = 0;
  let clientFirstBare = "";
  let serverFirst = "";
  const salt = encoder.encode("scram-salt-0001");
  return Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      async data(socket, request) {
        let offset = 0;
        while (offset < request.byteLength) {
          const size = new DataView(request.buffer, request.byteOffset + offset).getInt32(0);
          const frame = request.subarray(offset, offset + 4 + size);
          const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          const clientIdLength = view.getInt16(12);
          const authLength = key === 36 ? view.getInt32(14 + clientIdLength) : 0;
          const auth =
            key === 36
              ? decoder.decode(
                  frame.subarray(18 + clientIdLength, 18 + clientIdLength + authLength),
                )
              : "";

          let body: Writer;
          if (key === 18) body = apiVersions();
          else if (key === 17)
            body = new Writer()
              .i16(0)
              .array(["SCRAM-SHA-256"], (writer, mechanism) => writer.string(mechanism));
          else if (key === 36 && round++ === 0) {
            // Server-first-message: echo the client nonce, add ours.
            clientFirstBare = auth.replace(/^n,,/, "");
            const clientNonce = /r=([^,\s]+)/.exec(clientFirstBare)?.[1] ?? "";
            serverFirst = `r=${clientNonce}serverpart,s=${b64(salt)},i=4096`;
            body = new Writer().i16(0).string(null).bytes(encoder.encode(serverFirst)).i64(0);
          } else if (key === 36) {
            // Client-final-message: verify the proof before answering v=.
            const withoutProof = /^((?:c=[^,]+,r=[^,]+))(?:,p=([A-Za-z0-9+/=]+))?$/.exec(auth);
            const proof = withoutProof?.[2]
              ? Uint8Array.from(atob(withoutProof[2]), (char) => char.charCodeAt(0))
              : null;
            const saltedPassword = new Uint8Array(
              await crypto.subtle.deriveBits(
                {
                  name: "PBKDF2",
                  salt,
                  iterations: 4096,
                  hash: "SHA-256",
                },
                await crypto.subtle.importKey(
                  "raw",
                  encoder.encode(options.password),
                  "PBKDF2",
                  false,
                  ["deriveBits"],
                ),
                256,
              ),
            );
            const clientKey = await hmac(saltedPassword, "Client Key");
            const storedKey = await sha256(clientKey);
            const authMessage = `${clientFirstBare},${serverFirst},${withoutProof?.[1] ?? ""}`;
            const clientSignature = await hmac(storedKey, authMessage);
            const verified =
              proof !== null &&
              (await sha256(xor(proof, clientSignature))).every(
                (byte, index) => byte === storedKey[index],
              );
            options.onProofVerified?.(verified);
            if (!verified) {
              body = new Writer()
                .i16(49)
                .string("SCRAM proof mismatch")
                .bytes(new Uint8Array())
                .i64(0);
            } else {
              const serverKey = await hmac(saltedPassword, "Server Key");
              const verifier = b64(await hmac(serverKey, authMessage));
              body = new Writer()
                .i16(0)
                .string(null)
                .bytes(encoder.encode(`v=${verifier}`))
                .i64(0);
            }
          } else body = metadataBody(options.port());

          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
          offset += 4 + size;
        }
      },
    },
  });
}

describe("SASL/SCRAM-SHA-256 authentication (mock broker)", () => {
  test("completes a real SCRAM exchange before Kafka requests", async () => {
    let verified = false;
    const listener = scramListener({
      port: () => listener.port,
      password: "secret-pass",
      onProofVerified: (ok) => (verified = ok),
    });
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      sasl: { mechanism: "scram-sha-256", username: "user", password: "secret-pass" },
    });
    try {
      const metadata = await kafka.admin().metadata(["events"]);
      expect(metadata.brokers).toEqual([{ id: 1, host: "127.0.0.1", port: listener.port }]);
      expect(verified).toBe(true);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("rejects when the password does not derive the offered proof", async () => {
    let verified: boolean | undefined;
    const listener = scramListener({
      port: () => listener.port,
      password: "correct-pass",
      onProofVerified: (ok) => (verified = ok),
    });
    const kafka = new Kafka({
      brokers: [`127.0.0.1:${listener.port}`],
      sasl: { mechanism: "scram-sha-256", username: "user", password: "wrong-pass" },
      retry: { maxRetries: 0 },
    });
    try {
      await expect(kafka.admin().metadata(["events"])).rejects.toThrow();
      expect(verified).toBe(false);
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  });

  test("sasl config rejects unknown mechanisms", () => {
    expect(
      () =>
        new Kafka({
          brokers: ["127.0.0.1:9092"],
          // @ts-expect-error exercising runtime validation of the union
          sasl: { mechanism: "scram-md5", username: "u", password: "p" },
        }),
    ).toThrow();
  });
});
