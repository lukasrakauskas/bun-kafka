import { KafkaError, KafkaErrorCode } from "../../errors.ts";
import {
  type RequestBody,
  type ResponseBody,
  readSaslAuthenticateResponse,
  readSaslHandshakeResponse,
  writeSaslAuthenticateRequest,
  writeSaslHandshakeRequest,
} from "../../protocol/index.ts";
import { arrayBufferBytes, isFunction, isString, requiredValue } from "../../type-guards.ts";
import { API_SASL_AUTHENTICATE, API_SASL_HANDSHAKE } from "../shared.ts";
import type { BunKafkaSasl } from "./types.ts";

const SASL_REAUTH_FRACTION = 0.8;
const SCRAM_NONCE_BYTES = 18;
const SHA256_BITS = 256;
const SHA512_BITS = 512;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

type SendRequest = (
  socket: Bun.Socket,
  apiKey: number,
  apiVersion: number,
  body: RequestBody,
  timeoutMs: number,
) => Promise<ResponseBody>;
type FailConnection = (error: Error, socket: Bun.Socket) => void;

function bytes(value: string): Uint8Array {
  return textEncoder.encode(value);
}

function base64(value: Uint8Array): string {
  let text = "";
  for (const byte of value) {
    text += String.fromCharCode(byte);
  }
  return btoa(text);
}

function fromBase64(value: string): Uint8Array {
  const text = atob(value);
  const result = new Uint8Array(text.length);
  for (let index = 0; index < text.length; index++) {
    result[index] = text.charCodeAt(index);
  }
  return result;
}

async function digest(value: Uint8Array, algorithm: "SHA-256" | "SHA-512") {
  return new Uint8Array(await crypto.subtle.digest(algorithm, arrayBufferBytes(value)));
}

async function hmac(
  key: Uint8Array,
  value: string | Uint8Array,
  algorithm: "SHA-256" | "SHA-512",
): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    arrayBufferBytes(key),
    { name: "HMAC", hash: algorithm },
    false,
    ["sign"],
  );
  return new Uint8Array(
    await crypto.subtle.sign(
      "HMAC",
      cryptoKey,
      isString(value) ? arrayBufferBytes(bytes(value)) : arrayBufferBytes(value),
    ),
  );
}

async function pbkdf2(
  password: string,
  salt: Uint8Array,
  iterations: number,
  algorithm: "SHA-256" | "SHA-512",
): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey(
    "raw",
    arrayBufferBytes(bytes(password)),
    "PBKDF2",
    false,
    ["deriveBits"],
  );
  return new Uint8Array(
    await crypto.subtle.deriveBits(
      { name: "PBKDF2", salt: arrayBufferBytes(salt), iterations, hash: algorithm },
      key,
      algorithm === "SHA-256" ? SHA256_BITS : SHA512_BITS,
    ),
  );
}

function xor(left: Uint8Array, right: Uint8Array): Uint8Array {
  const result = new Uint8Array(left.length);
  for (let index = 0; index < left.length; index++) {
    result[index] = requiredValue(left[index]) ^ requiredValue(right[index]);
  }
  return result;
}

function parseFields(value: string): Map<string, string> {
  return new Map(
    value.split(",").map((field) => {
      const at = field.indexOf("=");
      return [field.slice(0, at), field.slice(at + 1)];
    }),
  );
}

export class SaslSession {
  #authenticated = false;
  #authenticating?: Promise<void>;
  #reauthTimer?: ReturnType<typeof setTimeout>;
  #sessionLifetimeMs = 0;

  constructor(
    readonly address: string,
    readonly options: BunKafkaSasl,
    readonly requestTimeoutMs: number,
    readonly send: SendRequest,
    readonly failConnection: FailConnection,
  ) {}

  async authenticate(socket: Bun.Socket, timeoutMs: number): Promise<void> {
    if (this.#authenticated) {
      return;
    }
    if (this.#authenticating) {
      return this.#authenticating;
    }
    this.#authenticating = this.#startAuthentication(socket, timeoutMs).finally(() => {
      this.#authenticating = undefined;
    });
    return this.#authenticating;
  }

  reset(): void {
    if (this.#reauthTimer) {
      clearTimeout(this.#reauthTimer);
    }
    this.#reauthTimer = undefined;
    this.#authenticated = false;
    this.#authenticating = undefined;
    this.#sessionLifetimeMs = 0;
  }

  async #startAuthentication(socket: Bun.Socket, timeoutMs: number): Promise<void> {
    const response = await this.send(
      socket,
      API_SASL_HANDSHAKE,
      1,
      writeSaslHandshakeRequest(this.options.mechanism.toUpperCase()),
      timeoutMs,
    );
    const { error } = readSaslHandshakeResponse(response);
    if (error) {
      throw new KafkaError(error, `SASL handshake failed on ${this.address}`);
    }
    await this.#authenticateMechanism(socket, timeoutMs);
    this.#authenticated = true;
  }

  async #authenticateMechanism(socket: Bun.Socket, timeoutMs: number): Promise<void> {
    if (this.options.mechanism === "plain") {
      return this.#authenticatePlain(socket, this.options, timeoutMs);
    }
    if (this.options.mechanism === "oauthbearer") {
      return this.#authenticateOauth(socket, this.options, timeoutMs);
    }
    await this.#scram(socket, this.options, timeoutMs);
  }

  async #authenticatePlain(
    socket: Bun.Socket,
    options: Extract<BunKafkaSasl, { mechanism: "plain" }>,
    timeoutMs: number,
  ): Promise<void> {
    const authentication = await this.#sasl(
      socket,
      bytes(`\0${options.username}\0${options.password}`),
      timeoutMs,
    );
    if (authentication.byteLength) {
      throw new KafkaError(-1, `Unexpected SASL/PLAIN challenge from ${this.address}`);
    }
  }

  async #authenticateOauth(
    socket: Bun.Socket,
    options: Extract<BunKafkaSasl, { mechanism: "oauthbearer" }>,
    timeoutMs: number,
  ): Promise<void> {
    const token = isFunction(options.token) ? await options.token() : options.token;
    if (!token) {
      throw new KafkaError(-1, `SASL/OAUTHBEARER token is empty for ${this.address}`);
    }
    const authentication = await this.#sasl(
      socket,
      bytes(`n,,\u0001auth=Bearer ${token}\u0001\u0001`),
      timeoutMs,
    );
    if (authentication.byteLength) {
      throw new KafkaError(-1, `Unexpected SASL/OAUTHBEARER challenge from ${this.address}`);
    }
    if (this.#sessionLifetimeMs > 0) {
      this.#scheduleReauthentication(socket);
    }
  }

  async #sasl(socket: Bun.Socket, payload: Uint8Array, timeoutMs: number): Promise<Uint8Array> {
    const response = await this.send(
      socket,
      API_SASL_AUTHENTICATE,
      1,
      writeSaslAuthenticateRequest(payload),
      timeoutMs,
    );
    const { error, message, authBytes, sessionLifetimeMs } = readSaslAuthenticateResponse(response);
    this.#sessionLifetimeMs = Number(sessionLifetimeMs);
    if (error) {
      throw new KafkaError(error, message ?? `SASL authentication failed on ${this.address}`);
    }
    return authBytes;
  }

  #scheduleReauthentication(socket: Bun.Socket): void {
    if (this.#reauthTimer) {
      clearTimeout(this.#reauthTimer);
    }
    const delay = Math.max(0, Math.floor(this.#sessionLifetimeMs * SASL_REAUTH_FRACTION));
    this.#reauthTimer = setTimeout(() => void this.#reauthenticate(socket), delay);
    this.#reauthTimer.unref?.();
  }

  async #reauthenticate(socket: Bun.Socket): Promise<void> {
    try {
      if (this.options.mechanism !== "oauthbearer") {
        return;
      }
      const token = isFunction(this.options.token)
        ? await this.options.token()
        : this.options.token;
      if (!token) {
        throw new KafkaError(
          -1,
          `SASL/OAUTHBEARER reauthentication token is empty for ${this.address}`,
        );
      }
      await this.#sasl(
        socket,
        bytes(`n,,\u0001auth=Bearer ${token}\u0001\u0001`),
        this.requestTimeoutMs,
      );
      if (this.#sessionLifetimeMs > 0) {
        this.#scheduleReauthentication(socket);
      }
    } catch (error) {
      this.failConnection(
        new KafkaError(
          KafkaErrorCode.SASL_AUTHENTICATION_FAILED,
          `SASL reauthentication failed on ${this.address}: ${String(error)}`,
          { fatal: true },
        ),
        socket,
      );
    }
  }

  async #scram(
    socket: Bun.Socket,
    options: Extract<BunKafkaSasl, { mechanism: "scram-sha-256" | "scram-sha-512" }>,
    timeoutMs: number,
  ): Promise<void> {
    const algorithm = options.mechanism === "scram-sha-256" ? "SHA-256" : "SHA-512";
    const nonceBytes = new Uint8Array(SCRAM_NONCE_BYTES);
    crypto.getRandomValues(nonceBytes);
    const nonce = base64(nonceBytes);
    const escapedUser = options.username.replaceAll("=", "=3D").replaceAll(",", "=2C");
    const clientFirstBare = `n=${escapedUser},r=${nonce}`;
    const serverFirstBytes = await this.#sasl(socket, bytes(`n,,${clientFirstBare}`), timeoutMs);
    const serverFirst = textDecoder.decode(serverFirstBytes);
    const fields = parseFields(serverFirst);
    const serverNonce = fields.get("r");
    const salt = fields.get("s");
    const iterations = Number(fields.get("i"));
    if (
      !serverNonce?.startsWith(nonce) ||
      !salt ||
      !Number.isSafeInteger(iterations) ||
      iterations < 1
    ) {
      throw new KafkaError(-1, `Invalid SCRAM server-first message from ${this.address}`);
    }
    const clientFinalWithoutProof = `c=biws,r=${serverNonce}`;
    const authMessage = `${clientFirstBare},${serverFirst},${clientFinalWithoutProof}`;
    const saltedPassword = await pbkdf2(options.password, fromBase64(salt), iterations, algorithm);
    const clientKey = await hmac(saltedPassword, "Client Key", algorithm);
    const storedKey = await digest(clientKey, algorithm);
    const clientSignature = await hmac(storedKey, authMessage, algorithm);
    const clientProof = base64(xor(clientKey, clientSignature));
    const serverKey = await hmac(saltedPassword, "Server Key", algorithm);
    const serverSignature = base64(await hmac(serverKey, authMessage, algorithm));
    const serverFinalBytes = await this.#sasl(
      socket,
      bytes(`${clientFinalWithoutProof},p=${clientProof}`),
      timeoutMs,
    );
    const serverFinal = parseFields(textDecoder.decode(serverFinalBytes));
    if (serverFinal.get("v") !== serverSignature) {
      throw new KafkaError(-1, `SCRAM server signature mismatch from ${this.address}`);
    }
  }
}
