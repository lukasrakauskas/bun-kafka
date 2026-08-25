import { KafkaError } from "../errors.ts";
import { Reader, Writer } from "./protocol.ts";

export type BunKafkaTls = boolean | Bun.TLSOptions;
export type BunKafkaSasl =
  | { mechanism: "plain"; username: string; password: string }
  | { mechanism: "scram-sha-256" | "scram-sha-512"; username: string; password: string }
  | { mechanism: "oauthbearer"; token: string | (() => string | Promise<string>) };

export type ConnectionOptions = {
  clientId: string;
  requestTimeoutMs: number;
  connectTimeoutMs: number;
  tls?: BunKafkaTls;
  sasl?: BunKafkaSasl;
  maxResponseBytes: number;
};

type Pending = {
  resolve: (reader: Reader) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function bytes(value: string): Uint8Array {
  return textEncoder.encode(value);
}

function base64(value: Uint8Array): string {
  let text = "";
  for (const byte of value) text += String.fromCharCode(byte);
  return btoa(text);
}

function fromBase64(value: string): Uint8Array {
  const text = atob(value);
  const result = new Uint8Array(text.length);
  for (let i = 0; i < text.length; i++) result[i] = text.charCodeAt(i);
  return result;
}

async function digest(value: Uint8Array, algorithm: "SHA-256" | "SHA-512"): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest(algorithm, value as Uint8Array<ArrayBuffer>));
}

async function hmac(key: Uint8Array, value: string | Uint8Array, algorithm: "SHA-256" | "SHA-512"): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey("raw", key as Uint8Array<ArrayBuffer>, { name: "HMAC", hash: algorithm }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", cryptoKey, typeof value === "string" ? bytes(value) as Uint8Array<ArrayBuffer> : value as Uint8Array<ArrayBuffer>));
}

async function pbkdf2(password: string, salt: Uint8Array, iterations: number, algorithm: "SHA-256" | "SHA-512"): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey("raw", bytes(password) as Uint8Array<ArrayBuffer>, "PBKDF2", false, ["deriveBits"]);
  return new Uint8Array(await crypto.subtle.deriveBits({ name: "PBKDF2", salt: salt as Uint8Array<ArrayBuffer>, iterations, hash: algorithm }, key, algorithm === "SHA-256" ? 256 : 512));
}

function xor(left: Uint8Array, right: Uint8Array): Uint8Array {
  const result = new Uint8Array(left.length);
  for (let i = 0; i < left.length; i++) result[i] = left[i]! ^ right[i]!;
  return result;
}

function parseFields(value: string): Map<string, string> {
  return new Map(value.split(",").map((field) => {
    const at = field.indexOf("=");
    return [field.slice(0, at), field.slice(at + 1)];
  }));
}

function parseAddress(address: string): { hostname: string; port: number } {
  const url = new URL(address.includes("://") ? address : `kafka://${address}`);
  const port = Number(url.port || 9092);
  const hostname = url.hostname.replace(/^\[|\]$/g, "");
  if (!hostname || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new TypeError(`Invalid Kafka broker: ${address}`);
  }
  return { hostname, port };
}

export class Connection {
  readonly address: string;
  #options: ConnectionOptions;
  #socket?: Bun.Socket;
  #connecting?: Promise<Bun.Socket>;
  #ignoredSockets = new WeakSet<Bun.Socket>();
  #pending = new Map<number, Pending>();
  #correlation = 0;
  #header = new Uint8Array(4);
  #headerOffset = 0;
  #frame?: Uint8Array;
  #frameOffset = 0;
  #closed = false;
  #requests = 0;
  #bytesSent = 0;
  #bytesReceived = 0;
  #authenticated = false;
  #reauthTimer?: ReturnType<typeof setTimeout>;
  #sessionLifetimeMs = 0;
  #authenticating?: Promise<void>;
  #versions?: Map<number, { min: number; max: number }>;
  #negotiating?: Promise<void>;

  constructor(address: string, options: ConnectionOptions) {
    parseAddress(address);
    this.address = address;
    this.#options = options;
  }

  async request(apiKey: number, apiVersion: number, body: Writer, timeoutMs = this.#options.requestTimeoutMs): Promise<Reader> {
    if (this.#closed) throw new Error("Kafka connection is closed");
    const socket = await this.#connect();
    await this.#prepare(socket, apiKey, apiVersion, timeoutMs);
    return this.#send(socket, apiKey, apiVersion, body, timeoutMs);
  }

  /** Send a request whose response never arrives (acks=0 Produce). */
  async sendOnly(apiKey: number, apiVersion: number, body: Writer, timeoutMs = this.#options.requestTimeoutMs): Promise<void> {
    if (this.#closed) throw new Error("Kafka connection is closed");
    const socket = await this.#connect();
    await this.#prepare(socket, apiKey, apiVersion, timeoutMs);
    const correlation = this.#correlation = (this.#correlation + 1) & 0x7fffffff;
    const frame = new Writer();
    frame.i32(0).i16(apiKey).i16(apiVersion).i32(correlation).string(this.#options.clientId).raw(body.result());
    frame.patchI32(0, frame.length - 4);
    this.#requests++;
    this.#bytesSent += frame.length;
    if (socket.write(frame.result()) < 0) {
      throw new KafkaError(-1, `Could not write to Kafka broker ${this.address}`, { retriable: true });
    }
  }

  /** Counters for statistics reporting. */
  get stats(): { requests: number; bytesSent: number; bytesReceived: number } {
    return { requests: this.#requests, bytesSent: this.#bytesSent, bytesReceived: this.#bytesReceived };
  }

  async #prepare(socket: Bun.Socket, apiKey: number, apiVersion: number, timeoutMs: number): Promise<void> {
    if (apiKey !== 18) await this.#negotiate(socket, timeoutMs);
    if (this.#options.sasl && apiKey !== 17 && apiKey !== 36) await this.#authenticate(socket, timeoutMs);
    const supported = this.#versions?.get(apiKey);
    if (supported && (apiVersion < supported.min || apiVersion > supported.max)) {
      throw new KafkaError(35, `Kafka broker ${this.address} does not support API ${apiKey} version ${apiVersion} (${supported.min}-${supported.max})`);
    }
  }

  async #negotiate(socket: Bun.Socket, timeoutMs: number): Promise<void> {
    if (this.#versions) return;
    if (this.#negotiating) return this.#negotiating;
    this.#negotiating = (async () => {
      const response = await this.#send(socket, 18, 0, new Writer(), timeoutMs);
      const error = response.i16();
      if (error) throw new KafkaError(error, `ApiVersions negotiation failed on ${this.address}`);
      this.#versions = new Map(response.array((reader) => [reader.i16(), { min: reader.i16(), max: reader.i16() }] as const));
    })().finally(() => { this.#negotiating = undefined; });
    return this.#negotiating;
  }

  async #authenticate(socket: Bun.Socket, timeoutMs: number): Promise<void> {
    if (this.#authenticated) return;
    if (this.#authenticating) return this.#authenticating;
    const sasl = this.#options.sasl!;
    this.#authenticating = (async () => {
      const handshake = await this.#send(socket, 17, 1, new Writer().string(sasl.mechanism.toUpperCase()), timeoutMs);
      const handshakeError = handshake.i16();
      if (handshakeError) throw new KafkaError(handshakeError, `SASL handshake failed on ${this.address}`);
      handshake.array((reader) => reader.string());
      if (sasl.mechanism === "plain") {
        const authentication = await this.#sasl(socket, bytes(`\0${sasl.username}\0${sasl.password}`), timeoutMs);
        if (authentication.byteLength) throw new KafkaError(-1, `Unexpected SASL/PLAIN challenge from ${this.address}`);
      } else if (sasl.mechanism === "oauthbearer") {
        const token = typeof sasl.token === "function" ? await sasl.token() : sasl.token;
        if (!token) throw new KafkaError(-1, `SASL/OAUTHBEARER token is empty for ${this.address}`);
        const authentication = await this.#sasl(socket, bytes(`n,,\u0001auth=Bearer ${token}\u0001\u0001`), timeoutMs);
        if (authentication.byteLength) throw new KafkaError(-1, `Unexpected SASL/OAUTHBEARER challenge from ${this.address}`);
        // Timed reauthentication (KIP-368): re-run SASL before the token expires.
        if (this.#sessionLifetimeMs > 0) this.scheduleReauthentication(socket, timeoutMs);
      } else {
        await this.#scram(socket, sasl, timeoutMs);
      }
      this.#authenticated = true;
    })().finally(() => { this.#authenticating = undefined; });
    return this.#authenticating;
  }

  async #sasl(socket: Bun.Socket, payload: Uint8Array, timeoutMs: number): Promise<Uint8Array> {
    const response = await this.#send(socket, 36, 1, new Writer().bytes(payload), timeoutMs);
    const error = response.i16();
    const message = response.string();
    const authBytes = response.bytes() ?? new Uint8Array();
    this.#sessionLifetimeMs = Number(response.i64());
    if (error) throw new KafkaError(error, message ?? `SASL authentication failed on ${this.address}`);
    return authBytes;
  }

  /** Re-run SASL on a live connection before the advertised session expires (KIP-368). */
  scheduleReauthentication(socket: Bun.Socket, timeoutMs: number): void {
    if (this.#reauthTimer) clearTimeout(this.#reauthTimer);
    const delay = Math.max(0, Math.floor(this.#sessionLifetimeMs * 0.8));
    this.#reauthTimer = setTimeout(() => {
      void this.reauthenticate(socket).catch(() => {});
    }, delay);
    this.#reauthTimer.unref?.();
  }

  private async reauthenticate(socket: Bun.Socket): Promise<void> {
    try {
      const sasl = this.#options.sasl;
      if (sasl?.mechanism !== "oauthbearer") return; // Other mechanisms cannot re-authenticate mid-session.
      const token = typeof sasl.token === "function" ? await sasl.token() : sasl.token;
      if (!token) throw new KafkaError(-1, `SASL/OAUTHBEARER reauthentication token is empty for ${this.address}`);
      await this.#sasl(socket, bytes(`n,,\u0001auth=Bearer ${token}\u0001\u0001`), this.#options.requestTimeoutMs);
      if (this.#sessionLifetimeMs > 0) this.scheduleReauthentication(socket, this.#options.requestTimeoutMs);
    } catch (error) {
      this.#fail(new KafkaError(58, `SASL reauthentication failed on ${this.address}: ${String(error)}`, { fatal: true }), socket);
    }
  }

  async #scram(socket: Bun.Socket, sasl: Extract<BunKafkaSasl, { mechanism: "scram-sha-256" | "scram-sha-512" }>, timeoutMs: number): Promise<void> {
    const algorithm = sasl.mechanism === "scram-sha-256" ? "SHA-256" : "SHA-512";
    const nonceBytes = new Uint8Array(18);
    crypto.getRandomValues(nonceBytes);
    const nonce = base64(nonceBytes);
    const escapedUser = sasl.username.replaceAll("=", "=3D").replaceAll(",", "=2C");
    const clientFirstBare = `n=${escapedUser},r=${nonce}`;
    const clientFirst = `n,,${clientFirstBare}`;
    const serverFirstBytes = await this.#sasl(socket, bytes(clientFirst), timeoutMs);
    const serverFirst = textDecoder.decode(serverFirstBytes);
    const serverFields = parseFields(serverFirst);
    const serverNonce = serverFields.get("r");
    const salt = serverFields.get("s");
    const iterations = Number(serverFields.get("i"));
    if (!serverNonce?.startsWith(nonce) || !salt || !Number.isSafeInteger(iterations) || iterations < 1) {
      throw new KafkaError(-1, `Invalid SCRAM server-first message from ${this.address}`);
    }
    const clientFinalWithoutProof = `c=biws,r=${serverNonce}`;
    const authMessage = `${clientFirstBare},${serverFirst},${clientFinalWithoutProof}`;
    const saltedPassword = await pbkdf2(sasl.password, fromBase64(salt), iterations, algorithm);
    const clientKey = await hmac(saltedPassword, "Client Key", algorithm);
    const storedKey = await digest(clientKey, algorithm);
    const clientSignature = await hmac(storedKey, authMessage, algorithm);
    const clientProof = base64(xor(clientKey, clientSignature));
    const serverKey = await hmac(saltedPassword, "Server Key", algorithm);
    const serverSignature = base64(await hmac(serverKey, authMessage, algorithm));
    const serverFinalBytes = await this.#sasl(socket, bytes(`${clientFinalWithoutProof},p=${clientProof}`), timeoutMs);
    const serverFinal = parseFields(textDecoder.decode(serverFinalBytes));
    if (serverFinal.get("v") !== serverSignature) throw new KafkaError(-1, `SCRAM server signature mismatch from ${this.address}`);
  }

  #send(socket: Bun.Socket, apiKey: number, apiVersion: number, body: Writer, timeoutMs: number): Promise<Reader> {
    const correlation = this.#correlation = (this.#correlation + 1) & 0x7fffffff;
    const frame = new Writer();
    frame.i32(0).i16(apiKey).i16(apiVersion).i32(correlation).string(this.#options.clientId).raw(body.result());
    frame.patchI32(0, frame.length - 4);
    this.#requests++;
    this.#bytesSent += frame.length;

    return new Promise<Reader>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(correlation);
        reject(new KafkaError(-1, `Kafka request ${apiKey} timed out after ${timeoutMs}ms`, { retriable: true }));
      }, timeoutMs);
      this.#pending.set(correlation, { resolve, reject, timer });
      const written = socket.write(frame.result());
      if (written < 0) {
        clearTimeout(timer);
        this.#pending.delete(correlation);
        reject(new KafkaError(-1, `Could not write to Kafka broker ${this.address}`, { retriable: true }));
      }
    });
  }

  async #connect(): Promise<Bun.Socket> {
    if (this.#socket) return this.#socket;
    if (this.#connecting) return this.#connecting;
    const { hostname, port } = parseAddress(this.address);
    let timedOut = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const attempt = Bun.connect({
      hostname,
      port,
      tls: this.#options.tls,
      socket: {
        data: (_socket, data) => this.#onData(new Uint8Array(data)),
        close: (socket) => this.#fail(new KafkaError(-1, `Kafka broker ${this.address} closed the connection`, { retriable: true }), socket),
        error: (socket, error) => this.#fail(error instanceof Error ? error : new Error(String(error)), socket),
      },
    }).then((socket) => {
      if (this.#closed || timedOut) {
        this.#ignoredSockets.add(socket);
        socket.end();
        throw new Error("Kafka connection is closed");
      }
      this.#socket = socket;
      return socket;
    }, (error) => {
      throw new KafkaError(-1, `Could not connect to Kafka broker ${this.address}: ${error}`, { retriable: true });
    });
    const connecting = Promise.race([
      attempt,
      new Promise<Bun.Socket>((_, reject) => {
        timer = setTimeout(() => {
          timedOut = true;
          reject(new KafkaError(-1, `Kafka broker ${this.address} connect timed out after ${this.#options.connectTimeoutMs}ms`, { retriable: true }));
        }, this.#options.connectTimeoutMs);
      }),
    ]).finally(() => {
      if (timer) clearTimeout(timer);
      if (this.#connecting === connecting) this.#connecting = undefined;
    });
    this.#connecting = connecting;
    return connecting;
  }

  #onData(chunk: Uint8Array): void {
    let offset = 0;
    while (offset < chunk.byteLength) {
      if (!this.#frame) {
        const count = Math.min(4 - this.#headerOffset, chunk.byteLength - offset);
        this.#header.set(chunk.subarray(offset, offset + count), this.#headerOffset);
        this.#headerOffset += count;
        offset += count;
        if (this.#headerOffset < 4) return;
        const size = new DataView(this.#header.buffer).getInt32(0);
        this.#headerOffset = 0;
        if (size < 4 || size > this.#options.maxResponseBytes) {
          this.#fail(new KafkaError(-1, `Invalid Kafka response size ${size}`));
          return;
        }
        this.#frame = new Uint8Array(size);
        this.#frameOffset = 0;
      }

      const count = Math.min(this.#frame.byteLength - this.#frameOffset, chunk.byteLength - offset);
      this.#frame.set(chunk.subarray(offset, offset + count), this.#frameOffset);
      this.#frameOffset += count;
      offset += count;
      if (this.#frameOffset === this.#frame.byteLength) {
        const frame = this.#frame;
        this.#frame = undefined;
        this.#frameOffset = 0;
        this.#bytesReceived += frame.byteLength;
        const correlation = new DataView(frame.buffer, frame.byteOffset, 4).getInt32(0);
        const pending = this.#pending.get(correlation);
        if (pending) {
          clearTimeout(pending.timer);
          this.#pending.delete(correlation);
          pending.resolve(new Reader(frame.subarray(4)));
        }
      }
    }
  }

  #fail(error: Error, socket?: Bun.Socket): void {
    if (socket && (this.#ignoredSockets.has(socket) || this.#socket && socket !== this.#socket)) return;
    if (this.#reauthTimer) clearTimeout(this.#reauthTimer);
    this.#reauthTimer = undefined;
    this.#socket = undefined;
    this.#connecting = undefined;
    this.#authenticated = false;
    this.#authenticating = undefined;
    this.#versions = undefined;
    this.#negotiating = undefined;
    this.#headerOffset = 0;
    this.#frame = undefined;
    this.#frameOffset = 0;
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.#pending.clear();
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#reauthTimer) clearTimeout(this.#reauthTimer);
    this.#reauthTimer = undefined;
    this.#socket?.end();
    this.#fail(new Error("Kafka connection is closed"));
  }
}
