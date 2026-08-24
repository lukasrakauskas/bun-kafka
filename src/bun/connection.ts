import { KafkaError } from "../errors.ts";
import { Reader, Writer } from "./protocol.ts";

export type BunKafkaTls = boolean | Bun.TLSOptions;

export type ConnectionOptions = {
  clientId: string;
  requestTimeoutMs: number;
  tls?: BunKafkaTls;
  maxResponseBytes: number;
};

type Pending = {
  resolve: (reader: Reader) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

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
  #pending = new Map<number, Pending>();
  #correlation = 0;
  #incoming = new Uint8Array();
  #closed = false;

  constructor(address: string, options: ConnectionOptions) {
    parseAddress(address);
    this.address = address;
    this.#options = options;
  }

  async request(apiKey: number, apiVersion: number, body: Writer, timeoutMs = this.#options.requestTimeoutMs): Promise<Reader> {
    if (this.#closed) throw new Error("Kafka connection is closed");
    const socket = await this.#connect();
    const correlation = this.#correlation = (this.#correlation + 1) & 0x7fffffff;
    const frame = new Writer();
    frame.i32(0).i16(apiKey).i16(apiVersion).i32(correlation).string(this.#options.clientId).raw(body.result());
    frame.patchI32(0, frame.length - 4);

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
    this.#connecting = Bun.connect({
      hostname,
      port,
      tls: this.#options.tls,
      socket: {
        data: (_socket, data) => this.#onData(new Uint8Array(data)),
        close: () => this.#fail(new KafkaError(-1, `Kafka broker ${this.address} closed the connection`, { retriable: true })),
        error: (_socket, error) => this.#fail(error instanceof Error ? error : new Error(String(error))),
      },
    }).then((socket) => {
      if (this.#closed) {
        socket.end();
        throw new Error("Kafka connection is closed");
      }
      this.#socket = socket;
      this.#connecting = undefined;
      return socket;
    }, (error) => {
      this.#connecting = undefined;
      throw new KafkaError(-1, `Could not connect to Kafka broker ${this.address}: ${error}`, { retriable: true });
    });
    return this.#connecting;
  }

  #onData(chunk: Uint8Array): void {
    // ponytail: copying partial frames is simple; use a ring buffer if profiles show large fragmented responses.
    const incoming = new Uint8Array(this.#incoming.byteLength + chunk.byteLength);
    incoming.set(this.#incoming);
    incoming.set(chunk, this.#incoming.byteLength);
    this.#incoming = incoming;

    let consumed = 0;
    while (this.#incoming.byteLength - consumed >= 4) {
      const view = new DataView(this.#incoming.buffer, this.#incoming.byteOffset + consumed);
      const size = view.getInt32(0);
      if (size < 4 || size > this.#options.maxResponseBytes) {
        this.#fail(new KafkaError(-1, `Invalid Kafka response size ${size}`));
        return;
      }
      if (this.#incoming.byteLength - consumed < size + 4) break;
      const frame = this.#incoming.subarray(consumed + 4, consumed + 4 + size);
      const correlation = new DataView(frame.buffer, frame.byteOffset, 4).getInt32(0);
      const pending = this.#pending.get(correlation);
      if (pending) {
        clearTimeout(pending.timer);
        this.#pending.delete(correlation);
        pending.resolve(new Reader(frame.subarray(4)));
      }
      consumed += size + 4;
    }
    if (consumed) this.#incoming = this.#incoming.slice(consumed);
  }

  #fail(error: Error): void {
    this.#socket = undefined;
    this.#connecting = undefined;
    this.#incoming = new Uint8Array();
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.#pending.clear();
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#socket?.end();
    this.#fail(new Error("Kafka connection is closed"));
  }
}
