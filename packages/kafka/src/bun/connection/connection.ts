import { KafkaError, KafkaErrorCode } from "../../errors.ts";
import {
  type RequestBody,
  type ResponseBody,
  readApiVersionsResponse,
  writeEmptyRequest,
} from "../../protocol/index.ts";
import {
  API_API_VERSIONS,
  API_SASL_AUTHENTICATE,
  API_SASL_HANDSHAKE,
  DEFAULT_BROKER_PORT,
  MAX_TCP_PORT,
} from "../shared.ts";
import { ConnectionMetrics } from "./metrics.ts";
import { RequestTracker } from "./requests.ts";
import { ResponseFramer } from "./response-framer.ts";
import { SaslSession } from "./sasl.ts";
import type { ConnectionOptions } from "./types.ts";

const CLOSED_MESSAGE = "Kafka connection is closed";

function parseAddress(address: string) {
  const url = new URL(address.includes("://") ? address : `kafka://${address}`);
  const port = Number(url.port || DEFAULT_BROKER_PORT);
  const hostname = url.hostname.replace(/^\[|\]$/g, "");
  if (!hostname || !Number.isInteger(port) || port < 1 || port > MAX_TCP_PORT) {
    throw new TypeError(`Invalid Kafka broker: ${address}`);
  }
  return { hostname, port };
}

export class Connection {
  readonly address: string;
  #socket?: Bun.Socket;
  #connecting?: Promise<Bun.Socket>;
  #ignoredSockets = new WeakSet<Bun.Socket>();
  #closed = false;
  #versions?: Map<number, { min: number; max: number }>;
  #negotiating?: Promise<void>;
  #metrics = new ConnectionMetrics();
  #requests: RequestTracker;
  #framer: ResponseFramer;
  #sasl?: SaslSession;

  constructor(
    address: string,
    readonly options: ConnectionOptions,
  ) {
    parseAddress(address);
    this.address = address;
    this.#requests = new RequestTracker(address, options.clientId, this.#metrics);
    this.#framer = new ResponseFramer(options.maxResponseBytes);
    if (options.sasl) {
      this.#sasl = new SaslSession(
        address,
        options.sasl,
        options.requestTimeoutMs,
        (socket, apiKey, apiVersion, body, timeoutMs) =>
          this.#requests.request(socket, apiKey, apiVersion, body, timeoutMs),
        (error, socket) => this.#fail(error, socket),
      );
    }
  }

  async request(
    apiKey: number,
    apiVersion: number,
    body: RequestBody,
    timeoutMs = this.options.requestTimeoutMs,
    flexible = false,
  ): Promise<ResponseBody> {
    this.#assertOpen();
    const socket = await this.#connect();
    await this.#prepare(socket, apiKey, apiVersion, timeoutMs);
    return this.#requests.request(socket, apiKey, apiVersion, body, timeoutMs, flexible);
  }

  /** Send a request whose response never arrives (acks=0 Produce). */
  async sendOnly(
    apiKey: number,
    apiVersion: number,
    body: RequestBody,
    timeoutMs = this.options.requestTimeoutMs,
  ): Promise<void> {
    this.#assertOpen();
    const socket = await this.#connect();
    await this.#prepare(socket, apiKey, apiVersion, timeoutMs);
    this.#requests.sendOnly(socket, apiKey, apiVersion, body);
  }

  /** Counters for statistics reporting. */
  get stats() {
    return this.#metrics.stats;
  }

  async #prepare(
    socket: Bun.Socket,
    apiKey: number,
    apiVersion: number,
    timeoutMs: number,
  ): Promise<void> {
    if (apiKey !== API_API_VERSIONS) {
      await this.#negotiate(socket, timeoutMs);
    }
    if (this.#sasl && apiKey !== API_SASL_HANDSHAKE && apiKey !== API_SASL_AUTHENTICATE) {
      await this.#sasl.authenticate(socket, timeoutMs);
    }
    const supported = this.#versions?.get(apiKey);
    if (supported && (apiVersion < supported.min || apiVersion > supported.max)) {
      throw new KafkaError(
        KafkaErrorCode.UNSUPPORTED_VERSION,
        `Kafka broker ${this.address} does not support API ${apiKey} version ${apiVersion} (${supported.min}-${supported.max})`,
      );
    }
  }

  async #negotiate(socket: Bun.Socket, timeoutMs: number): Promise<void> {
    if (this.#versions) {
      return;
    }
    if (this.#negotiating) {
      return this.#negotiating;
    }
    this.#negotiating = this.#requests
      .request(socket, API_API_VERSIONS, 0, writeEmptyRequest(), timeoutMs)
      .then((response) => {
        this.#versions = readApiVersionsResponse(response);
      })
      .finally(() => {
        this.#negotiating = undefined;
      });
    return this.#negotiating;
  }

  async #connect(): Promise<Bun.Socket> {
    if (this.#socket) {
      return this.#socket;
    }
    if (this.#connecting) {
      return this.#connecting;
    }
    const { hostname, port } = parseAddress(this.address);
    let timedOut = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const attempt = Bun.connect({
      hostname,
      port,
      tls: this.options.tls,
      socket: {
        data: (_socket, data) => this.#onData(new Uint8Array(data)),
        close: (socket) =>
          this.#fail(
            new KafkaError(-1, `Kafka broker ${this.address} closed the connection`, {
              retriable: true,
            }),
            socket,
          ),
        error: (socket, error) =>
          this.#fail(error instanceof Error ? error : new Error(String(error)), socket),
      },
    }).then(
      (socket) => {
        if (this.#closed || timedOut) {
          this.#ignoredSockets.add(socket);
          socket.end();
          throw new Error(CLOSED_MESSAGE);
        }
        this.#socket = socket;
        return socket;
      },
      (error) => {
        throw new KafkaError(-1, `Could not connect to Kafka broker ${this.address}: ${error}`, {
          retriable: true,
        });
      },
    );
    const connecting = Promise.race([
      attempt,
      new Promise<Bun.Socket>((_, reject) => {
        timer = setTimeout(() => {
          timedOut = true;
          reject(
            new KafkaError(
              -1,
              `Kafka broker ${this.address} connect timed out after ${this.options.connectTimeoutMs}ms`,
              { retriable: true },
            ),
          );
        }, this.options.connectTimeoutMs);
      }),
    ]).finally(() => {
      if (timer) {
        clearTimeout(timer);
      }
      if (this.#connecting === connecting) {
        this.#connecting = undefined;
      }
    });
    this.#connecting = connecting;
    return connecting;
  }

  #onData(chunk: Uint8Array): void {
    try {
      for (const frame of this.#framer.push(chunk)) {
        this.#requests.receive(frame);
      }
    } catch (error) {
      this.#fail(error instanceof Error ? error : new Error(String(error)));
    }
  }

  #fail(error: Error, socket?: Bun.Socket): void {
    if (socket && (this.#ignoredSockets.has(socket) || (this.#socket && socket !== this.#socket))) {
      return;
    }
    this.#socket = undefined;
    this.#connecting = undefined;
    this.#versions = undefined;
    this.#negotiating = undefined;
    this.#sasl?.reset();
    this.#framer.reset();
    this.#requests.fail(error);
  }

  #assertOpen(): void {
    if (this.#closed) {
      throw new Error(CLOSED_MESSAGE);
    }
  }

  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#socket?.end();
    this.#fail(new Error(CLOSED_MESSAGE));
  }
}
