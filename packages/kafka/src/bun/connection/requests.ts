import { KafkaError } from "../../errors.ts";
import {
  type RequestBody,
  type ResponseBody,
  readResponsePayload,
  writeRequestFrame,
} from "../../protocol/index.ts";
import { HEX_DUMP_BYTES, INT32_MAX, RADIX_HEX, SIZE_I32 } from "../shared.ts";
import { ConnectionMetrics } from "./metrics.ts";

type PendingRequest = {
  resolve: (body: ResponseBody) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
  flexible: boolean;
  sent?: () => void;
};

export class RequestTracker {
  #correlation = 0;
  #pending = new Map<number, PendingRequest>();
  #writes = new Map<number, Uint8Array>();

  constructor(
    readonly address: string,
    readonly clientId: string,
    readonly metrics: ConnectionMetrics,
    readonly onFailure: (error: Error, socket: Bun.Socket) => void = () => {},
  ) {}

  request(
    socket: Bun.Socket,
    apiKey: number,
    apiVersion: number,
    body: RequestBody,
    timeoutMs: number,
    flexible = false,
  ): Promise<ResponseBody> {
    const { correlation, frame } = this.#createFrame(apiKey, apiVersion, body, flexible);
    return new Promise<ResponseBody>((resolve, reject) => {
      const timer = this.#timer(socket, correlation, apiKey, timeoutMs);
      this.#pending.set(correlation, { resolve, reject, timer, flexible });
      this.#enqueue(socket, correlation, frame);
    });
  }

  sendOnly(
    socket: Bun.Socket,
    apiKey: number,
    apiVersion: number,
    body: RequestBody,
    timeoutMs: number,
  ): Promise<void> {
    const { correlation, frame } = this.#createFrame(apiKey, apiVersion, body, false);
    return new Promise<void>((resolve, reject) => {
      const timer = this.#timer(socket, correlation, apiKey, timeoutMs);
      this.#pending.set(correlation, {
        resolve: () => resolve(),
        reject,
        timer,
        flexible: false,
        sent: resolve,
      });
      this.#enqueue(socket, correlation, frame);
    });
  }

  drain(socket: Bun.Socket): void {
    try {
      for (const [correlation, frame] of this.#writes) {
        const written = socket.write(frame);
        if (written < 0) {
          throw this.#writeError();
        }
        if (written < frame.byteLength) {
          this.#writes.set(correlation, frame.subarray(written));
          return;
        }
        this.#writes.delete(correlation);
        const pending = this.#pending.get(correlation);
        if (pending?.sent) {
          clearTimeout(pending.timer);
          this.#pending.delete(correlation);
          pending.sent();
        }
      }
    } catch (error) {
      this.#abort(error instanceof Error ? error : new Error(String(error)), socket);
    }
  }

  receive(frame: Uint8Array): void {
    this.metrics.recordResponse(frame.byteLength);
    const correlation = new DataView(frame.buffer, frame.byteOffset, SIZE_I32).getInt32(0);
    const pending = this.#pending.get(correlation);
    if (!pending || pending.sent) {
      return;
    }
    clearTimeout(pending.timer);
    this.#pending.delete(correlation);
    try {
      const { body } = readResponsePayload(frame, pending.flexible);
      pending.resolve(body);
    } catch (error) {
      pending.reject(error instanceof Error ? error : new Error(String(error)));
    }
  }

  fail(error: Error): void {
    this.#writes.clear();
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.#pending.clear();
  }

  #enqueue(socket: Bun.Socket, correlation: number, frame: Uint8Array): void {
    const blocked = this.#writes.size > 0;
    this.#writes.set(correlation, frame);
    if (!blocked) {
      this.drain(socket);
    }
  }

  #timer(socket: Bun.Socket, correlation: number, apiKey: number, timeoutMs: number) {
    return setTimeout(() => {
      const error = new KafkaError(-1, `Kafka request ${apiKey} timed out after ${timeoutMs}ms`, {
        retriable: true,
      });
      // Never drop a queued remainder and reuse the stream: that corrupts Kafka framing.
      if (this.#writes.has(correlation)) {
        this.#abort(error, socket);
      } else {
        this.#pending.get(correlation)?.reject(error);
        this.#pending.delete(correlation);
      }
    }, timeoutMs);
  }

  #abort(error: Error, socket: Bun.Socket): void {
    this.fail(error);
    this.onFailure(error, socket);
    socket.end();
  }

  #createFrame(apiKey: number, apiVersion: number, body: RequestBody, flexible: boolean) {
    this.#correlation = (this.#correlation + 1) & INT32_MAX;
    const correlation = this.#correlation;
    const frame = writeRequestFrame({
      apiKey,
      apiVersion,
      correlationId: correlation,
      clientId: this.clientId,
      body,
      flexible,
    });
    this.metrics.recordRequest(frame.byteLength);
    if (process.env.DEBUG_TXKEYS) {
      console.error("TX", apiKey, `v${apiVersion}`);
    }
    if (process.env.DEBUG_FRAME) {
      console.error(
        "FRAME",
        apiKey,
        `v${apiVersion}`,
        Array.from(frame)
          .slice(0, HEX_DUMP_BYTES)
          .map((byte) => byte.toString(RADIX_HEX).padStart(2, "0"))
          .join(" "),
      );
    }
    return { correlation, frame };
  }

  #writeError(): KafkaError {
    return new KafkaError(-1, `Could not write to Kafka broker ${this.address}`, {
      retriable: true,
    });
  }
}
