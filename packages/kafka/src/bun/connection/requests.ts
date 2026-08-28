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
};

export class RequestTracker {
  #correlation = 0;
  #pending = new Map<number, PendingRequest>();

  constructor(
    readonly address: string,
    readonly clientId: string,
    readonly metrics: ConnectionMetrics,
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
      const timer = setTimeout(() => {
        this.#pending.delete(correlation);
        reject(
          new KafkaError(-1, `Kafka request ${apiKey} timed out after ${timeoutMs}ms`, {
            retriable: true,
          }),
        );
      }, timeoutMs);
      this.#pending.set(correlation, { resolve, reject, timer, flexible });
      if (socket.write(frame) < 0) {
        clearTimeout(timer);
        this.#pending.delete(correlation);
        reject(this.#writeError());
      }
    });
  }

  sendOnly(socket: Bun.Socket, apiKey: number, apiVersion: number, body: RequestBody): void {
    const { frame } = this.#createFrame(apiKey, apiVersion, body, false);
    if (socket.write(frame) < 0) {
      throw this.#writeError();
    }
  }

  receive(frame: Uint8Array): void {
    this.metrics.recordResponse(frame.byteLength);
    const correlation = new DataView(frame.buffer, frame.byteOffset, SIZE_I32).getInt32(0);
    const pending = this.#pending.get(correlation);
    if (!pending) {
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
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.#pending.clear();
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
