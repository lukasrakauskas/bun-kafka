import { KafkaErrorCode } from "../errors.ts";
import type { Cluster } from "../bun/cluster.ts";
import {
  API_HEARTBEAT,
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  GROUP_INSTANCE_API_VERSION,
  kafkaError,
} from "../bun/shared.ts";
import { readConsumerHeartbeatResponse, writeConsumerHeartbeatRequest } from "../protocol/index.ts";
import type { ConsumerSettings, ConsumerState } from "./types.ts";

export class Heartbeat {
  #timer?: ReturnType<typeof setTimeout>;
  #active = false;

  constructor(
    private readonly cluster: Cluster,
    private readonly options: ConsumerSettings,
    private readonly state: ConsumerState,
    private readonly restart: () => Promise<void>,
    private readonly consumerHeartbeat: (coordinator: number) => Promise<void>,
  ) {}

  start(coordinator: number): void {
    this.stop();
    this.#active = true;
    this.#schedule(coordinator);
  }

  stop(): void {
    this.#active = false;
    if (this.#timer) {
      clearTimeout(this.#timer);
    }
    this.#timer = undefined;
  }

  #schedule(coordinator: number): void {
    if (!this.#active) {
      return;
    }
    const interval =
      this.options.groupProtocol === "consumer"
        ? (this.state.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS)
        : (this.options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS);
    this.#timer = setTimeout(() => void this.#beat(coordinator), interval);
    this.#timer.unref?.();
  }

  async #beat(coordinator: number): Promise<void> {
    this.#timer = undefined;
    if (!this.state.groupId || this.state.generationId < 0 || this.state.rejoining) {
      this.#schedule(coordinator);
      return;
    }
    try {
      if (this.options.groupProtocol === "consumer") {
        await this.consumerHeartbeat(coordinator);
        this.#schedule(coordinator);
        return;
      }
      const instanceId = this.options.groupInstanceId;
      const version = instanceId === undefined ? 0 : GROUP_INSTANCE_API_VERSION;
      const response = await this.cluster.request(
        coordinator,
        API_HEARTBEAT,
        version,
        writeConsumerHeartbeatRequest(
          this.state.groupId,
          this.state.generationId,
          this.state.memberId,
          instanceId,
        ),
      );
      const error = readConsumerHeartbeatResponse(response, version === GROUP_INSTANCE_API_VERSION);
      if (!error) {
        this.#schedule(coordinator);
        return;
      }
      if (error === KafkaErrorCode.UNKNOWN_MEMBER_ID) {
        this.state.memberId = "";
      }
      if (
        error !== KafkaErrorCode.ILLEGAL_GENERATION &&
        error !== KafkaErrorCode.UNKNOWN_MEMBER_ID &&
        error !== KafkaErrorCode.REBALANCE_IN_PROGRESS
      ) {
        throw kafkaError(error, `Kafka group ${this.state.groupId} heartbeat`);
      }
    } catch {
      this.state.coordinator = undefined;
    }
    this.#active = false;
    await this.restart();
  }
}
