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
  #timer?: ReturnType<typeof setInterval>;
  constructor(
    private readonly cluster: Cluster,
    private readonly options: ConsumerSettings,
    private readonly state: ConsumerState,
    private readonly restart: () => Promise<void>,
  ) {}

  start(coordinator: number): void {
    this.stop();
    this.#timer = setInterval(
      () => void this.#beat(coordinator),
      this.options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS,
    );
  }
  stop(): void {
    if (this.#timer) {
      clearInterval(this.#timer);
    }
    this.#timer = undefined;
  }
  async #beat(coordinator: number): Promise<void> {
    if (!this.state.groupId || this.state.generationId < 0 || this.state.rejoining) {
      return;
    }
    try {
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
      if (error === undefined) {
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
    await this.restart();
  }
}
