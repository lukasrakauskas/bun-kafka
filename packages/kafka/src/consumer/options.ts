import {
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_REBALANCE_TIMEOUT_MS,
  DEFAULT_SESSION_TIMEOUT_MS,
} from "../bun/shared.ts";
import type { ConsumerSettings } from "./types.ts";

function validateProtocolOptions(
  protocol: "classic" | "consumer",
  options: ConsumerSettings,
): void {
  if (protocol === "consumer") {
    const unsupported = [
      options.partitionAssigner,
      options.sessionTimeoutMs,
      options.heartbeatIntervalMs,
    ];
    if (unsupported.some((value) => value !== undefined)) {
      throw new TypeError(
        "partitionAssigner, sessionTimeoutMs, and heartbeatIntervalMs require groupProtocol=classic",
      );
    }
    return;
  }
  if (options.groupRemoteAssignor !== undefined) {
    throw new TypeError("groupRemoteAssignor requires groupProtocol=consumer");
  }
}

function validateTimeouts(protocol: "classic" | "consumer", options: ConsumerSettings): void {
  const session = options.sessionTimeoutMs ?? DEFAULT_SESSION_TIMEOUT_MS;
  const rebalance = options.rebalanceTimeoutMs ?? DEFAULT_REBALANCE_TIMEOUT_MS;
  const heartbeat = options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
  const timeouts = protocol === "consumer" ? [rebalance] : [session, rebalance, heartbeat];
  const invalid = !timeouts.every((value) => Number.isSafeInteger(value) && value > 0);
  if (invalid || (protocol === "classic" && heartbeat >= session)) {
    throw new RangeError("Invalid consumer group timeout options");
  }
}

export function validateConsumerOptions(options: ConsumerSettings): void {
  const protocol = options.groupProtocol ?? "classic";
  if (protocol !== "classic" && protocol !== "consumer") {
    throw new TypeError("groupProtocol must be classic or consumer");
  }
  validateProtocolOptions(protocol, options);
  validateTimeouts(protocol, options);
}
