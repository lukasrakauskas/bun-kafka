import type { ConsumerGroupEvent, ConsumerOptions } from "../../consumer/index.ts";
import { hasStringName, isBoolean, isNumber, isString } from "../../type-guards.ts";
import type { CompatOptions, CompatValue } from "../types.ts";

export function numberOption(value: CompatValue): number | undefined {
  return isNumber(value) ? value : undefined;
}

export function createCompatConsumerOptions(
  options: CompatOptions,
  onGroupEvent: (event: ConsumerGroupEvent) => void,
): ConsumerOptions {
  const assignors = Array.isArray(options.partitionAssignors) ? options.partitionAssignors : [];
  const cooperative = assignors.some(
    (assignor) => hasStringName(assignor) && assignor.name === "CooperativeStickyAssignor",
  );
  return {
    groupId: isString(options.groupId) ? options.groupId : undefined,
    onGroupEvent,
    sessionTimeoutMs: numberOption(options.sessionTimeout),
    rebalanceTimeoutMs: numberOption(options.rebalanceTimeout),
    heartbeatIntervalMs: numberOption(options.heartbeatInterval),
    fromBeginning: isBoolean(options.fromBeginning) ? options.fromBeginning : undefined,
    isolationLevel:
      options.isolationLevel === "read_uncommitted" || options.isolationLevel === "read_committed"
        ? options.isolationLevel
        : undefined,
    groupInstanceId: isString(options.groupInstanceId) ? options.groupInstanceId : undefined,
    partitionAssigner: cooperative ? "cooperative-sticky" : undefined,
  };
}

export function highestOffset(offsets: Set<string>): bigint {
  return [...offsets]
    .map(BigInt)
    .reduce((highest, offset) => (offset > highest ? offset : highest));
}
