import { KafkaError } from "../../errors.ts";
import { partitionKey } from "../shared.ts";
import type { PartitionRecords, PendingSend, ProduceResult, ProducerMessage } from "./types.ts";

function notifyDeliveryCallback(
  callback: NonNullable<ProducerMessage["onDelivery"]>,
  error: KafkaError | null,
  result: ProduceResult | null,
): void {
  try {
    callback(error, result);
  } catch {
    /* Delivery callbacks must not break flushing. */
  }
}

export function notifyDeliveryFailures(
  pending: PendingSend[],
  error: Error,
  notified: Set<NonNullable<ProducerMessage["onDelivery"]>>,
): void {
  for (const { input } of pending) {
    for (const message of input.messages) {
      if (!message.onDelivery || notified.has(message.onDelivery)) {
        continue;
      }
      notified.add(message.onDelivery);
      notifyDeliveryCallback(
        message.onDelivery,
        error instanceof KafkaError ? error : new KafkaError(-1, String(error)),
        null,
      );
    }
  }
}

export function notifyDeliverySuccess(
  routedPartitions: PartitionRecords[],
  results: ProduceResult[],
  notified: Set<NonNullable<ProducerMessage["onDelivery"]>>,
): void {
  const byPartition = new Map(
    results.map((result) => [partitionKey(result.topic, result.partition), result]),
  );
  for (const routed of routedPartitions) {
    const result = byPartition.get(partitionKey(routed.topic, routed.partition));
    if (!result) {
      continue;
    }
    for (const message of routed.messages) {
      if (!message.onDelivery || notified.has(message.onDelivery)) {
        continue;
      }
      notified.add(message.onDelivery);
      notifyDeliveryCallback(message.onDelivery, null, result);
    }
  }
}
