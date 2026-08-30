import { CompatConsumerRunner } from "./consumer/runner.ts";

export class CompatConsumer extends CompatConsumerRunner {}

export type {
  CompatEachBatchPayload,
  CompatEachMessagePayload,
  RunOptions,
} from "./consumer/types.ts";
