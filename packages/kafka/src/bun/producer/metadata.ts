import { KafkaErrorCode } from "../../errors.ts";
import type { TopicMetadata } from "../shared.ts";

export function topicMetadataReady(metadata: TopicMetadata): boolean {
  return !metadata.err && metadata.partitions.length > 0;
}

export function topicMetadataRetryable(metadata: TopicMetadata): boolean {
  return (
    metadata.err === KafkaErrorCode.UNKNOWN_TOPIC_OR_PARTITION ||
    metadata.err === KafkaErrorCode.LEADER_NOT_AVAILABLE
  );
}
