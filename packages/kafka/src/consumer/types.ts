export interface ConsumerOptions<K = Uint8Array | null, V = Uint8Array | null> {
  fromBeginning?: boolean;
  fetchMaxBytes?: number;
  groupId?: string;
  /** Called before and after each consumer-group join. Listener failures are ignored. */
  onGroupEvent?: (event: ConsumerGroupEvent) => void;
  /** Static group membership identity (KIP-345); requires a broker that supports JoinGroup v3+. */
  groupInstanceId?: string;
  /**
   * Consumer group partition assignor protocol. "range" (default) is eager:
   * every rebalance revokes all partitions. "cooperative-sticky" (KIP-429)
   * retains ownership across rebalances so only moved partitions change hands.
   */
  partitionAssigner?: "range" | "cooperative-sticky";
  /** Transaction visibility: read_committed filters aborted transaction records (default read_uncommitted). */
  isolationLevel?: "read_uncommitted" | "read_committed";
  sessionTimeoutMs?: number;
  rebalanceTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  autoCommit?: boolean;
  /** Replaces message keys before they are returned. */
  keyDeserializer?: (data: Uint8Array | null, context: DeserializerContext) => K;
  /** Replaces message values before they are returned. */
  valueDeserializer?: (data: Uint8Array | null, context: DeserializerContext) => V;
}

export type ConsumerSettings = Omit<ConsumerOptions, "keyDeserializer" | "valueDeserializer">;

export type DeserializerContext = {
  topic: string;
  partition: number;
  offset: bigint;
  timestamp: bigint;
};

export interface ConsumerSubscribe {
  topics?: string | RegExp | Array<string | RegExp>;
  topic?: string | RegExp;
  fromBeginning?: boolean;
  groupId?: string;
}

export interface ConsumerAssignment {
  topic: string;
  partition: number;
  offset?: bigint | "earliest" | "latest";
}

export interface FetchOptions {
  maxWaitMs?: number;
  minBytes?: number;
  maxBytes?: number;
  maxPartitionBytes?: number;
  maxMessages?: number;
  /** Copy payloads instead of returning stable views into the response buffer. */
  copy?: boolean;
}

export type Assigned = { topic: string; partition: number; leader: number };
export type GroupAssignment = { topic: string; partitions: number[] };
export type GroupMember = {
  memberId: string;
  topics: string[];
  owned: Array<{ topic: string; partition: number }>;
};

export type ConsumerGroupEvent =
  | { type: "rebalancing"; groupId: string; memberId: string }
  | {
      type: "group_join";
      groupId: string;
      memberId: string;
      generationId: number;
      memberAssignment: Record<string, number[]>;
      duration: number;
    };

export type ConsumerState = {
  groupId?: string;
  memberId: string;
  generationId: number;
  coordinator?: number;
  groupTopics: string[];
  rejoining?: Promise<void>;
};
