export interface CompatCreateTopicsInput {
  validateOnly?: boolean;
  waitForLeaders?: boolean;
  timeout?: number;
  topics: Array<{
    topic: string;
    numPartitions?: number;
    replicationFactor?: number;
    replicaAssignment?: number[][];
    configEntries?: Array<{ name: string; value: string | null }>;
  }>;
}
