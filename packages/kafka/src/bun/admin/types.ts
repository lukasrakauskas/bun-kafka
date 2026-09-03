export interface TopicPartitionInput {
  topic: string;
  partition: number;
}

export interface PartitionReassignmentInput extends TopicPartitionInput {
  /** New replica IDs, or null to cancel an active reassignment. */
  replicas: readonly number[] | null;
}

export interface PartitionResult extends TopicPartitionInput {
  error: number;
  message: string | null;
}

export interface OngoingPartitionReassignment extends TopicPartitionInput {
  replicas: number[];
  addingReplicas: number[];
  removingReplicas: number[];
}

export interface CreateTopicInput {
  name: string;
  numPartitions: number;
  replicationFactor?: number;
  assignments?: number[][];
  configs?: Record<string, string | null>;
}

export interface TopicResult {
  name: string;
  error: number;
  message: string | null;
}

export interface CreatePartitionsInput {
  name: string;
  count: number;
  assignments?: number[][];
}

export interface ConfigResource {
  resourceType: number;
  resourceName: string;
  error: number;
  message: string | null;
  configs: Array<{
    name: string;
    value: string | null;
    source: number;
    sensitive: boolean;
    readOnly: boolean;
  }>;
}

export interface GroupMemberDescription {
  memberId: string;
  clientId: string;
  clientHost: string;
  memberMetadata: Uint8Array | null;
  memberAssignment: Uint8Array | null;
}

export interface GroupDescription {
  error: number;
  message: string | null;
  groupId: string;
  state: string;
  protocolType: string;
  protocol: string | null;
  members: GroupMemberDescription[];
}

export type DeleteRecordsResult = {
  name: string;
  index: number;
  lowWatermark: bigint;
  error: number;
};

export type AclResourceType = number;
export type AclOperation = number;
export type AclPermissionType = number;

export interface AclBinding {
  resourceType: AclResourceType;
  resourceName: string;
  principal: string;
  host: string;
  operation: AclOperation;
  permissionType: AclPermissionType;
}

export type AclFilter = {
  resourceType: AclResourceType;
  resourceName?: string;
  principal?: string;
  host?: string;
  operation: AclOperation;
  permissionType: AclPermissionType;
};

export interface AclListing {
  error?: number;
  message?: string | null;
  resourceType: number;
  resourceName: string;
  principal: string;
  host: string;
  operation: number;
  permissionType: number;
}
