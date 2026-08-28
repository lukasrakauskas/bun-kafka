import { AdminSecurity } from "./admin/security.ts";

export class BunAdmin extends AdminSecurity {}

export type {
  AclBinding,
  AclFilter,
  AclListing,
  AclOperation,
  AclPermissionType,
  AclResourceType,
  ConfigResource,
  CreatePartitionsInput,
  CreateTopicInput,
  DeleteRecordsResult,
  GroupDescription,
  GroupMemberDescription,
  TopicResult,
} from "./admin/types.ts";
