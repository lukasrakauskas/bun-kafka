import { type AclBinding, type AclFilter } from "../../bun/admin.ts";
import { CONFIG_SOURCE_DEFAULT } from "../../bun/shared.ts";
import { isString } from "../../type-guards.ts";
import { wrapError } from "../errors.ts";
import type { CompatOptions } from "../types.ts";
import { CompatAdminGroups } from "./groups.ts";

export class CompatAdminConfigs extends CompatAdminGroups {
  async describeConfigs({
    resources,
  }: {
    resources: Array<{ type: number; name: string; configNames?: string[] }>;
  }): Promise<{
    resources: Array<{
      resourceName: string;
      resourceType: number;
      configEntries: Record<
        string,
        {
          value: string | null;
          isDefault: boolean;
          isSensitive: boolean;
          readOnly: boolean;
          configSource: number;
        }
      >;
    }>;
  }> {
    try {
      const described = await this.underlying().describeConfigs(
        resources.map((resource) => ({
          resourceType: resource.type,
          resourceName: resource.name,
          configNames: resource.configNames,
        })),
      );
      return {
        resources: described.map((resource) => {
          const configEntries: Record<
            string,
            {
              value: string | null;
              isDefault: boolean;
              isSensitive: boolean;
              readOnly: boolean;
              configSource: number;
            }
          > = {};
          for (const config of resource.configs) {
            configEntries[config.name] = {
              value: config.value,
              isDefault: config.source === CONFIG_SOURCE_DEFAULT,
              isSensitive: config.sensitive,
              readOnly: config.readOnly,
              configSource: config.source,
            };
          }
          return {
            resourceName: resource.resourceName,
            resourceType: resource.resourceType,
            configEntries,
          };
        }),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async alterConfigs({
    resources,
  }: {
    validateOnly?: boolean;
    resources: Array<{ type: number; name: string; configEntries: Record<string, string | null> }>;
  }): Promise<void> {
    try {
      await this.underlying().alterConfigs(
        resources.map((resource) => ({
          resourceType: resource.type,
          resourceName: resource.name,
          configs: resource.configEntries,
        })),
      );
    } catch (error) {
      throw wrapError(error);
    }
  }

  async createAcls({ acl }: { acl: Array<CompatOptions> }): Promise<boolean[]> {
    try {
      const bindings: AclBinding[] = acl.map((entry) => ({
        resourceType: Number(entry.resourceType ?? entry.resourceResourceType ?? 2),
        resourceName: String(entry.resourceName ?? entry.resourceResourceName),
        principal: String(entry.principal),
        host: String(entry.host),
        operation: Number(entry.operation),
        permissionType: Number(entry.permissionType),
      }));
      const results = await this.underlying().createAcls(bindings);
      return results.map((result) => result.error === 0);
    } catch (error) {
      throw wrapError(error);
    }
  }

  async describeAcls(filter: CompatOptions): Promise<{
    resources: Array<{
      resourceType: number;
      resourceName: string;
      principal: string;
      host: string;
      operation: number;
      permissionType: number;
    }>;
  }> {
    try {
      const aclFilter: AclFilter = {
        resourceType: Number(filter.resourceType ?? 1),
        resourceName: isString(filter.resourceName) ? filter.resourceName : undefined,
        principal: isString(filter.principal) ? filter.principal : undefined,
        host: isString(filter.host) ? filter.host : undefined,
        operation: Number(filter.operation ?? 1),
        permissionType: Number(filter.permissionType ?? 1),
      };
      const described = await this.underlying().describeAcls(aclFilter);
      return {
        resources: described.acls.map((acl) => ({
          resourceType: acl.resourceType,
          resourceName: acl.resourceName,
          principal: acl.principal,
          host: acl.host,
          operation: acl.operation,
          permissionType: acl.permissionType,
        })),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }

  async deleteAcls(filters: { filters: Array<CompatOptions> }): Promise<{
    entries: Array<{
      errorCode: number;
      errorMessage?: string;
      resources: Array<{
        resourceType: number;
        resourceName: string;
        principal: string;
        host: string;
        operation: number;
        permissionType: number;
      }>;
    }>;
  }> {
    try {
      const aclFilters: AclFilter[] = filters.filters.map((filter) => ({
        resourceType: Number(filter.resourceType ?? 1),
        resourceName: isString(filter.resourceName) ? filter.resourceName : undefined,
        principal: isString(filter.principal) ? filter.principal : undefined,
        host: isString(filter.host) ? filter.host : undefined,
        operation: Number(filter.operation ?? 1),
        permissionType: Number(filter.permissionType ?? 1),
      }));
      const result = await this.underlying().deleteAcls(aclFilters);
      return {
        entries: result.map((entry) => ({
          errorCode: entry.error,
          errorMessage: entry.message ?? undefined,
          resources: entry.acls.map((acl) => ({
            resourceType: acl.resourceType,
            resourceName: acl.resourceName,
            principal: acl.principal,
            host: acl.host,
            operation: acl.operation,
            permissionType: acl.permissionType,
          })),
        })),
      };
    } catch (error) {
      throw wrapError(error);
    }
  }
}
