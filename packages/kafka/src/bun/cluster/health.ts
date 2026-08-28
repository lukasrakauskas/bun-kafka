import type { ResponseBody } from "../../protocol/index.ts";
import type { HealthReport } from "../shared.ts";

type HealthCheckOptions = {
  brokers: Map<number, string>;
  bootstrap: string[];
  request(address: string, timeoutMs: number): Promise<ResponseBody>;
  log(message: string): void;
};

export async function checkClusterHealth(
  options: HealthCheckOptions,
  timeoutMs: number,
): Promise<HealthReport> {
  const targets = new Map<string, number | undefined>();
  for (const [id, address] of options.brokers) {
    targets.set(address, id);
  }
  for (const address of options.bootstrap) {
    if (!targets.has(address)) {
      targets.set(address, undefined);
    }
  }
  const checks = await Promise.all(
    [...targets].map(async ([address, brokerId]) => {
      const startedAt = performance.now();
      try {
        await options.request(address, timeoutMs);
        return {
          address,
          brokerId,
          ok: true as const,
          latencyMs: Math.round(performance.now() - startedAt),
        };
      } catch (error) {
        options.log(`health check failed for ${address}: ${String(error)}`);
        return {
          address,
          brokerId,
          ok: false as const,
          latencyMs: Math.round(performance.now() - startedAt),
          error,
        };
      }
    }),
  );
  return { brokers: checks };
}
