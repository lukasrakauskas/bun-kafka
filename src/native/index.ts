import type { NativeKind } from "../types.ts";
import type { NativeDriver } from "./types.ts";
import { ffiDriver } from "./ffi/driver.ts";

export type { NativeDriver, NativeProducer, NativeConsumer, NativeAdmin } from "./types.ts";

let current: NativeDriver = ffiDriver;

export function getDriver(): NativeDriver {
  return current;
}

export function setDriver(driver: NativeDriver): void {
  current = driver;
}

export async function useNative(
  kind: NativeKind = (process.env.BUN_KAFKA_NATIVE as NativeKind) || "ffi",
): Promise<NativeDriver> {
  if (kind === "ffi") {
    current = ffiDriver;
    return current;
  }
  const { napiDriver } = await import("./napi/driver.ts");
  current = napiDriver;
  current.version();
  return current;
}

export { ffiDriver };
