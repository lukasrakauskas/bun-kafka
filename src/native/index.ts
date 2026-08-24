import { existsSync } from "node:fs";
import { join } from "node:path";
import type { NativeKind } from "../types.ts";
import type { NativeDriver } from "./types.ts";
import { ffiDriver } from "./ffi/driver.ts";

export type { NativeDriver, NativeProducer, NativeConsumer, NativeAdmin, ClientEventHandlers } from "./types.ts";

function napiBuilt(): boolean {
  const p = join(import.meta.dir, "../../native/build/bun_kafka_native.node");
  return existsSync(p) || existsSync(join(process.cwd(), "native/build/bun_kafka_native.node"));
}

function defaultKind(): NativeKind {
  const env = process.env.BUN_KAFKA_NATIVE as NativeKind | undefined;
  if (env === "ffi" || env === "napi") return env;
  // Production-oriented default: NAPI when addon is present, else FFI.
  return napiBuilt() ? "napi" : "ffi";
}

let current: NativeDriver = ffiDriver;
let booted = false;

async function ensureBoot(): Promise<void> {
  if (booted) return;
  booted = true;
  await useNative(defaultKind());
}

export function getDriver(): NativeDriver {
  // sync path: if napi preferred and not loaded yet, stay on current until useNative
  return current;
}

export function setDriver(driver: NativeDriver): void {
  current = driver;
  booted = true;
}

export async function useNative(
  kind: NativeKind = defaultKind(),
): Promise<NativeDriver> {
  if (kind === "ffi") {
    current = ffiDriver;
    booted = true;
    return current;
  }
  const { napiDriver } = await import("./napi/driver.ts");
  current = napiDriver;
  current.version();
  booted = true;
  return current;
}

/** Prefer NAPI when built — call once at process start in production. */
export async function useProductionNative(): Promise<NativeDriver> {
  if (napiBuilt()) return useNative("napi");
  return useNative("ffi");
}

export { ffiDriver, ensureBoot, defaultKind, napiBuilt };
