import { logLevel } from "./constants.ts";

export interface LoggerEntry {
  namespace: string;
  level: number;
  label: string;
  log: Record<string, unknown>;
}

const LEVEL_LABELS: Record<number, string> = { 0: "NOTHING", 1: "ERROR", 2: "WARN", 3: "INFO", 4: "DEBUG" };

export class Logger {
  #level: number;
  #namespace: string;
  #creator?: (entry: LoggerEntry) => void;
  constructor(level: number, namespace: string, creator?: (entry: LoggerEntry) => void) {
    this.#level = level;
    this.#namespace = namespace;
    this.#creator = creator;
  }
  #write(level: number, message: string, extra: Record<string, unknown>): void {
    if (level > this.#level) return;
    const entry: LoggerEntry = {
      namespace: this.#namespace,
      level,
      label: LEVEL_LABELS[level] ?? String(level),
      log: { message, ...extra },
    };
    if (this.#creator) {
      this.#creator(entry);
      return;
    }
    const target = level <= 1 ? console.error : level === 2 ? console.warn : console.log;
    target(`{"level":"${entry.label}","timestamp":${Date.now()},"logger":"${entry.namespace}","message":${JSON.stringify(message)}}`);
  }
  debug(message: string, extra: Record<string, unknown> = {}): void {
    this.#write(logLevel.DEBUG, message, extra);
  }
  info(message: string, extra: Record<string, unknown> = {}): void {
    this.#write(logLevel.INFO, message, extra);
  }
  warn(message: string, extra: Record<string, unknown> = {}): void {
    this.#write(logLevel.WARN, message, extra);
  }
  error(message: string, extra: Record<string, unknown> = {}): void {
    this.#write(logLevel.ERROR, message, extra);
  }
  namespace(namespace: string): Logger {
    return new Logger(this.#level, `${this.#namespace}:${namespace}`, this.#creator);
  }
  setLogLevel(level: number): void {
    this.#level = level;
  }
  level(): number {
    return this.#level;
  }
}

/** Minimal listener registry shared by all compat clients. */
export class Emitter {
  #listeners = new Map<string, Set<(event: Record<string, unknown>) => void>>();
  on(type: string, listener: (event: Record<string, unknown>) => void): () => void {
    const set = this.#listeners.get(type) ?? new Set();
    set.add(listener);
    this.#listeners.set(type, set);
    return () => set.delete(listener);
  }
  removeListener(type: string, listener: (event: Record<string, unknown>) => void): void {
    this.#listeners.get(type)?.delete(listener);
  }
  emit(type: string, payload: Record<string, unknown> = {}): void {
    const event = { id: crypto.randomUUID(), type, timestamp: Date.now(), ...payload };
    for (const listener of [...(this.#listeners.get(type) ?? [])]) {
      try {
        listener(event);
      } catch {
        // Listener failures must not break the emitting client.
      }
    }
  }
}
