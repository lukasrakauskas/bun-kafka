import { BunAdmin } from "../../bun/admin.ts";
import { ADMIN_EVENTS } from "../constants.ts";
import type { ClusterGetter } from "../config.ts";
import { Emitter, Logger } from "../logger.ts";
import type { LogFields } from "../types.ts";

export class CompatAdminBase {
  events = ADMIN_EVENTS;
  protected admin?: BunAdmin;
  protected emitter = new Emitter();

  constructor(
    protected getter: () => ClusterGetter,
    protected log: Logger,
  ) {}

  on(event: string, listener: (event: LogFields) => void): () => void {
    return this.emitter.on(event, listener);
  }

  logger(): Logger {
    return this.log;
  }

  protected underlying(): BunAdmin {
    this.admin ??= new BunAdmin(this.getter().acquire(), this.getter().release);
    return this.admin;
  }

  async connect(): Promise<void> {
    await this.getter().ready();
    this.underlying();
    this.emitter.emit(ADMIN_EVENTS.CONNECT);
  }

  async disconnect(): Promise<void> {
    await this.admin?.close().catch(() => {});
    this.admin = undefined;
    this.emitter.emit(ADMIN_EVENTS.DISCONNECT);
  }
}
