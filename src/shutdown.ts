type Closer = { close: (timeoutMs?: number) => Promise<void> | void; flush?: (timeoutMs?: number) => Promise<void> | void };

export type ShutdownOptions = {
  /** Max time for flush/close work (default 10_000). */
  timeoutMs?: number;
  /** Also bind SIGINT (default true). */
  sigint?: boolean;
  /** Exit process after close (default true). */
  exit?: boolean;
  /** Called with errors from close/flush. */
  onError?: (err: unknown) => void;
};

/**
 * Register SIGTERM/SIGINT handlers that flush producers and close clients.
 * Returns an uninstall function.
 */
export function installShutdown(
  clients: Closer | Closer[],
  opts: ShutdownOptions = {},
): () => void {
  const list = Array.isArray(clients) ? clients : [clients];
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const exit = opts.exit !== false;
  let ran = false;

  const run = async () => {
    if (ran) return;
    ran = true;
    for (const c of list) {
      try {
        if (c.flush) await c.flush(timeoutMs);
      } catch (e) {
        opts.onError?.(e);
      }
      try {
        await c.close(timeoutMs);
      } catch (e) {
        opts.onError?.(e);
      }
    }
    if (exit) process.exit(0);
  };

  const handler = () => {
    void run();
  };
  process.on("SIGTERM", handler);
  if (opts.sigint !== false) process.on("SIGINT", handler);

  return () => {
    process.off("SIGTERM", handler);
    if (opts.sigint !== false) process.off("SIGINT", handler);
  };
}
