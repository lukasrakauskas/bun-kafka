type Closer = {
  close: (timeoutMs?: number) => Promise<void> | void;
  flush?: (timeoutMs?: number) => Promise<void> | void;
};

export type ShutdownOptions = {
  /** Max time for flush/close work (default 10_000). */
  timeoutMs?: number;
  /** Also bind SIGINT (default true). */
  sigint?: boolean;
  /** Exit process after close (default true). */
  exit?: boolean;
  /** Called with errors from close/flush. */
  onError?: (err: Error) => void;
};

/**
 * Register SIGTERM/SIGINT handlers that flush producers and close clients.
 * Returns an uninstall function.
 */
async function closeClient(
  client: Closer,
  timeoutMs: number,
  onError?: (error: Error) => void,
): Promise<void> {
  try {
    if (client.flush) {
      await client.flush(timeoutMs);
    }
  } catch (error) {
    onError?.(error instanceof Error ? error : new Error(String(error)));
  }
  try {
    await client.close(timeoutMs);
  } catch (error) {
    onError?.(error instanceof Error ? error : new Error(String(error)));
  }
}

export function installShutdown(
  clients: Closer | Closer[],
  opts: ShutdownOptions = {},
): () => void {
  const list = Array.isArray(clients) ? clients : [clients];
  const timeoutMs = opts.timeoutMs ?? 10_000;
  const exit = opts.exit !== false;
  let ran = false;

  const run = async () => {
    if (ran) {
      return;
    }
    ran = true;
    for (const client of list) {
      await closeClient(client, timeoutMs, opts.onError);
    }
    if (exit) {
      process.exit(0);
    }
  };

  const handler = () => {
    void run();
  };
  process.on("SIGTERM", handler);
  if (opts.sigint !== false) {
    process.on("SIGINT", handler);
  }

  return () => {
    process.off("SIGTERM", handler);
    if (opts.sigint !== false) {
      process.off("SIGINT", handler);
    }
  };
}
