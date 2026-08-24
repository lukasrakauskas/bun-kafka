export class KafkaError extends Error {
  readonly code: number;
  readonly fatal: boolean;
  readonly retriable: boolean;

  constructor(
    code: number,
    message?: string | null,
    opts?: { fatal?: boolean; retriable?: boolean },
  ) {
    super(message || `kafka error ${code}`);
    this.name = "KafkaError";
    this.code = code;
    this.fatal = opts?.fatal ?? false;
    this.retriable = opts?.retriable ?? false;
  }
}

export function check(code: number, err2str: (c: number) => string, what?: string): void {
  if (code === 0) return;
  const base = err2str(code) || `kafka error ${code}`;
  throw new KafkaError(code, what ? `${what}: ${base}` : base);
}
