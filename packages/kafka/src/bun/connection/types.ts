export type BunKafkaTls = boolean | Bun.TLSOptions;

export type BunKafkaSasl =
  | { mechanism: "plain"; username: string; password: string }
  | { mechanism: "scram-sha-256" | "scram-sha-512"; username: string; password: string }
  | { mechanism: "oauthbearer"; token: string | (() => string | Promise<string>) };

export type ConnectionOptions = {
  clientId: string;
  requestTimeoutMs: number;
  connectTimeoutMs: number;
  tls?: BunKafkaTls;
  sasl?: BunKafkaSasl;
  maxResponseBytes: number;
};
