---
title: Security
description: Connect with TLS, mTLS, SASL, SCRAM, or OAuth bearer tokens.
---

## TLS

```ts
const kafka = new Kafka({
  brokers: ["kafka-1:9093"],
  tls: true, // system CAs
});
```

Custom CA / mutual TLS — pass any Bun TLS options:

```ts
const kafka = new Kafka({
  brokers: ["kafka-1:9093"],
  tls: {
    ca: Bun.file("ca.pem"), // or a string/array of certs
    cert: Bun.file("client.pem"),
    key: Bun.file("client.key"),
    serverName: "kafka.internal",
  },
});
```

`rejectUnauthorized: false` disables validation — never in production.

## SASL mechanisms

```ts
// PLAIN (send over TLS only)
sasl: { mechanism: "plain", username: "user", password: "secret" }

// SCRAM-SHA-256 / SCRAM-SHA-512 — salted challenge/response; safe without TLS exposure of the password exchange
sasl: { mechanism: "scram-sha-256", username: "user", password: "secret" }
sasl: { mechanism: "scram-sha-512", username: "user", password: "secret" }

// OAUTHBEARER — static token
sasl: { mechanism: "oauthbearer", token: currentToken }

// OAUTHBEARER — token provider (called at connect and at re-authentication time)
sasl: { mechanism: "oauthbearer", token: async () => await fetchToken() }
```

All mechanisms use the SASL handshake + authenticate APIs. SCRAM is implemented with Web Crypto
(PBKDF2 + HMAC) — no native dependencies.

## Timed reauthentication (KIP-368)

Brokers advertise a session lifetime for SASL sessions. The client schedules re-authentication
at 80% of that lifetime and fetches a fresh token from your provider at that moment. A failed
re-authentication closes the connection with a fatal typed error rather than letting requests
silently continue on an expired session.

## Kerberos/GSSAPI is not supported

GSSAPI requires OS-level credential caches and native libraries, which conflicts with the
zero-dependency goal. Alternatives: SCRAM-SHA-512, mTLS, OAUTHBEARER, or delegation tokens.

## What to run in production

The common hardening baseline:

```ts
const kafka = new Kafka({
  brokers: ["b1:9093", "b2:9093", "b3:9093"],
  clientId: "orders-api",
  tls: { ca: caCert },
  sasl: { mechanism: "scram-sha-512", username, password },
  requestTimeoutMs: 30_000,
  retry: { maxRetries: 5 },
});
```

Read credentials from your secret manager, not the source tree.
