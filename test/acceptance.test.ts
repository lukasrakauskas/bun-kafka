import { beforeAll, describe, expect, test } from "bun:test";
import { Consumer, Producer, Admin, version, getDriver } from "../src/index.ts";
import { BROKERS, dec, initNative, topic } from "./helpers.ts";

beforeAll(async () => { await initNative(); });

describe("acceptance", () => {
  test("full produce/consume pipeline", async () => {
    expect(version().string).toMatch(/\d+\.\d+/);
    expect(["ffi", "napi"]).toContain(getDriver().kind);

    const t = topic("accept");
    const p = new Producer({
      "bootstrap.servers": BROKERS,
      acks: "all",
      "linger.ms": 0,
    });

    const payload = { hello: "world", n: 42 };
    p.send({
      topic: t,
      key: "user-1",
      value: JSON.stringify(payload),
      headers: { "content-type": "application/json", "x-trace": "abc" },
      timestamp: Date.now(),
    });
    await p.flush(15_000);
    await p.close();

    const c = new Consumer({
      "bootstrap.servers": BROKERS,
      "group.id": `accept-${crypto.randomUUID()}`,
      "enable.auto.commit": false,
      "auto.offset.reset": "earliest",
    });
    c.subscribe(t);

    let saw: any = null;
    for await (const msg of c) {
      saw = {
        topic: msg.topic,
        key: dec(msg.key),
        value: JSON.parse(dec(msg.value)!),
        ct: dec(msg.headers["content-type"] ?? null),
        trace: dec(msg.headers["x-trace"] ?? null),
        offset: msg.offset,
        timestamp: msg.timestamp,
      };
      break;
    }
    expect(saw).toBeTruthy();
    expect(saw.topic).toBe(t);
    expect(saw.key).toBe("user-1");
    expect(saw.value).toEqual(payload);
    expect(saw.ct).toBe("application/json");
    expect(saw.trace).toBe("abc");
    expect(saw.offset >= 0n).toBe(true);
    expect(saw.timestamp > 0n).toBe(true);

    c.commit([{ topic: t, partition: 0, offset: saw.offset + 1n }]);
    await c.close();

    const a = new Admin({ "bootstrap.servers": BROKERS });
    const meta = a.metadata();
    expect(meta.topics.some((x) => x.name === t)).toBe(true);
    await a.close();
  }, 60_000);

  test("high throughput batch roundtrip", async () => {
    const t = topic("tput");
    const N = 1000;
    const p = new Producer({
      "bootstrap.servers": BROKERS,
      acks: "1",
      "linger.ms": 5,
      "batch.num.messages": 1000,
    });
    const start = performance.now();
    for (let i = 0; i < N; i++) p.send({ topic: t, key: String(i % 16), value: `v-${i}` });
    await p.flush(60_000);
    const producedMs = performance.now() - start;
    await p.close();

    const c = new Consumer({
      "bootstrap.servers": BROKERS,
      "group.id": `tput-${crypto.randomUUID()}`,
      "enable.auto.commit": true,
      "auto.offset.reset": "earliest",
    });
    c.subscribe(t);
    let n = 0;
    const cstart = performance.now();
    for await (const _ of c.messages({ timeoutMs: 100 })) {
      n++;
      if (n >= N) break;
    }
    const consumedMs = performance.now() - cstart;
    await c.close();

    expect(n).toBe(N);
    expect(producedMs).toBeLessThan(60_000);
    expect(consumedMs).toBeLessThan(60_000);
  }, 120_000);
});
