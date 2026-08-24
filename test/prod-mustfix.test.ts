import { afterEach, beforeAll, describe, expect, test } from "bun:test";
import {
  Producer,
  Consumer,
  installShutdown,
  useNative,
  useProductionNative,
  napiBuilt,
  getDriver,
  RD_KAFKA_OFFSET_BEGINNING,
  type DeliveryReport,
} from "../src/index.ts";
import {
  BROKERS,
  initNative,
  produceN,
  producer as makeProducer,
  consumer as makeConsumer,
  topic,
  waitFor,
  waitTopic,
  dec,
} from "./helpers.ts";

beforeAll(async () => {
  await initNative("ffi"); // callbacks path covered on FFI
});

const open: { close: () => Promise<void> | void }[] = [];
function track<T extends { close: () => Promise<void> | void }>(c: T): T {
  open.push(c);
  return c;
}
afterEach(async () => {
  while (open.length) {
    try { await open.pop()!.close(); } catch {}
  }
});

describe("prod must-fix", () => {
  test("delivery reports via onDelivery + poll/flush", async () => {
    const t = topic("dr");
    const reports: DeliveryReport[] = [];
    const p = track(
      new Producer(
        { "bootstrap.servers": BROKERS, acks: "all", "linger.ms": 0 },
        {
          onDelivery: (r) => reports.push(r),
        },
      ),
    );
    p.send({ topic: t, value: "hello-dr", key: "k" });
    await p.flush(15_000);
    for (let i = 0; i < 50 && reports.length === 0; i++) p.poll(20);
    expect(reports.length).toBeGreaterThanOrEqual(1);
    expect(reports[0]!.topic).toBe(t);
    expect(reports[0]!.errorCode).toBe(0);
    expect(reports[0]!.offset >= 0n).toBe(true);
    expect(p.fatalError()).toBeNull();
    await p.close();
    await waitTopic(t);
  });

  test("fatalError API returns null when healthy", async () => {
    const p = track(makeProducer());
    const c = track(makeConsumer());
    expect(p.fatalError()).toBeNull();
    expect(c.fatalError()).toBeNull();
    await p.close();
    await c.close();
  });

  test("message key/value are safe copies by default", async () => {
    const t = topic("copy");
    const p = track(makeProducer());
    p.send({ topic: t, key: "abc", value: "xyz" });
    await p.flush(15_000);
    await p.close();
    await waitTopic(t);

    const c = track(makeConsumer());
    c.assign([{ topic: t, partition: 0, offset: RD_KAFKA_OFFSET_BEGINNING }]);
    const msg = await waitFor(() => c.poll(500));
    const key = msg.key!;
    const value = msg.value!;
    msg.done();
    // after done, copies still readable
    expect(dec(key)).toBe("abc");
    expect(dec(value)).toBe("xyz");
    await c.close();
  });

  test("auto rebalance allows group subscribe consume", async () => {
    const t = topic("rb");
    const p = track(makeProducer());
    await produceN(p, t, 5);
    await p.close();
    await waitTopic(t);

    const events: string[] = [];
    const c = track(
      new Consumer(
        {
          "bootstrap.servers": BROKERS,
          "group.id": `rb-${crypto.randomUUID()}`,
          "enable.auto.commit": true,
          "auto.offset.reset": "earliest",
        },
        {
          autoRebalance: true,
          onRebalance: (e) => events.push(e.kind),
        },
      ),
    );
    c.subscribe(t);
    let n = 0;
    for await (const m of c.messages({ timeoutMs: 200 })) {
      n++;
      if (n >= 5) break;
    }
    expect(n).toBe(5);
    // assign event may or may not have been observed depending on timing; protocol should be set after join
    expect(["NONE", "EAGER", "COOPERATIVE"]).toContain(c.rebalanceProtocol());
    await c.close();
  });

  test("installShutdown registers and uninstalls", () => {
    const p = track(makeProducer());
    const stop = installShutdown(p, { exit: false, sigint: false });
    expect(typeof stop).toBe("function");
    stop();
  });

  test("useProductionNative picks napi when built", async () => {
    const d = await useProductionNative();
    if (napiBuilt()) expect(d.kind).toBe("napi");
    else expect(d.kind).toBe("ffi");
    // restore ffi for other tests in file that need DR callbacks
    await useNative("ffi");
    expect(getDriver().kind).toBe("ffi");
  });
});
