import { describe, expect, test } from "bun:test";
import { useNative, getDriver, Producer, Consumer, version } from "../src/index.ts";
import { BROKERS, dec, topic } from "./helpers.ts";
import { existsSync } from "node:fs";
import { join } from "node:path";

describe("native backends", () => {
  test("ffi backend roundtrip", async () => {
    await useNative("ffi");
    expect(getDriver().kind).toBe("ffi");
    expect(version().number).toBeGreaterThan(0);

    const t = topic("ffi");
    const p = new Producer({ "bootstrap.servers": BROKERS, acks: "all" });
    p.send({ topic: t, key: "k", value: "ffi-ok", headers: { b: "ffi" } });
    await p.flush();
    await p.close();

    const c = new Consumer({
      "bootstrap.servers": BROKERS,
      "group.id": `ffi-${crypto.randomUUID()}`,
      "auto.offset.reset": "earliest",
      "enable.auto.commit": false,
    });
    c.subscribe(t);
    let got = "";
    for await (const msg of c) {
      got = dec(msg.value)!;
      expect(dec(msg.headers.b ?? null)).toBe("ffi");
      break;
    }
    expect(got).toBe("ffi-ok");
    await c.close();
  }, 60_000);

  test("napi backend roundtrip when built", async () => {
    const nodePath = join(import.meta.dir, "../native/build/bun_kafka_native.node");
    if (!existsSync(nodePath)) {
      console.log("skip napi: addon not built");
      return;
    }
    await useNative("napi");
    expect(getDriver().kind).toBe("napi");
    expect(version().number).toBeGreaterThan(0);

    const t = topic("napi");
    const p = new Producer({ "bootstrap.servers": BROKERS, acks: "all" });
    p.send({ topic: t, key: "k", value: "napi-ok", headers: { b: "napi" } });
    await p.flush();
    await p.close();

    const c = new Consumer({
      "bootstrap.servers": BROKERS,
      "group.id": `napi-${crypto.randomUUID()}`,
      "auto.offset.reset": "earliest",
      "enable.auto.commit": false,
    });
    c.subscribe(t);
    let got = "";
    for await (const msg of c) {
      got = dec(msg.value)!;
      expect(dec(msg.headers.b ?? null)).toBe("napi");
      break;
    }
    expect(got).toBe("napi-ok");
    await c.close();
    await useNative("ffi");
  }, 60_000);
});
