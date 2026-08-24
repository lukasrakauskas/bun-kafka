import { beforeAll, describe, expect, test } from "bun:test";
import { version, getDriver } from "../src/index.ts";
import { initNative } from "./helpers.ts";

beforeAll(async () => { await initNative(); });

describe("librdkafka binding", () => {
  test("version is available", () => {
    const v = version();
    expect(v.string.length).toBeGreaterThan(0);
    expect(v.number).toBeGreaterThan(0);
    expect(getDriver().err2str(0)).toBeTruthy();
  });
});
