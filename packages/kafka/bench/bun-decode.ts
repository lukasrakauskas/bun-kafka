import { encodeRecordBatch, RecordSetDecoder } from "../src/protocol/index.ts";
import { requiredValue } from "../src/type-guards.ts";

const codec = requiredValue(
  (["gzip", "snappy", "lz4", "zstd"] as const).find(
    (value) => value === (process.argv[2] ?? "lz4"),
  ),
  "Expected gzip, snappy, lz4, or zstd",
);
// Period 1: repeated byte; 8: repeated pattern; 0: deterministic incompressible bytes.
const period = Number(process.argv[3] ?? 1);
if (![0, 1, 8].includes(period)) throw new RangeError("Expected period 0, 1, or 8");
const width = 100;
const total = 100_000;
let seed = 42;
const records = Array.from({ length: width }, (_, i) => ({
  key: String(i),
  value: Uint8Array.from({ length: 1024 }, (_, j) => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return period ? (i + (j % period)) & 255 : seed >>> 24;
  }),
}));
const batch = encodeRecordBatch(records, 1_700_000_000_000, codec);
for (let i = 0; i < 100; i++) new RecordSetDecoder(batch, "bench", 0, 1).read();
let checksum = 0n;
const start = performance.now();
for (let i = 0; i < total / width; i++) {
  const decoded = new RecordSetDecoder(batch, "bench", 0, 1).read();
  checksum += BigInt(decoded.length) + requiredValue(decoded.at(-1)).offset;
}
console.log(
  JSON.stringify({
    codec,
    period,
    total,
    decodeMs: performance.now() - start,
    checksum: String(checksum),
  }),
);
