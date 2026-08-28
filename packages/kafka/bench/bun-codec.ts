import { decodeRecordSet, encodeRecordBatch } from "../src/bun/protocol.ts";
import { requiredValue } from "../src/type-guards.ts";

const width = Number(process.argv[2] ?? 100);
const total = Number(process.argv[3] ?? 100_000);
const payload = new Uint8Array(100).fill(0x78);
const records = Array.from({ length: width }, (_, i) => ({ key: String(i % 64), value: payload }));
let checksum = 0n;

for (let done = 0; done < total; done += width) {
  const size = Math.min(width, total - done);
  const batch = encodeRecordBatch(
    size === width ? records : records.slice(0, size),
    1_700_000_000_000,
  );
  const decoded = decodeRecordSet(batch, "bench", 0, 1);
  checksum += BigInt(decoded.length) + requiredValue(decoded.at(-1)).offset;
}

console.log(JSON.stringify({ recordsPerBatch: width, total, checksum: String(checksum) }));
