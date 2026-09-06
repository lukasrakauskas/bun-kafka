import { Producer } from "../index.ts";
import { Cluster } from "../src/bun/cluster.ts";
import { encoder, ResponseBody } from "../src/protocol/index.ts";
import { Reader } from "../src/protocol/wire/reader.ts";

// Isolate dispatch, routing, encoding, and acknowledgement decoding; no broker/network cost.
const cluster = new Cluster({ brokers: ["127.0.0.1:1"] });
cluster.topic = async () => ({ name: "bench", err: 0, partitions: [{ id: 0, err: 0, leader: 1 }] });
const response = encoder()
  .array(["bench"], (w) => w.string("bench").array([0], (p) => p.i32(0).i16(0).i64(0).i64(0)))
  .i32(0)
  .result();
let requests = 0;
cluster.request = async () => {
  requests++;
  return new ResponseBody(new Reader(response));
};
const burst = Number(process.argv[2] ?? 10000);
if (!Number.isSafeInteger(burst) || burst < 1)
  throw new RangeError("Expected a positive burst size");
const producer = new Producer(cluster, {
  lingerMs: Number(process.argv[3] ?? 0),
  batchMaxMessages: 1000,
});
const message = { value: new Uint8Array(100) };
async function run(): Promise<void> {
  const results = await Promise.all(
    Array.from({ length: burst }, () => producer.send({ topic: "bench", messages: [message] })),
  );
  await producer.flush();
  if (results.some((result) => result.length !== 1)) throw new Error("Missing acknowledgement");
}
try {
  for (let i = 0; i < 2; i++) await run();
  requests = 0;
  const start = performance.now();
  for (let i = 0; i < 10; i++) await run();
  console.log(
    JSON.stringify({ dispatchMs: performance.now() - start, requests, records: burst * 10 }),
  );
} finally {
  await producer.close();
  cluster.close();
}
