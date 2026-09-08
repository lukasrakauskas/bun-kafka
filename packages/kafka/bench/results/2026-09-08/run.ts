const cwd = new URL("../../../", import.meta.url).pathname;
const out = import.meta.dir;
await Bun.$`mkdir -p ${out}`;
const env = {
  ...process.env,
  KAFKA_BROKERS: process.env.KAFKA_BROKERS ?? "127.0.0.1:19092",
  MSG_SIZE: "100",
};
const lanes: [string, string[]][] = [
  ["codec-1", ["bun", "bench/bun-codec.ts", "1", "20000"]],
  ["codec-100", ["bun", "bench/bun-codec.ts", "100", "20000"]],
  ...["gzip", "snappy", "lz4", "zstd"].flatMap((codec) =>
    [0, 1, 8].map(
      (period) =>
        [`decode-${codec}-${period}`, ["bun", "bench/bun-decode.ts", codec, String(period)]] as [
          string,
          string[],
        ],
    ),
  ),
  ...[0, 5].map(
    (linger) =>
      [
        `dispatch-${linger}`,
        ["bun", "bench/bun-producer-dispatch.ts", "10000", String(linger)],
      ] as [string, string[]],
  ),
  ...[1024, 65536, 1048576].map(
    (size) =>
      [`transport-${size}`, ["bun", "bench/bun-transport.ts", String(size), "10000"]] as [
        string,
        string[],
      ],
  ),
  ["bun-kafka", ["bun", "bench/bun-produce-consume.ts"]],
  ["bun-kafka-alternate", ["bun", "bench/produce_consume.ts"]],
  ["franz-go", ["native/build/bench-go"]],
  ["rdkafka-rust", ["native/build/bench-rust"]],
  ["kafkajs", ["bun", "bench/kafkajs.ts"]],
];
const results =
  process.env.BENCH_RESUME === "1" ? (await Bun.file(`${out}/samples.json`).json()).results : [];
for (const [name, base] of lanes) {
  if (results.some((result: { name: string }) => result.name === name)) continue;
  console.log(`Running ${name}`);
  const samples = [];
  for (let i = -2; i < 10; i++) {
    const command =
      name.includes("kafka") || name === "franz-go" || name === "rdkafka-rust"
        ? [...base, `refresh-${name}-${crypto.randomUUID()}`, "5000"]
        : base;
    const start = performance.now();
    const child = Bun.spawn(command, { cwd, env, stdout: "pipe", stderr: "pipe" });
    const timer = setTimeout(() => child.kill(), 180000);
    const [stdout, stderr, code] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    clearTimeout(timer);
    const wallMs = performance.now() - start;
    if (code !== 0) throw new Error(`${name}: exit ${code}\n${stdout}\n${stderr}`);
    const measurement = stdout
      .trim()
      .split("\n")
      .reverse()
      .map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .find((x) => x && (x.count || x.total || x.records));
    if (!measurement) throw new Error(`Missing measurement: ${name} ${stdout}`);
    if (i >= 0) samples.push({ wallMs, measurement });
  }
  results.push({ name, command: base, samples });
  await Bun.write(
    `${out}/samples.json`,
    JSON.stringify(
      {
        date: "2026-09-08",
        commit: "4b88c455f611d32b86b26b014d733d741a6c210d",
        warmups: 2,
        runs: 10,
        results,
      },
      null,
      2,
    ) + "\n",
  );
}
