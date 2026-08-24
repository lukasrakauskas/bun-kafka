import { describe, expect, test } from "bun:test";
import { Kafka, type KafkaMessage } from "../index.ts";

const enabled = process.env.CHAOS_CLUSTER === "1";
const chaos = enabled ? describe.serial : describe.skip;
const brokers = (process.env.CHAOS_BROKERS ?? "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092").split(",");
const composeFile = process.env.CHAOS_COMPOSE_FILE ?? `${import.meta.dir}/chaos.compose.yml`;
const project = process.env.CHAOS_COMPOSE_PROJECT ?? "bun-kafka-chaos";
const decode = (value: Uint8Array | null) => value ? new TextDecoder().decode(value) : "";

function compose(...args: string[]): string {
  const result = Bun.spawnSync(["docker", "compose", "-f", composeFile, "-p", project, ...args], { stdout: "pipe", stderr: "pipe" });
  if (result.exitCode) throw new Error(new TextDecoder().decode(result.stderr));
  return new TextDecoder().decode(result.stdout);
}

const service = (id: number) => `redpanda-${id}`;
const options = () => ({
  brokers,
  requestTimeoutMs: 300,
  connectTimeoutMs: 300,
  retry: { maxRetries: 1, initialBackoffMs: 25, maxBackoffMs: 50 },
});
const uniqueTopic = (prefix: string) => `${prefix}-${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;

async function waitFor<T>(fn: () => Promise<T | undefined>, timeoutMs = 15_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const result = await fn();
      if (result !== undefined) return result;
    } catch (error) { lastError = error; }
    await Bun.sleep(100);
  }
  throw lastError ?? new Error("Chaos condition timed out");
}

async function createTopic(client: Kafka, name: string, partitions = 1): Promise<void> {
  compose("exec", "-T", "redpanda-0", "rpk", "topic", "create", name, "-p", String(partitions), "-r", "3", "-X", "brokers=127.0.0.1:9092");
  await waitFor(async () => {
    const found = (await client.admin().metadata([name])).topics.find((item) => item.name === name);
    return found && !found.err && found.partitions.length === partitions ? true : undefined;
  });
}

async function start(id: number): Promise<void> {
  compose("start", service(id));
  await waitFor(async () => {
    const client = new Kafka({ brokers: [brokers[id]!], connectTimeoutMs: 300, requestTimeoutMs: 300, retry: { maxRetries: 0 } });
    try { return (await client.admin().metadata()).brokers.length ? true : undefined; }
    finally { await client.disconnect(); }
  }, 30_000);
  compose("exec", "-T", service(id), "rpk", "cluster", "health", "-e", "-X", "brokers=127.0.0.1:9092");
}

function stop(id: number): void { compose("stop", "--timeout", "1", service(id)); }
function pause(id: number): void { compose("pause", service(id)); }
function unpause(id: number): void { compose("unpause", service(id)); }

async function outcome(promise: Promise<unknown>, timeoutMs = 3_000): Promise<"fulfilled" | "rejected"> {
  return Promise.race([
    promise.then(() => "fulfilled" as const, () => "rejected" as const),
    Bun.sleep(timeoutMs).then(() => { throw new Error(`Operation remained pending for ${timeoutMs}ms`); }),
  ]);
}

async function leader(client: Kafka, name: string, partition = 0): Promise<number> {
  return waitFor(async () => {
    const found = (await client.admin().metadata([name])).topics.find((item) => item.name === name)?.partitions.find((item) => item.id === partition);
    return found && !found.err && found.leader >= 0 ? found.leader : undefined;
  });
}

async function scan(client: Kafka, name: string, partitions: number): Promise<KafkaMessage[]> {
  const consumer = client.consumer();
  const assignments = Array.from({ length: partitions }, (_, partition) => ({ topic: name, partition, offset: "earliest" as const }));
  await consumer.assign(assignments);
  const highs = await Promise.all(assignments.map(({ partition }) => consumer.watermarks(name, partition).then(({ high }) => high)));
  const messages: KafkaMessage[] = [];
  await waitFor(async () => {
    messages.push(...await consumer.fetch({ maxWaitMs: 50, maxMessages: 500, copy: true }));
    return assignments.every(({ partition }) => (consumer.position(name, partition) ?? 0n) >= highs[partition]!) ? true : undefined;
  });
  await consumer.close();
  return messages;
}

chaos("three-broker Kafka chaos", () => {
  test("bounds Produce and Fetch when a partition leader is killed, then recovers", async () => {
    const client = new Kafka(options());
    const name = uniqueTopic("leader-kill");
    let killed = -1;
    try {
      await createTopic(client, name);
      const producer = client.producer({ lingerMs: 0, idempotent: true });
      await producer.send({ topic: name, timeoutMs: 300, acks: "all", messages: [{ partition: 0, value: "before-kill" }] });
      const consumer = client.consumer({ fromBeginning: true });
      await consumer.assign([{ topic: name, partition: 0, offset: 0n }]);
      const held = (await consumer.fetch({ maxWaitMs: 50, maxMessages: 1 }))[0]!;
      killed = await leader(client, name);
      stop(killed);

      await outcome(producer.send({ topic: name, timeoutMs: 300, acks: "all", messages: [{ partition: 0, value: "during-kill" }] }));
      await outcome(consumer.fetch({ maxWaitMs: 10, maxMessages: 1 }));
      expect(decode(held.value)).toBe("before-kill");

      await start(killed);
      killed = -1;
      const recovered = new Kafka(options());
      try {
        await waitFor(async () => {
          await recovered.admin().metadata([name]);
          try {
            await recovered.producer({ lingerMs: 0, idempotent: true }).send({ topic: name, timeoutMs: 1_000, acks: "all", messages: [{ partition: 0, value: "after-kill" }] });
            return true;
          } catch { return undefined; }
        });
        const resumed = recovered.consumer();
        await resumed.assign([{ topic: name, partition: 0, offset: held.offset + 1n }]);
        await waitFor(async () => (await resumed.fetch({ maxWaitMs: 50 })).some((message) => decode(message.value) === "after-kill") ? true : undefined);
      } finally { await recovered.disconnect(); }
    } finally {
      if (killed >= 0) await start(killed);
      await client.disconnect();
    }
  }, 60_000);

  test("times out a blackholed leader and recovers after broker resume", async () => {
    const client = new Kafka({ ...options(), retry: { maxRetries: 0, initialBackoffMs: 0, maxBackoffMs: 0 } });
    const name = uniqueTopic("blackhole");
    let paused = -1;
    try {
      await createTopic(client, name);
      const producer = client.producer({ lingerMs: 0 });
      await producer.send({ topic: name, timeoutMs: 300, messages: [{ partition: 0, value: "warm" }] });
      paused = await leader(client, name);
      pause(paused);
      expect(await outcome(producer.send({ topic: name, timeoutMs: 200, messages: [{ partition: 0, value: "blackholed" }] }))).toBe("rejected");
      unpause(paused);
      paused = -1;
      await producer.send({ topic: name, timeoutMs: 1_000, messages: [{ partition: 0, value: "recovered" }] });
    } finally {
      if (paused >= 0) unpause(paused);
      await client.disconnect();
    }
  }, 30_000);

  test("keeps acknowledged data through a rolling broker restart", async () => {
    const client = new Kafka(options());
    const name = uniqueTopic("rolling");
    const acknowledged: string[] = [];
    let stopped = -1;
    try {
      await createTopic(client, name, 3);
      const producer = client.producer({ lingerMs: 0, idempotent: true });
      for (let id = 0; id < 3; id++) {
        stop(id);
        stopped = id;
        const messageId = `during-${id}-${crypto.randomUUID()}`;
        if (await outcome(producer.send({ topic: name, timeoutMs: 500, acks: "all", messages: [{ partition: id, value: messageId }] })) === "fulfilled") acknowledged.push(messageId);
        await start(id);
        stopped = -1;
      }
      const recovered = new Kafka(options());
      try {
        const finalId = `after-${crypto.randomUUID()}`;
        await waitFor(async () => {
          await recovered.admin().metadata([name]);
          try {
            await recovered.producer({ lingerMs: 0 }).send({ topic: name, timeoutMs: 1_000, acks: "all", messages: [{ partition: 0, value: finalId }] });
            return true;
          } catch { return undefined; }
        });
        acknowledged.push(finalId);
        const values = (await scan(recovered, name, 3)).map((message) => decode(message.value));
        for (const id of acknowledged) expect(values.filter((value) => value === id)).toHaveLength(1);
      } finally { await recovered.disconnect(); }
    } finally {
      if (stopped >= 0) await start(stopped);
      await client.disconnect();
    }
  }, 120_000);

  test("follows an explicit leader transfer", async () => {
    const client = new Kafka(options());
    const name = uniqueTopic("leader-transfer");
    try {
      await createTopic(client, name);
      const from = await leader(client, name);
      const target = (from + 1) % 3;
      compose("exec", "-T", "redpanda-0", "rpk", "cluster", "partitions", "transfer-leadership", "-p", `${name}/0:${target}`, "-X", "brokers=127.0.0.1:9092");
      await waitFor(async () => {
        try { return (await client.producer({ lingerMs: 0 }).send({ topic: name, messages: [{ partition: 0, value: "new-leader" }] }))[0]?.partition === 0 ? true : undefined; }
        catch { return undefined; }
      });
    } finally { await client.disconnect(); }
  }, 30_000);

  test("does not mix records across topic deletion and recreation", async () => {
    const client = new Kafka(options());
    const name = uniqueTopic("recreate");
    try {
      await createTopic(client, name);
      await client.producer({ lingerMs: 0 }).send({ topic: name, messages: [{ partition: 0, value: "old-topic" }] });
      compose("exec", "-T", "redpanda-0", "rpk", "topic", "delete", name, "-X", "brokers=127.0.0.1:9092");
      await waitFor(async () => !compose("exec", "-T", "redpanda-0", "rpk", "topic", "list", "-X", "brokers=127.0.0.1:9092").split("\n").includes(name) ? true : undefined);
      await createTopic(client, name);
      await client.producer({ lingerMs: 0 }).send({ topic: name, messages: [{ partition: 0, value: "new-topic" }] });
      expect((await scan(client, name, 1)).map((message) => decode(message.value))).toEqual(["new-topic"]);
    } finally { await client.disconnect(); }
  }, 60_000);

  (process.env.CHAOS_NETEM === "1" ? test : test.skip)("stays bounded under delay and packet loss profiles", async () => {
    const client = new Kafka(options());
    const name = uniqueTopic("netem");
    let id = -1;
    try {
      await createTopic(client, name);
      id = await leader(client, name);
      for (const [delay, jitter, loss] of [[10, 2, 0.1], [50, 10, 0.5], [200, 50, 2]]) {
        compose("exec", "-T", service(id), "sh", "-c", `tc qdisc replace dev eth0 root netem delay ${delay}ms ${jitter}ms loss ${loss}%`);
        await outcome(client.producer({ lingerMs: 0 }).send({ topic: name, timeoutMs: 1_000, messages: [{ partition: 0, value: `netem-${delay}` }] }), 3_000);
      }
    } finally {
      if (id >= 0) compose("exec", "-T", service(id), "sh", "-c", "tc qdisc del dev eth0 root || true");
      await client.disconnect();
    }
  }, 30_000);
});
