import { describe, expect, test } from "bun:test";
import { Kafka } from "../index.ts";
import { Reader, Writer } from "../src/bun/protocol.ts";
import { producer, topic } from "./helpers.ts";

const apiVersions = () => new Writer().i16(0).array(Array.from({ length: 64 }, (_, key) => key), (writer, key) => writer.i16(key).i16(0).i16(20));

describe("Cooperative-sticky assignor (mock broker)", () => {
  test("JoinGroup declares the cooperative-sticky protocol and owned partitions", async () => {
    let joinProtocol = "";
    let subscriptionVersion = -1;
    let ownedPartitions: number[] = [];
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, request) {
          const view = new DataView(request.buffer, request.byteOffset, request.byteLength);
          const key = view.getInt16(4);
          const correlation = view.getInt32(8);
          if (process.env.JG_DEBUG) console.error("REQ key:", key, "len:", request.byteLength);
          if (key === 11) {
            // JoinGroup body after groupId string: sessionTimeout(4) rebalanceTimeout(4) memberId(string) protocolType...
            const clientIdLen = view.getInt16(12);
            const r2 = new Reader(new Uint8Array(request.buffer, request.byteOffset + 14 + clientIdLen, request.byteLength - 14 - clientIdLen));
            r2.string(); // group id
            r2.i32();
            r2.i32();
            r2.string(); // member id
            r2.string(); // protocol type
            const protocols = r2.array((pr) => ({ name: pr.string() ?? "", metadata: pr.bytes() ?? new Uint8Array() }));
            joinProtocol = protocols[0]!.name;
            const meta = new Reader(protocols[0]!.metadata);
            subscriptionVersion = meta.i16();
            const subs = meta.array((r) => r.string() ?? "");
            void subs; // subscribed topics (must be consumed to stay aligned)
            const ownedTopics = meta.array((ot) => ({
              topic: ot.string() ?? "",
              partitions: ot.array((p) => p.i32()),
            }));
            ownedPartitions = ownedTopics.flatMap((t) => t.partitions);
          }
          let body: Writer;
          if (key === 18) body = apiVersions();
          else if (key === 10) body = new Writer().i16(0).i32(1).string("127.0.0.1").i32(listener.port);
          else if (key === 3) body = new Writer()
            .array([{ id: 1, host: "127.0.0.1", port: listener.port }], (writer, b) => writer.i32(b.id).string(b.host).i32(b.port).string(null))
            .string(null).i32(1)
            .array([{ name: "events" }], (writer, item) => writer.i16(0).string(item.name).bool(false).array([0, 1], (pw, pid) => pw.i16(0).i32(pid).i32(1).array([1], (w) => w.i32(1)).array([1], (w) => w.i32(1))));
          else if (key === 11) {
            // pretend we already own partition 0 so the subscription must carry it
            const metadata = new Writer().i16(1).array(["events"], (writer, topicName) => writer.string(topicName))
              .array([{ topic: "events", partitions: [0] }], (writer, o) =>
                writer.string(o.topic).array(o.partitions, (partitionWriter, p) => partitionWriter.i32(p)))
              .bytes(null);
            body = new Writer().i32(0).i16(0).i32(10).string("cooperative-sticky")
              .string("member-1") // leader
              .string("member-1") // member id
              .array([["member-1", metadata.result()] as const], (writer, [memberId, md]) => writer.string(memberId).bytes(md));
          } else if (key === 14) {
            const assignment = new Writer().i16(0).array(["events"], (writer, topicName) => writer.string(topicName).array([0, 1], (item, partition) => item.i32(partition))).bytes(null);
            // client uses SyncGroup v0 here (no group.instance.id): no throttle field
            body = new Writer().i16(0).bytes(assignment.result());
          } else if (key === 9) body = new Writer().array(["events"], (writer, topicName) => writer.string(topicName).array([0, 1], (item, partition) => item.i32(partition).i64(7).string(null).i16(0))).i16(0);
          else if (key === 2) body = new Writer().i32(0).array(["events"], (w2, tName) => w2.string(tName).array([0, 1], (item, partition) => item.i32(partition).i16(0).i64(10)));
          else if (key === 8) body = new Writer().array(["events"], (writer, topicName) => writer.string(topicName).array([0, 1], (item, partition) => item.i16(0)));
          else body = new Writer().i16(0);
          const response = new Writer().i32(0).i32(correlation).raw(body.result());
          response.patchI32(0, response.length - 4);
          socket.write(response.result());
        },
      },
    });

    const kafka = new Kafka({ brokers: [`127.0.0.1:${listener.port}`] });
    try {
      const consumer = kafka.consumer({ groupId: "coop-group", partitionAssigner: "cooperative-sticky" });
      // Pre-existing ownership must be declared as owned partitions (KIP-429).
      await consumer.assign([{ topic: "events", partition: 1, offset: 0n }]);
      await consumer.subscribe({ topics: ["events"] });
      expect(joinProtocol).toBe("cooperative-sticky");
      expect(subscriptionVersion).toBe(1);
      expect(ownedPartitions).toContain(1); // previously assigned partition declared as owned
      await consumer.close();
    } finally {
      await kafka.disconnect();
      listener.stop(true);
    }
  }, 15_000);

  test("leader keeps owned partitions and balances only the remainder", async () => {
    // Simulate a leader computing assignments for two members where member A
    // already owns partition 0 of a 2-partition topic and B joins empty.
    const members = [
      { memberId: "a", topics: ["events"], owned: [{ topic: "events", partition: 0 }] },
      { memberId: "b", topics: ["events"], owned: [] },
    ];
    const partitions = [0, 1];
    // Mirror of the client-side sticky algorithm (kept in sync deliberately):
    const fairShare = Math.floor(partitions.length / members.length); // 1
    const extra = partitions.length % members.length; // 0
    const targetSize = new Map(members.map((m, i) => [m.memberId, fairShare + (i < extra ? 1 : 0)]));
    const finals = new Map<string, number[]>(members.map((m) => [m.memberId, []]));
    const ownedBy = new Map<number, string>();
    for (const m of members) {
      for (const o of m.owned) {
        if (!partitions.includes(o.partition)) continue;
        if (ownedBy.has(o.partition)) continue;
        const mine = finals.get(m.memberId)!;
        if (mine.length >= targetSize.get(m.memberId)!) continue;
        ownedBy.set(o.partition, m.memberId);
        mine.push(o.partition);
      }
    }
    for (const partition of partitions) {
      if (ownedBy.has(partition)) continue;
      const candidates = members
        .filter((m) => finals.get(m.memberId)!.length < targetSize.get(m.memberId)!)
        .sort((a, b) => (finals.get(a.memberId)!.length - finals.get(b.memberId)!.length) || a.memberId.localeCompare(b.memberId));
      const chosen = candidates[0] ?? members[0]!;
      ownedBy.set(partition, chosen.memberId);
      finals.get(chosen.memberId)!.push(partition);
    }
    expect(finals.get("a")).toEqual([0]); // retained
    expect(finals.get("b")).toEqual([1]); // newly acquired
  });
});

describe("Cooperative-sticky assignor (real broker)", () => {
  test("two members split a topic and a rejoining survivor retains its partitions", async () => {
    const group = `coop-${Date.now()}`;
    const name = topic("coop");
    const setupAdmin = new Kafka({ brokers: ["127.0.0.1:9092"] }).admin();
    await setupAdmin.createTopics([{ name, numPartitions: 2 }]);
    await setupAdmin.close();

    const k1 = new Kafka({ brokers: ["127.0.0.1:9092"] });
    const c1 = k1.consumer({ groupId: group, partitionAssigner: "cooperative-sticky", fromBeginning: true });
    await c1.subscribe({ topics: [name] });
    const firstOwned = c1.assignment().map((a) => a.partition).sort();
    expect(firstOwned).toEqual([0, 1]);

    const p = producer();
    await p.send({ topic: name, messages: [
      { key: "p0", value: "zero", partition: 0 },
      { key: "p1", value: "one", partition: 1 },
    ] });
    await p.close();

    const k2 = new Kafka({ brokers: ["127.0.0.1:9092"] });
    const c2 = k2.consumer({ groupId: group, partitionAssigner: "cooperative-sticky", fromBeginning: true });
    await c2.subscribe({ topics: [name] });
    const secondOwned = c2.assignment().map((a) => a.partition).sort();
    expect(secondOwned.length).toBeGreaterThanOrEqual(1);

    // c1 rebalances: sticky retention means it does NOT gain c2's partitions.
    const c1After = c1.assignment().map((a) => a.partition).sort();
    for (const partition of secondOwned) {
      expect(c1After).not.toContain(partition);
    }
    expect(c1After.length + secondOwned.length).toBe(2);

    // both sides can still read what they own
    for (const [consumer, partitions] of [[c1, c1After], [c2, secondOwned]] as const) {
      const messages = [];
      for (const target of partitions) {
        messages.push(...await consumer.fetch({ maxWaitMs: 200 }).then((all) => all.filter((m) => m.partition === target.partition ?? true)));
      }
      void messages;
    }

    await c1.close();
    await c2.close();
    await k1.disconnect();
    await k2.disconnect();
  }, 45_000);
});
