import { describe, expect, test } from "bun:test";
import { responseFromBytes } from "../../src/protocol/body.ts";
import {
  decoder,
  encoder,
  readAlterPartitionReassignmentsResponse,
  readElectLeadersResponse,
  readListPartitionReassignmentsResponse,
  writeAlterPartitionReassignmentsRequest,
  writeElectLeadersRequest,
  writeListPartitionReassignmentsRequest,
} from "../../src/protocol/index.ts";

describe("partition administration wire protocol", () => {
  test("encodes KIP-455 v0 and KIP-460 v1 requests", () => {
    const alter = decoder(
      writeAlterPartitionReassignmentsRequest(
        [{ topic: "events", partition: 2, replicas: [1, 3] }],
        5000,
      ).toBytes(),
    );
    expect(alter.i32()).toBe(5000);
    expect(
      alter.compactArray((topic) => {
        const value = {
          name: topic.compactString(),
          partitions: topic.compactArray((partition) => {
            const item = {
              index: partition.i32(),
              replicas: partition.compactArray((replica) => replica.i32()),
            };
            partition.skipTags();
            return item;
          }),
        };
        topic.skipTags();
        return value;
      }),
    ).toEqual([{ name: "events", partitions: [{ index: 2, replicas: [1, 3] }] }]);
    alter.skipTags();
    expect(alter.remaining).toBe(0);

    const list = decoder(writeListPartitionReassignmentsRequest(null, 6000).toBytes());
    expect(list.i32()).toBe(6000);
    expect(list.uvarint()).toBe(0);
    list.skipTags();
    expect(list.remaining).toBe(0);

    const elect = decoder(
      writeElectLeadersRequest("unclean", [{ topic: "events", partition: 2 }], 7000).toBytes(),
    );
    expect(elect.i8()).toBe(1);
    expect(
      elect.array((topic) => [topic.string(), topic.array((partition) => partition.i32())]),
    ).toEqual([["events", [2]]]);
    expect(elect.i32()).toBe(7000);
  });

  test("decodes per-partition results and reassignment state", () => {
    const alter = readAlterPartitionReassignmentsResponse(
      responseFromBytes(
        encoder()
          .i32(1)
          .i16(0)
          .compactString(null)
          .compactArray(["events"], (topic) =>
            topic
              .compactString("events")
              .compactArray([2], (partition) =>
                partition.i32(2).i16(39).compactString("invalid replicas").tags(),
              )
              .tags(),
          )
          .tags()
          .result(),
      ),
    );
    expect(alter.results).toEqual([
      { topic: "events", partition: 2, error: 39, message: "invalid replicas" },
    ]);

    const list = readListPartitionReassignmentsResponse(
      responseFromBytes(
        encoder()
          .i32(2)
          .i16(0)
          .compactString(null)
          .compactArray(["events"], (topic) =>
            topic
              .compactString("events")
              .compactArray([2], (partition) =>
                partition
                  .i32(2)
                  .compactArray([1, 3], (replica, id) => replica.i32(id))
                  .compactArray([3], (replica, id) => replica.i32(id))
                  .compactArray([2], (replica, id) => replica.i32(id))
                  .tags(),
              )
              .tags(),
          )
          .tags()
          .result(),
      ),
    );
    expect(list.reassignments).toEqual([
      {
        topic: "events",
        partition: 2,
        replicas: [1, 3],
        addingReplicas: [3],
        removingReplicas: [2],
      },
    ]);

    const elect = readElectLeadersResponse(
      responseFromBytes(
        encoder()
          .i32(3)
          .i16(0)
          .array(["events"], (topic) =>
            topic.string("events").array([2], (partition) => partition.i32(2).i16(0).string(null)),
          )
          .result(),
      ),
    );
    expect(elect.results).toEqual([{ topic: "events", partition: 2, error: 0, message: null }]);
  });
});
