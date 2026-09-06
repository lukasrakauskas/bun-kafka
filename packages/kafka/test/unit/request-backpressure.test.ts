import { expect, spyOn, test } from "bun:test";
import { RequestTracker } from "../../src/bun/connection/requests.ts";
import { ConnectionMetrics } from "../../src/bun/connection/metrics.ts";
import { Connection } from "../../src/bun/connection/connection.ts";
import { requestFromBuilder } from "../../src/protocol/frame.ts";
import { ResponseFramer } from "../../src/bun/connection/response-framer.ts";
import {
  writeResponseFrame,
  writeEmptyRequest,
  writeRequestFrame,
} from "../../src/protocol/index.ts";

test("partial writes preserve frame order and acks=0 waits for drain", async () => {
  const tracker = new RequestTracker("test", "test", new ConnectionMetrics());
  const accepted: number[] = [];
  const steps = [3, 0, 0, 2];
  let calls = 0;
  const socket = {
    write(frame: Uint8Array) {
      calls++;
      const size = Math.min(steps.shift() ?? frame.length, frame.length);
      accepted.push(...frame.subarray(0, size));
      return size;
    },
    end() {},
  } as unknown as Bun.Socket;
  const body = writeEmptyRequest();
  const first = tracker.request(socket, 18, 0, body, 1000);
  let sent = false;
  const second = Promise.resolve(tracker.sendOnly(socket, 18, 0, body, 1000)).then(() => {
    sent = true;
  });
  await Promise.resolve();
  expect(sent).toBe(false);
  expect(calls).toBe(1);
  for (let i = 0; i < 4; i++) tracker.drain(socket);
  await second;
  expect(calls).toBe(6);
  expect(accepted).toEqual(
    [1, 2].flatMap((correlationId) => [
      ...writeRequestFrame({ apiKey: 18, apiVersion: 0, correlationId, clientId: "test", body }),
    ]),
  );
  tracker.receive(new Uint8Array([0, 0, 0, 1]));
  await first;
});

for (const failure of ["negative", "throw", "timeout", "close"] as const) {
  test(`${failure} clears blocked writes, rejects both send modes and permits fresh frames`, async () => {
    let aborted = 0;
    let ended = 0;
    let calls = 0;
    let failing = false;
    const tracker = new RequestTracker("test", "test", new ConnectionMetrics(), () => {
      aborted++;
    });
    const socket = {
      write() {
        calls++;
        if (failing && failure === "negative") return -1;
        if (failing && failure === "throw") throw new Error("write failed");
        return 1;
      },
      end() {
        ended++;
      },
    } as unknown as Bun.Socket;
    const body = writeEmptyRequest();
    const first = tracker.request(socket, 18, 0, body, 20);
    const second = tracker.sendOnly(socket, 18, 0, body, 20);
    const results = Promise.allSettled([first, second]);
    failing = true;
    if (failure === "close") tracker.fail(new Error("closed"));
    else if (failure !== "timeout") tracker.drain(socket);
    expect((await results).map((result) => result.status)).toEqual(["rejected", "rejected"]);
    const before = calls;
    tracker.drain(socket);
    expect(calls).toBe(before);
    await Bun.sleep(30);
    expect(aborted).toBe(failure === "close" ? 0 : 1);
    expect(ended).toBe(failure === "close" ? 0 : 1);
    const bytes: number[] = [];
    const fresh = {
      write(frame: Uint8Array) {
        bytes.push(...frame);
        return frame.length;
      },
    } as unknown as Bun.Socket;
    await tracker.sendOnly(fresh, 18, 0, body, 20);
    expect(bytes).toEqual([
      ...writeRequestFrame({ apiKey: 18, apiVersion: 0, correlationId: 3, clientId: "test", body }),
    ]);
  });
}

test("acks=0 timeout aborts a zero-progress stream; response timeout does not", async () => {
  let ended = 0;
  const tracker = new RequestTracker("test", "test", new ConnectionMetrics());
  const socket = {
    write: () => 0,
    end() {
      ended++;
    },
  } as unknown as Bun.Socket;
  await expect(tracker.sendOnly(socket, 18, 0, writeEmptyRequest(), 10)).rejects.toThrow(
    "timed out",
  );
  expect(ended).toBe(1);
  socket.write = (frame) => (frame as Uint8Array).byteLength;
  await expect(tracker.request(socket, 18, 0, writeEmptyRequest(), 10)).rejects.toThrow(
    "timed out",
  );
  expect(ended).toBe(1);
});

test("Connection reconnect never replays queued bytes or accepts stale socket events", async () => {
  const handlers: Bun.SocketHandler<undefined>[] = [];
  const sockets: Bun.Socket[] = [];
  const frames: Uint8Array[] = [];
  const connect = spyOn(Bun, "connect").mockImplementation((options) => {
    handlers.push(options.socket as Bun.SocketHandler<undefined>);
    const index = sockets.length;
    const socket = {
      write(frame: Uint8Array) {
        if (index === 0) return 3;
        frames.push(frame.slice());
        return frame.length;
      },
      end() {},
    } as unknown as Bun.Socket;
    sockets.push(socket);
    return Promise.resolve(socket);
  });
  const connection = new Connection("localhost:9092", {
    clientId: "test",
    requestTimeoutMs: 1000,
    connectTimeoutMs: 1000,
    maxResponseBytes: 1024,
  });
  try {
    const body = writeEmptyRequest();
    const pending = Promise.allSettled([
      connection.request(18, 0, body),
      connection.sendOnly(18, 0, body),
    ]);
    await Bun.sleep(0);
    handlers[0]!.close!(sockets[0]!);
    expect((await pending).map((result) => result.status)).toEqual(["rejected", "rejected"]);
    const fresh = connection.request(18, 0, body);
    await Bun.sleep(0);
    handlers[0]!.drain!(sockets[0]!);
    handlers[0]!.close!(sockets[0]!);
    handlers[0]!.data!(sockets[0]!, Buffer.from([0xff, 0xff, 0xff, 0xff]));
    handlers[1]!.data!(sockets[1]!, Buffer.from(writeResponseFrame(3, new Uint8Array())));
    await fresh;
    expect(frames).toEqual([
      writeRequestFrame({ apiKey: 18, apiVersion: 0, correlationId: 3, clientId: "test", body }),
    ]);
    const closing = Promise.allSettled([connection.request(18, 0, body)]);
    await Bun.sleep(0);
    connection.close();
    expect((await closing)[0]!.status).toBe("rejected");
  } finally {
    connection.close();
    connect.mockRestore();
  }
});

test("real TCP throttled reader receives complete large frames through Connection drain", async () => {
  const payload = new Uint8Array(16 * 1024 * 1024).fill(0x5a);
  const body = requestFromBuilder((writer) => writer.raw(payload));
  const framer = new ResponseFramer(64 * 1024 * 1024);
  const received: number[] = [];
  const parsed = Promise.withResolvers<void>();
  const timers = new Set<ReturnType<typeof setTimeout>>();
  let peer: Bun.Socket | undefined;
  let shortWrites = 0;
  let drains = 0;
  const originalDrain = RequestTracker.prototype.drain;
  const drain = spyOn(RequestTracker.prototype, "drain").mockImplementation(
    function (this: RequestTracker, socket) {
      drains++;
      const write = socket.write;
      socket.write = function (frame) {
        const written = write.call(this, frame);
        if (written >= 0 && written < (frame as Uint8Array).byteLength) shortWrites++;
        return written;
      };
      try {
        originalDrain.call(this, socket);
      } finally {
        socket.write = write;
      }
    },
  );
  const server = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      open(socket) {
        peer = socket;
        socket.pause();
        timers.add(setTimeout(() => socket.resume(), 50));
      },
      data(socket, data) {
        try {
          for (const frame of framer.push(new Uint8Array(data))) {
            const correlation = new DataView(frame.buffer, frame.byteOffset).getInt32(4);
            expect(frame).toEqual(
              writeRequestFrame({
                apiKey: 18,
                apiVersion: 0,
                correlationId: correlation,
                clientId: "test",
                body,
              }).subarray(4),
            );
            received.push(correlation);
            if (correlation === 1) socket.write(writeResponseFrame(correlation, new Uint8Array()));
            if (received.length === 2) parsed.resolve();
          }
          socket.pause();
          timers.add(setTimeout(() => socket.resume(), 1));
        } catch (error) {
          parsed.reject(error);
        }
      },
    },
  });
  const connection = new Connection(`127.0.0.1:${server.port}`, {
    clientId: "test",
    requestTimeoutMs: 10000,
    connectTimeoutMs: 1000,
    maxResponseBytes: 1024,
  });
  try {
    await Promise.all([
      connection.request(18, 0, body),
      connection.sendOnly(18, 0, body),
      parsed.promise,
    ]);
    expect(received).toEqual([1, 2]);
    expect(shortWrites).toBeGreaterThan(0);
    expect(drains).toBeGreaterThan(2);
  } finally {
    connection.close();
    peer?.terminate();
    server.stop(true);
    for (const timer of timers) clearTimeout(timer);
    drain.mockRestore();
  }
}, 15000);
