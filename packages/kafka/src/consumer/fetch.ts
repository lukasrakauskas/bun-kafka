import { KafkaError, KafkaErrorCode } from "../errors.ts";
import type { ConsumedMessage } from "../types.ts";
import type { RecordSetDecoder } from "../protocol/index.ts";
import { createRecordSetDecoder, readFetchResponse, writeFetchRequest } from "../protocol/index.ts";
import type { Cluster } from "../bun/cluster.ts";
import {
  API_FETCH,
  DEFAULT_FETCH_IDLE_SLEEP_MS,
  DEFAULT_FETCH_MAX_BYTES,
  DEFAULT_FETCH_MAX_MESSAGES,
  DEFAULT_FETCH_MAX_PARTITION_BYTES,
  DEFAULT_FETCH_MAX_WAIT_MS,
  FETCH_API_VERSION,
  FETCH_RACK_API_VERSION,
  HEX_DUMP_BYTES,
  RADIX_HEX,
  kafkaError,
  partitionKey,
  retryDelay,
} from "../bun/shared.ts";
import type { Assigned, ConsumerSettings, ConsumerState, FetchOptions } from "./types.ts";

type FetchSessionState = {
  id: number;
  epoch: number;
  sent: Map<string, bigint>;
  streaming: Map<string, boolean>;
};

export class Fetcher<K, V> {
  #sessions = new Map<number, FetchSessionState>();
  #preferredReplicas = new Map<string, number>();
  constructor(
    private readonly cluster: Cluster,
    private readonly options: ConsumerSettings,
    private readonly state: ConsumerState,
    private readonly assigned: Map<string, Assigned>,
    private readonly positions: Map<string, bigint>,
    private readonly paused: Set<string>,
    private readonly decoder: {
      add(decoders: RecordSetDecoder[]): void;
      drain(max: number): Array<ConsumedMessage<K, V>>;
      pending: boolean;
    },
    private readonly onCommit: () => Promise<void>,
  ) {}

  reset(): void {
    this.#sessions.clear();
    this.#preferredReplicas.clear();
  }
  resetPartition(key: string): void {
    this.#preferredReplicas.delete(key);
    for (const session of this.#sessions.values()) {
      session.sent.delete(key);
      session.streaming.delete(key);
    }
  }

  // Retry handling surrounds the complete fetch transaction.
  // eslint-disable-next-line sonarjs/cognitive-complexity
  async fetch(options: FetchOptions): Promise<Array<ConsumedMessage<K, V>>> {
    for (let attempt = 0; attempt <= this.cluster.retryOptions.maxRetries; attempt++) {
      try {
        const messages = await this.once(options);
        if (messages.length && this.state.groupId && this.options.autoCommit) {
          await this.onCommit();
        }
        return messages;
      } catch (error) {
        if (!(error instanceof KafkaError)) {
          throw error;
        }
        if (!error.retriable || attempt === this.cluster.retryOptions.maxRetries) {
          throw error;
        }
        await this.retry(attempt, error);
      }
    }
    return [];
  }

  // The fetch path combines broker sessions, retries, and decoder draining.
  // eslint-disable-next-line sonarjs/cognitive-complexity
  private async once(options: FetchOptions): Promise<Array<ConsumedMessage<K, V>>> {
    const maxMessages = options.maxMessages ?? DEFAULT_FETCH_MAX_MESSAGES;
    if (!Number.isSafeInteger(maxMessages) || maxMessages < 1) {
      throw new RangeError("maxMessages must be a positive integer");
    }
    if (this.decoder.pending) {
      return this.decoder.drain(maxMessages);
    }
    const active = [...this.assigned].filter(([key]) => !this.paused.has(key));
    if (!active.length) {
      await Bun.sleep(
        Math.min(options.maxWaitMs ?? DEFAULT_FETCH_MAX_WAIT_MS, DEFAULT_FETCH_IDLE_SLEEP_MS),
      );
      return [];
    }
    const isolationLevel = this.options.isolationLevel === "read_committed" ? 1 : 0;
    const brokers = Map.groupBy(
      active,
      ([key, assignment]) => this.#preferredReplicas.get(key) ?? assignment.leader,
    );
    const batches = await Promise.all(
      [...brokers].map(([broker, entries]) =>
        this.fetchBroker(broker, entries, options, isolationLevel),
      ),
    );
    this.decoder.add(batches.flat());
    return this.decoder.drain(maxMessages);
  }

  // The fetch path combines broker sessions and protocol response handling.
  // eslint-disable-next-line sonarjs/cognitive-complexity
  private async fetchBroker(
    broker: number,
    entries: Array<[string, Assigned]>,
    options: FetchOptions,
    isolationLevel: number,
  ): Promise<RecordSetDecoder[]> {
    const session = this.#sessions.get(broker) ?? {
      id: 0,
      epoch: 0,
      sent: new Map(),
      streaming: new Map(),
    };
    const requested = entries.filter(([key]) => {
      const position = this.positions.get(key) ?? 0n;
      return (
        session.epoch === 0 || session.sent.get(key) !== position || session.streaming.get(key)
      );
    });
    const forgotten = new Map<string, number[]>();
    for (const key of [...session.sent.keys(), ...session.streaming.keys()]) {
      if (this.assigned.has(key) && !this.paused.has(key)) {
        continue;
      }
      const [topic, partition] = key.split("\0");
      if (topic !== undefined && partition !== undefined) {
        forgotten.set(topic, [...(forgotten.get(topic) ?? []), Number(partition)]);
      }
      session.sent.delete(key);
      session.streaming.delete(key);
    }
    const response = await this.cluster.request(
      broker,
      API_FETCH,
      this.cluster.rackId === undefined ? FETCH_API_VERSION : FETCH_RACK_API_VERSION,
      writeFetchRequest(
        options.maxWaitMs ?? DEFAULT_FETCH_MAX_WAIT_MS,
        options.minBytes ?? 1,
        options.maxBytes ?? this.options.fetchMaxBytes ?? DEFAULT_FETCH_MAX_BYTES,
        isolationLevel,
        session.id,
        session.epoch,
        requested.map(([key, assignment]) => ({
          topic: assignment.topic,
          partition: assignment.partition,
          offset: this.positions.get(key) ?? 0n,
          maxPartitionBytes: options.maxPartitionBytes ?? DEFAULT_FETCH_MAX_PARTITION_BYTES,
        })),
        forgotten,
        this.cluster.rackId,
      ),
      (options.maxWaitMs ?? DEFAULT_FETCH_MAX_WAIT_MS) + this.cluster.requestTimeoutMs,
      false,
    );
    if (process.env.DEBUG_FETCH) {
      console.error(
        "fetch resp:",
        Array.from(response.data.slice(0, HEX_DUMP_BYTES))
          .map((b) => b.toString(RADIX_HEX).padStart(2, "0"))
          .join(" "),
      );
    }
    const fetched = readFetchResponse(response, this.cluster.rackId !== undefined);
    this.cluster.throttle(API_FETCH, fetched.throttleMs);
    if (fetched.topError) {
      this.#sessions.delete(broker);
      if (
        fetched.topError === KafkaErrorCode.FETCH_SESSION_ID_NOT_FOUND ||
        fetched.topError === KafkaErrorCode.INVALID_FETCH_SESSION_EPOCH
      ) {
        return this.fetchBroker(broker, entries, options, isolationLevel);
      }
      throw kafkaError(fetched.topError, `Fetch from broker ${broker}`);
    }
    session.id = session.id === 0 ? fetched.sessionId : session.id;
    session.epoch = session.id === fetched.sessionId && session.epoch === 0 ? 1 : session.epoch + 1;
    this.#sessions.set(broker, session);
    let routeChanged = false;
    const decoders = fetched.partitions
      .map((partition) => {
        const key = partitionKey(partition.topic, partition.partition);
        session.sent.set(key, this.positions.get(key) ?? 0n);
        session.streaming.set(key, Boolean(partition.records));
        if (partition.error) {
          throw kafkaError(partition.error, `${partition.topic}[${partition.partition}]`);
        }
        if (
          partition.preferredReadReplica >= 0 &&
          this.#preferredReplicas.get(key) !== partition.preferredReadReplica
        ) {
          this.#preferredReplicas.set(key, partition.preferredReadReplica);
          routeChanged = true;
        }
        return partition.records
          ? createRecordSetDecoder(
              partition.records,
              partition.topic,
              partition.partition,
              broker,
              {
                minOffset: this.positions.get(key) ?? 0n,
                copy: options.copy,
                abortedTransactions:
                  isolationLevel === 1 ? partition.abortedTransactions : undefined,
              },
            )
          : null;
      })
      .filter((decoder): decoder is RecordSetDecoder => decoder !== null);
    if (routeChanged) {
      this.#sessions.clear();
    }
    return decoders;
  }

  private async retry(attempt: number, error: KafkaError): Promise<void> {
    this.#preferredReplicas.clear();
    this.#sessions.clear();
    for (const assigned of this.assigned.values()) {
      const metadata = await this.cluster.topic(assigned.topic, true);
      const partition = metadata.partitions.find((item) => item.id === assigned.partition);
      if (partition) {
        assigned.leader = partition.leader;
      }
    }
    this.cluster.bumpRetries();
    const delay = retryDelay(this.cluster.retryOptions, attempt);
    this.cluster.log(
      "warn",
      `retrying fetch attempt ${attempt + 1} in ${delay}ms: ${String(error)}`,
    );
    this.cluster.event({
      type: "retry",
      apiKey: API_FETCH,
      attempt: attempt + 1,
      delayMs: delay,
      error,
    });
    if (delay) {
      await Bun.sleep(delay);
    }
  }
}
