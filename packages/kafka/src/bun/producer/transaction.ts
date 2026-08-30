import { Cluster } from "../cluster.ts";
import { withRetry } from "./retry.ts";
import {
  kafkaError,
  partitionKey,
  type CommittedOffset,
  API_ADD_OFFSETS_TO_TXN,
  API_ADD_PARTITIONS_TO_TXN,
  API_END_TXN,
  API_INIT_PRODUCER_ID,
  API_TXN_OFFSET_COMMIT,
  DEFAULT_TRANSACTION_TIMEOUT_MS,
} from "../shared.ts";
import {
  readEndTransactionResponse,
  readInitProducerIdResponse,
  readTopicPartitionErrors,
  readTransactionOffsetCommitResponse,
  writeAddOffsetsToTransactionRequest,
  writeAddPartitionsToTransactionRequest,
  writeEndTransactionRequest,
  writeInitProducerIdRequest,
  writeTransactionOffsetCommitRequest,
  type RequestBody,
  type ResponseBody,
} from "../../protocol/index.ts";

export type ProducerIdentity = { id: bigint; epoch: number };
export type TransactionPartition = { topic: string; partition: number };

export interface ProducerTransactionOptions {
  transactionalId?: string;
  transactionTimeoutMs?: number;
  onProducerId?: () => void;
}

export class ProducerTransaction {
  #cluster: Cluster;
  #transactionalId?: string;
  #transactionTimeoutMs: number;
  #onProducerId: () => void;
  #producer?: ProducerIdentity;
  #open = false;
  #addedPartitions = new Set<string>();
  #coordinator?: number;

  constructor(cluster: Cluster, options: ProducerTransactionOptions = {}) {
    this.#cluster = cluster;
    this.#transactionalId = options.transactionalId;
    this.#transactionTimeoutMs = options.transactionTimeoutMs ?? DEFAULT_TRANSACTION_TIMEOUT_MS;
    this.#onProducerId = options.onProducerId ?? (() => {});
  }

  get transactionalId(): string | undefined {
    return this.#transactionalId;
  }

  get isTransactional(): boolean {
    return Boolean(this.#transactionalId);
  }

  get isOpen(): boolean {
    return this.#open;
  }

  get producer(): ProducerIdentity | undefined {
    return this.#producer;
  }

  async begin(): Promise<void> {
    if (!this.#transactionalId) {
      throw new Error("beginTransaction requires a transactionalId producer option");
    }
    if (this.#open) {
      throw new Error("A transaction is already in progress");
    }
    if (!this.#producer) {
      await this.initProducerId();
    }
    this.#open = true;
    this.#addedPartitions.clear();
  }

  async commit(flush: () => Promise<void>): Promise<void> {
    await this.#end(true, `Commit transaction ${this.#transactionalId}`, flush);
    await this.endTxnCleanup();
  }

  async abort(flush: () => Promise<void>): Promise<void> {
    await this.#end(false, `Abort transaction ${this.#transactionalId}`, flush);
    await this.endTxnCleanup();
  }

  async endTxnCleanup(): Promise<void> {
    this.#open = false;
    this.#addedPartitions.clear();
    // KIP-360: bump the producer epoch after a completed transaction.
    await this.initProducerId();
  }

  async sendOffsetsToTransaction(
    offsets: readonly CommittedOffset[],
    groupId: string,
  ): Promise<void> {
    const transactionalId = this.#requireOpen(
      "sendOffsetsToTransaction requires an open transaction",
    );
    if (!offsets.length) {
      return;
    }
    const producer = this.#producer;
    if (!producer) {
      throw new Error("Producer identity is not initialized");
    }
    const topics = Map.groupBy(offsets, (offset) => offset.topic);
    const body = writeTransactionOffsetCommitRequest(
      transactionalId,
      groupId,
      producer.id,
      producer.epoch,
      topics,
    );
    const addOffsetsResponse = await this.#txnCoordinatorRequest(
      API_ADD_OFFSETS_TO_TXN,
      0,
      writeAddOffsetsToTransactionRequest(transactionalId, producer.id, producer.epoch, groupId),
    );
    const addOffsets = readEndTransactionResponse(addOffsetsResponse);
    this.#cluster.throttle(API_ADD_OFFSETS_TO_TXN, addOffsets.throttleMs);
    if (addOffsets.error) {
      throw kafkaError(addOffsets.error, `AddOffsetsToTxn group ${groupId}`);
    }
    const response = await this.#txnCoordinatorRequest(API_TXN_OFFSET_COMMIT, 0, body);
    const offsetCommit = readTransactionOffsetCommitResponse(response);
    this.#cluster.throttle(API_TXN_OFFSET_COMMIT, offsetCommit.throttleMs);
    for (const result of offsetCommit.topics) {
      for (const partition of result.partitions) {
        if (partition.error) {
          throw kafkaError(partition.error, `${result.topic}[${partition.index}]`);
        }
      }
    }
  }

  async addPartitions(partitions: readonly { topic: string; partition: number }[]): Promise<void> {
    const producer = this.#producer;
    const transactionalId = this.#transactionalId;
    if (!transactionalId || !this.#open || !producer) {
      return;
    }
    const fresh = partitions.filter(
      (partition) => !this.#addedPartitions.has(partitionKey(partition.topic, partition.partition)),
    );
    if (!fresh.length) {
      return;
    }
    const byTopic = Map.groupBy(fresh, (partition) => partition.topic);
    const response = await this.#txnCoordinatorRequest(
      API_ADD_PARTITIONS_TO_TXN,
      1,
      writeAddPartitionsToTransactionRequest(transactionalId, producer.id, producer.epoch, byTopic),
    );
    const added = readTopicPartitionErrors(response);
    this.#cluster.throttle(API_ADD_PARTITIONS_TO_TXN, added.throttleMs);
    for (const topic of added.topics) {
      for (const partition of topic.partitions) {
        if (partition.error) {
          throw kafkaError(partition.error, `AddPartitionsToTxn ${topic.name}[${partition.index}]`);
        }
      }
    }
    for (const partition of fresh) {
      this.#addedPartitions.add(partitionKey(partition.topic, partition.partition));
    }
  }

  async initProducerId(): Promise<void> {
    if (this.#transactionalId) {
      this.#coordinator = await this.#cluster.findTxnCoordinator(this.#transactionalId);
    }
    await withRetry(
      this.#cluster.retryOptions,
      async () => {
        const response = await this.#txnCoordinatorRequest(
          API_INIT_PRODUCER_ID,
          1,
          writeInitProducerIdRequest(this.#transactionalId ?? null, this.#transactionTimeoutMs),
        );
        const result = readInitProducerIdResponse(response);
        this.#cluster.throttle(API_INIT_PRODUCER_ID, result.throttleMs);
        if (result.error) {
          throw kafkaError(result.error, "Initialize idempotent producer");
        }
        this.#producer = { id: result.producerId, epoch: result.producerEpoch };
        this.#onProducerId();
      },
      (attempt, error, delay) => {
        this.#cluster.log("warn", `retrying InitProducerId attempt ${attempt + 1} in ${delay}ms`);
        this.#cluster.event({
          type: "retry",
          apiKey: API_INIT_PRODUCER_ID,
          attempt: attempt + 1,
          delayMs: delay,
          error,
        });
      },
    );
  }

  async #end(committed: boolean, label: string, flush: () => Promise<void>): Promise<void> {
    const transactionalId = this.#requireOpen("No transaction is in progress");
    await flush();
    const producer = this.#producer;
    if (!producer) {
      throw new Error("Producer identity is not initialized");
    }
    const response = await this.#txnCoordinatorRequest(
      API_END_TXN,
      1,
      writeEndTransactionRequest(transactionalId, producer.id, producer.epoch, committed),
    );
    const result = readEndTransactionResponse(response);
    this.#cluster.throttle(API_END_TXN, result.throttleMs);
    if (result.error) {
      throw kafkaError(result.error, label);
    }
  }

  async #txnCoordinatorRequest(
    apiKey: number,
    apiVersion: number,
    body: RequestBody,
  ): Promise<ResponseBody> {
    if (this.#transactionalId) {
      if (this.#coordinator === undefined) {
        this.#coordinator = await this.#cluster.findTxnCoordinator(this.#transactionalId);
      }
      return this.#cluster.request(this.#coordinator, apiKey, apiVersion, body);
    }
    return this.#cluster.anyRequest(apiKey, apiVersion, body);
  }

  #requireOpen(message: string): string {
    if (!this.#transactionalId || !this.#open) {
      throw new Error(message);
    }
    return this.#transactionalId;
  }
}
