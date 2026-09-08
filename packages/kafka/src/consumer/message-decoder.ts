import type { ConsumedMessage } from "../types.ts";
import { requiredValue } from "../type-guards.ts";
import type { RecordSetDecoder } from "../protocol/index.ts";
import { partitionKey } from "../bun/shared.ts";
import type { ConsumerOptions, DeserializerContext } from "./types.ts";

export class MessageDecoder<K, V> {
  constructor(
    private readonly options: ConsumerOptions<K, V>,
    private readonly positions: Map<string, bigint>,
  ) {}
  #decoders: RecordSetDecoder[] = [];

  add(decoders: RecordSetDecoder[]): void {
    this.#decoders.push(...decoders);
  }
  clear(): void {
    this.#decoders = [];
  }
  clearPartition(key: string): void {
    this.#decoders = this.#decoders.filter(
      (decoder) => partitionKey(decoder.topic, decoder.partition) !== key,
    );
  }
  drain(max: number): Array<ConsumedMessage<K, V>> {
    let messages: Array<ConsumedMessage<K, V>> = [];
    while (this.#decoders.length && messages.length < max) {
      const decoder = requiredValue(this.#decoders[0]);
      const decoded = this.convertBatch(decoder.read(max - messages.length));
      if (!messages.length) {
        messages = decoded;
      } else {
        for (const message of decoded) {
          messages.push(message);
        }
      }
      if (decoder.done) {
        this.#decoders.shift();
      }
    }
    return messages;
  }
  get pending(): boolean {
    return this.#decoders.length > 0;
  }
  private convertBatch(messages: ConsumedMessage[]): Array<ConsumedMessage<K, V>> {
    if (this.options.keyDeserializer || this.options.valueDeserializer) {
      return messages.map((message) => this.convert(message));
    }
    const last = messages.at(-1);
    if (last) {
      this.positions.set(partitionKey(last.topic, last.partition), last.offset + 1n);
    }
    return messages as Array<ConsumedMessage<K, V>>;
  }
  private convert(message: ConsumedMessage): ConsumedMessage<K, V> {
    this.positions.set(partitionKey(message.topic, message.partition), message.offset + 1n);
    if (!this.options.keyDeserializer && !this.options.valueDeserializer) {
      return message as ConsumedMessage<K, V>;
    }
    const context: DeserializerContext = {
      topic: message.topic,
      partition: message.partition,
      offset: message.offset,
      timestamp: message.timestamp,
    };
    return {
      ...message,
      key: this.options.keyDeserializer?.(message.key, context) ?? null,
      value: this.options.valueDeserializer?.(message.value, context) ?? null,
    } as ConsumedMessage<K, V>;
  }
}
