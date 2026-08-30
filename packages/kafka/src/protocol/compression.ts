import { lz4Compress, lz4Decompress } from "./lz4.ts";
import { snappyCompress, snappyDecompress } from "./snappy.ts";

export type RecordCompression = "none" | "gzip" | "snappy" | "lz4" | "zstd";

export interface CompressionStrategy {
  readonly code: number;
  compress(records: Uint8Array): Uint8Array<ArrayBuffer>;
  decompress(records: Uint8Array): Uint8Array<ArrayBuffer>;
}

function ownedBytes(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
  const copy = new Uint8Array(bytes.byteLength);
  copy.set(bytes);
  return copy;
}

export class NoneCompressionStrategy implements CompressionStrategy {
  readonly code = 0;

  compress(records: Uint8Array): Uint8Array<ArrayBuffer> {
    return ownedBytes(records);
  }

  decompress(records: Uint8Array): Uint8Array<ArrayBuffer> {
    return ownedBytes(records);
  }
}

export class GzipCompressionStrategy implements CompressionStrategy {
  readonly code = 1;

  compress(records: Uint8Array): Uint8Array<ArrayBuffer> {
    return ownedBytes(Bun.gzipSync(ownedBytes(records)));
  }

  decompress(records: Uint8Array): Uint8Array<ArrayBuffer> {
    return ownedBytes(Bun.gunzipSync(ownedBytes(records)));
  }
}

export class SnappyCompressionStrategy implements CompressionStrategy {
  readonly code = 2;

  compress(records: Uint8Array): Uint8Array<ArrayBuffer> {
    return ownedBytes(snappyCompress(records));
  }

  decompress(records: Uint8Array): Uint8Array<ArrayBuffer> {
    return ownedBytes(snappyDecompress(records));
  }
}

export class Lz4CompressionStrategy implements CompressionStrategy {
  readonly code = 3;

  compress(records: Uint8Array): Uint8Array<ArrayBuffer> {
    return ownedBytes(lz4Compress(records));
  }

  decompress(records: Uint8Array): Uint8Array<ArrayBuffer> {
    return ownedBytes(lz4Decompress(records));
  }
}

export class ZstdCompressionStrategy implements CompressionStrategy {
  readonly code = 4;

  compress(records: Uint8Array): Uint8Array<ArrayBuffer> {
    return ownedBytes(Bun.zstdCompressSync(ownedBytes(records)));
  }

  decompress(records: Uint8Array): Uint8Array<ArrayBuffer> {
    return ownedBytes(Bun.zstdDecompressSync(ownedBytes(records)));
  }
}

const compressionStrategies = {
  none: new NoneCompressionStrategy(),
  gzip: new GzipCompressionStrategy(),
  snappy: new SnappyCompressionStrategy(),
  lz4: new Lz4CompressionStrategy(),
  zstd: new ZstdCompressionStrategy(),
} satisfies Record<RecordCompression, CompressionStrategy>;

const compressionStrategiesByCode = new Map<number, CompressionStrategy>(
  Object.values(compressionStrategies).map((strategy) => [strategy.code, strategy] as const),
);

export function getCompressionStrategy(compression: RecordCompression): CompressionStrategy {
  if (!Object.hasOwn(compressionStrategies, compression)) {
    throw new RangeError(`Unsupported Kafka compression: ${compression}`);
  }
  return compressionStrategies[compression];
}

export function getCompressionStrategyByCode(code: number): CompressionStrategy | undefined {
  return compressionStrategiesByCode.get(code);
}
