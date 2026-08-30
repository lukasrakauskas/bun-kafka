import { requiredValue } from "../../type-guards.ts";

const CRC32C_POLY = 0x82f6_3b78;
const CRC32C_INIT = 0xffff_ffff;
const BYTE_MASK = 0xff;
const BITS_PER_BYTE = 8;
const CRC_TABLE_SIZE = 256;

function createCrcTable(): Uint32Array {
  const table = new Uint32Array(CRC_TABLE_SIZE);
  for (let i = 0; i < table.length; i++) {
    let value = i;
    for (let bit = 0; bit < BITS_PER_BYTE; bit++) {
      value = value & 1 ? CRC32C_POLY ^ (value >>> 1) : value >>> 1;
    }
    table[i] = value >>> 0;
  }
  return table;
}

let crcTable: Uint32Array | undefined;

export function crc32c(bytes: Uint8Array): number {
  if (!crcTable) {
    crcTable = createCrcTable();
  }
  const table = requiredValue(crcTable);
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.byteLength; i++) {
    crc =
      requiredValue(table[(crc ^ requiredValue(bytes[i])) & BYTE_MASK]) ^ (crc >>> BITS_PER_BYTE);
  }
  return (crc ^ CRC32C_INIT) >>> 0;
}
