/* oxlint-disable no-magic-numbers -- CRC slicing table offsets and bit shifts */
import { requiredValue } from "../../type-guards.ts";

const CRC32C_POLY = 0x82f6_3b78;
const CRC32C_INIT = 0xffff_ffff;
const BYTE_MASK = 0xff;
const BITS_PER_BYTE = 8;
const CRC_TABLE_SIZE = 256;

function createCrcTable(): Uint32Array {
  const table = new Uint32Array(CRC_TABLE_SIZE * 4);
  for (let i = 0; i < CRC_TABLE_SIZE; i++) {
    let value = i;
    for (let bit = 0; bit < BITS_PER_BYTE; bit++) {
      value = value & 1 ? CRC32C_POLY ^ (value >>> 1) : value >>> 1;
    }
    table[i] = value >>> 0;
  }
  // Each additional table advances a byte's contribution by another zero byte.
  for (let i = CRC_TABLE_SIZE; i < table.length; i++) {
    const value = requiredValue(table[i - CRC_TABLE_SIZE]);
    table[i] = requiredValue(table[value & BYTE_MASK]) ^ (value >>> BITS_PER_BYTE);
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
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let i = 0;
  // Slicing-by-four breaks the serial dependency between four byte-wise updates.
  for (; i + 4 <= bytes.byteLength; i += 4) {
    crc ^= view.getUint32(i, true);
    crc =
      requiredValue(table[3 * CRC_TABLE_SIZE + (crc & BYTE_MASK)]) ^
      requiredValue(table[2 * CRC_TABLE_SIZE + ((crc >>> 8) & BYTE_MASK)]) ^
      requiredValue(table[CRC_TABLE_SIZE + ((crc >>> 16) & BYTE_MASK)]) ^
      requiredValue(table[crc >>> 24]);
  }
  for (; i < bytes.byteLength; i++) {
    crc =
      requiredValue(table[(crc ^ requiredValue(bytes[i])) & BYTE_MASK]) ^ (crc >>> BITS_PER_BYTE);
  }
  return (crc ^ CRC32C_INIT) >>> 0;
}
