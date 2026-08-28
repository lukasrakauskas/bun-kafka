export function requiredValue<T>(value: T | undefined, context = "Expected value"): T {
  if (value === undefined) {
    throw new Error(context);
  }
  return value;
}

export function isString<T>(value: T): value is T & string {
  return typeof value === "string";
}

export function isFunction<T>(value: T): value is T & ((...args: never[]) => void) {
  return typeof value === "function";
}

export function isBigInt<T>(value: T): value is T & bigint {
  return typeof value === "bigint";
}

export function isNumber<T>(value: T): value is T & number {
  return typeof value === "number";
}

export function isBoolean<T>(value: T): value is T & boolean {
  return typeof value === "boolean";
}

export function isUint8Array<T>(value: T): value is T & Uint8Array {
  return value instanceof Uint8Array;
}

export function isArrayBufferBytes(value: Uint8Array): value is Uint8Array<ArrayBuffer> {
  return value.buffer instanceof ArrayBuffer;
}

export function arrayBufferBytes(value: Uint8Array): Uint8Array<ArrayBuffer> {
  return isArrayBufferBytes(value) ? value : new Uint8Array(value);
}

export function isObject<T>(value: T): value is T & object {
  return typeof value === "object" && value !== null;
}

export function hasStringValue<T>(value: T): value is T & { readonly value?: string } {
  return isObject(value) && "value" in value;
}

export function hasStringName<T>(value: T): value is T & { readonly name: string } {
  return isObject(value) && "name" in value && isString(value.name);
}
