export function isString<T>(value: T): value is T & string {
  return typeof value === "string";
}

export function isFunction<T>(value: T): value is T & ((...args: never[]) => void) {
  return typeof value === "function";
}

export function isBigInt<T>(value: T): value is T & bigint {
  return typeof value === "bigint";
}

export function isObject<T>(value: T): value is T & object {
  return typeof value === "object" && value !== null;
}

export function hasStringValue<T>(value: T): value is T & { readonly value?: string } {
  return isObject(value) && "value" in value;
}
