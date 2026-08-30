export type CompatValue =
  | string
  | number
  | boolean
  | bigint
  | Uint8Array
  | null
  | undefined
  | CompatValue[]
  | { readonly [key: string]: CompatValue }
  | ((...args: never[]) => CompatValue);

export type CompatOptions = { readonly [key: string]: CompatValue };

export type LogValue =
  | string
  | number
  | boolean
  | bigint
  | null
  | undefined
  | Error
  | LogValue[]
  | { readonly [key: string]: LogValue };

export type LogFields = { readonly [key: string]: LogValue };
