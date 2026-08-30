import { RequestBody, ResponseBody } from "../body.ts";
import { decodeResponse, encodeRequest, type KafkaDecoder } from "../codec.ts";

type Token = {
  principalType: string;
  principalName: string;
  tokenRequester: string;
  issueTimestampMs: bigint;
  expiryTimestampMs: bigint;
  maxTimestampMs: bigint;
  tokenId: string;
  hmac: Uint8Array | null;
};
function token(r: KafkaDecoder): Token {
  const v = {
    principalType: r.compactString() ?? "",
    principalName: r.compactString() ?? "",
    tokenRequester: r.compactString() ?? "",
    issueTimestampMs: r.i64(),
    expiryTimestampMs: r.i64(),
    maxTimestampMs: r.i64(),
    tokenId: r.compactString() ?? "",
    hmac: r.compactBytes(),
  };
  r.skipTags();
  return v;
}
export function writeCreateDelegationTokenRequest(
  a: string | null,
  b: string | null,
  p: number | bigint,
): RequestBody {
  return encodeRequest((w) => w.compactString(a).compactString(b).i64(BigInt(p)).tags());
}
export function readCreateDelegationTokenResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    error = r.i16(),
    message = r.compactString();
  return { throttleMs, error, message, token: token(r) };
}
export function writeDescribeDelegationTokenRequest(
  o: readonly { principalType: string; principalName: string }[],
): RequestBody {
  return encodeRequest((w) =>
    w
      .compactArray(o.length ? o : null, (x, v) =>
        x.compactString(v.principalType).compactString(v.principalName).tags(),
      )
      .tags(),
  );
}
export function readDescribeDelegationTokenResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    error = r.i16(),
    message = r.compactString();
  return { throttleMs, error, message, tokens: r.compactArray(token) };
}
export function writeTokenPeriodRequest(h: Uint8Array, p: number | bigint): RequestBody {
  return encodeRequest((w) => w.compactBytes(h).i64(BigInt(p)).tags());
}
export function readTokenPeriodResponse(body: ResponseBody) {
  const r = decodeResponse(body),
    throttleMs = r.i32(),
    error = r.i16(),
    message = r.compactString();
  return { throttleMs, error, message, expiryTimestampMs: r.i64() };
}
