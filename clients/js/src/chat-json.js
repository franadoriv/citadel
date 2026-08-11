import { parse, stringify } from "lossless-json";

export const U64_MAX = 18_446_744_073_709_551_615n;

function parseChatNumber(source) {
  if (/^-?\d+$/.test(source)) {
    const integer = BigInt(source);
    if (integer >= Number.MIN_SAFE_INTEGER && integer <= Number.MAX_SAFE_INTEGER) {
      return Number(integer);
    }
    return integer;
  }
  return Number(source);
}

export function parseChatJsonText(text) {
  return parse(text, null, { parseNumber: parseChatNumber });
}

export function stringifyChatJson(value) {
  return stringify(value);
}

export function isU64(value, positive = false) {
  if (typeof value === "number") {
    return Number.isSafeInteger(value) && value >= (positive ? 1 : 0);
  }
  return typeof value === "bigint"
    && value >= (positive ? 1n : 0n)
    && value <= U64_MAX;
}

export function maxU64(left, right) {
  return left >= right ? left : right;
}

export function isNextU64(previous, next) {
  return BigInt(next) === BigInt(previous) + 1n;
}

export function equalU64(left, right) {
  return isU64(left) && isU64(right) && BigInt(left) === BigInt(right);
}
