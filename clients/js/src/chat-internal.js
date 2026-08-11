// Package-private capabilities used by CitadelClient to drive one cursor's
// reconciliation lifecycle. This module is intentionally not package-exported.
// Privileged methods live in this module-scoped WeakMap rather than as
// discoverable Symbol properties on the public cursor prototype.

const cursorCapabilities = new WeakMap();

export function registerChatCursor(cursor, capabilities) {
  if (cursorCapabilities.has(cursor)) throw new Error("chat cursor is already registered");
  cursorCapabilities.set(cursor, Object.freeze({ ...capabilities }));
}

function capability(cursor, name) {
  const capabilities = cursorCapabilities.get(cursor);
  if (!capabilities || typeof capabilities[name] !== "function") {
    throw new TypeError("expected a registered ChatEventCursor");
  }
  return capabilities[name];
}

export function beginChatHistory(cursor, limit) {
  return capability(cursor, "beginHistory")(limit);
}

export function acceptChatHistory(cursor, handle, response) {
  return capability(cursor, "acceptHistory")(handle, response);
}

export function completeChatHistoryApply(cursor, handle) {
  return capability(cursor, "completeHistoryApply")(handle);
}

export function abortChatHistoryApply(cursor, handle) {
  return capability(cursor, "abortHistoryApply")(handle);
}

export function beginChatAck(cursor) {
  return capability(cursor, "beginAck")();
}

export function acceptChatAck(cursor, handle, response) {
  return capability(cursor, "acceptAck")(handle, response);
}

export function abortChatRequest(cursor, handle) {
  return capability(cursor, "abortRequest")(handle);
}
