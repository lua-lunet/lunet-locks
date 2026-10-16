// The journal data layer's tests: the LKE1 parser and the merge engine's
// break arm. Node's built-in test runner; the browser globals the modules
// read at import time are shimmed before the dynamic imports run.

import { test } from "node:test";
import assert from "node:assert/strict";

// ---- the browser-global shims (state.mjs reads these at module scope) ----

globalThis.document = {
  getElementById: (id) =>
    id === "la-config"
      ? { textContent: JSON.stringify({ dataSource: "mock", expiryDefaultOffsetMs: 0 }) }
      : { textContent: "{}" },
};
globalThis.location = { search: "" };
globalThis.sessionStorage = {
  getItem: () => null,
  setItem: () => {},
};

const { parseRecords, crc32 } = await import("../assets/lib/journal/parse.mjs");
const { ingestFileEvents } = await import("../assets/lib/journal/merge.mjs");
const { store } = await import("../assets/lib/state.mjs");

// ---- the LKE1 encoder (mirrors journal.rs's record layout) ----

/**
 * Encode one 61-byte LKE1 record.
 * @param {number} kind 1 hold, 2 renew, 3 release, 4 break
 * @param {number} ts
 * @param {number} lockId
 * @param {number} leaseId
 * @param {string} holderHex 32 hex chars
 * @param {number} expiry
 * @returns {ArrayBuffer}
 */
function encodeRecord(kind, ts, lockId, leaseId, holderHex, expiry) {
  const buf = new ArrayBuffer(61);
  const view = new DataView(buf);
  const u8 = new Uint8Array(buf);
  u8.set([0x4c, 0x4b, 0x45, 0x31], 0); // "LKE1"
  view.setUint32(4, 53, false); // payload length
  u8[8] = kind;
  view.setBigUint64(9, BigInt(ts), false);
  view.setBigUint64(17, BigInt(lockId), false);
  view.setBigUint64(25, BigInt(leaseId), false);
  for (let i = 0; i < 16; i++) {
    u8[33 + i] = parseInt(holderHex.slice(i * 2, i * 2 + 2), 16);
  }
  view.setBigUint64(49, BigInt(expiry), false);
  view.setUint32(57, crc32(u8, 8, 57), false);
  return buf;
}

const HOLDER = "11111111111111111111111111111111";

function joinBuffers(parts) {
  const total = parts.reduce((n, b) => n + b.byteLength, 0);
  const out = new Uint8Array(total);
  let at = 0;
  for (const part of parts) {
    out.set(new Uint8Array(part), at);
    at += part.byteLength;
  }
  return out.buffer;
}

// ---- the parser: a break record mid-stream parses whole, never halts ----

test("parseRecords: a break record mid-stream parses with the records after it", () => {
  const file = joinBuffers([
    encodeRecord(1, 1000, 9001, 5, HOLDER, 99_000),
    encodeRecord(4, 2000, 9001, 5, HOLDER, 99_000), // the sweeper's break
    encodeRecord(1, 3000, 9002, 6, HOLDER, 99_000),
  ]);
  const events = parseRecords(file);
  assert.equal(events.length, 3, "all three records parse across the break");
  assert.equal(events[0].kind, "hold");
  assert.equal(events[1].kind, "break");
  assert.equal(events[2].kind, "hold");
});

test("parseRecords: an unknown kind still halts (corrupt-tail tolerance stands)", () => {
  const file = joinBuffers([
    encodeRecord(1, 1000, 9001, 5, HOLDER, 99_000),
    encodeRecord(9, 2000, 9001, 5, HOLDER, 99_000), // not a kind
    encodeRecord(1, 3000, 9002, 6, HOLDER, 99_000),
  ]);
  const events = parseRecords(file);
  assert.equal(events.length, 1, "parsing stops at the first unknown kind");
});

// ---- the merge engine: the break arm ----

const future = Date.now() + 3_600_000;

async function ingest(events) {
  await ingestFileEvents(`test-${Math.random()}`, events);
}

function activeLock(lockId) {
  return store.state.journalLocks.find((lock) => lock.lockId === lockId);
}

test("merge: a break removes the active lock it broke", async () => {
  await ingest([
    { kind: "hold", ts: 1000, lockId: 9101, leaseId: 5, holder: HOLDER, expiry: future },
    { kind: "break", ts: 2000, lockId: 9101, leaseId: 5, holder: HOLDER, expiry: future },
  ]);
  assert.equal(activeLock(9101), undefined, "the broken lock is gone");
});

test("merge: a break ahead of its hold tomstones it — the lock never resurrects", async () => {
  await ingest([
    { kind: "break", ts: 1000, lockId: 9102, leaseId: 5, holder: HOLDER, expiry: future },
    { kind: "hold", ts: 2000, lockId: 9102, leaseId: 5, holder: HOLDER, expiry: future },
  ]);
  assert.equal(activeLock(9102), undefined, "the tombstone-ahead break holds");
});

test("merge: an older break does not retire a newer generation", async () => {
  await ingest([
    { kind: "hold", ts: 1000, lockId: 9103, leaseId: 6, holder: HOLDER, expiry: future },
    { kind: "break", ts: 2000, lockId: 9103, leaseId: 5, holder: HOLDER, expiry: future },
  ]);
  const lock = activeLock(9103);
  assert.ok(lock, "the newer generation stands");
  assert.equal(lock.leaseId, 6, "the incumbent lease survives the stale break");
});
