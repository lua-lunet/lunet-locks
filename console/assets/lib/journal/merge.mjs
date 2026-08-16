// Single-threaded journal merge engine. Two sources feed it:
//   - Worker fileEvents (historical rolled files)
//   - WS event messages (live tail)
//
// Maintains:
//   - Active-lock set: last known lease per lockId
//   - Per-second rate buckets for the telemetry chart
//   - Recent events list (newest first)
//   - Tombstone-ahead rule: a release for an unseen acquisition is stored as
//     a pending tombstone keyed by (lockId, leaseId) and applied when that
//     acquisition arrives from a file pull.
//
// After every batch, writes merged events to IndexedDB and updates the store.

import { store } from "../state.mjs";
import { db } from "../db.mjs";

const SESSION_KEY = "lock-admin-live";
const MAX_RECENT_EVENTS = 500;
const RATE_WINDOW_SEC = 300; // keep 5 minutes of rate buckets

// ---- internal state ----

/** Map<lockId, {leaseId, holder, expiry, acquiredTs, renewCount}> */
const activeLocks = new Map();

/**
 * Pending tombstones: releases that arrived before their acquisition.
 * Map<"lockId:leaseId", {ts, expiry}>
 */
const pendingTombstones = new Map();

/** Rate buckets: Map<tsSec, {acquire, renew, release}> */
const rateBuckets = new Map();

/** Recent events ring buffer (newest first). */
let recentEvents = [];

/** Status counters. */
let filesTotal = 0;
let filesLoaded = 0;
let wsConnected = false;

// ---- helpers ----

function tombstoneKey(lockId, leaseId) {
  return `${lockId}:${leaseId}`;
}

function bumpRate(tsMs, kind) {
  const sec = Math.floor(tsMs / 1000);
  let b = rateBuckets.get(sec);
  if (!b) {
    b = { tsSec: sec, acquire: 0, renew: 0, release: 0 };
    rateBuckets.set(sec, b);
  }
  if (kind === "hold") b.acquire++;
  else if (kind === "renew") b.renew++;
  else if (kind === "release") b.release++;
}

function pruneRates() {
  const cutoff = Math.floor(Date.now() / 1000) - RATE_WINDOW_SEC;
  for (const [sec] of rateBuckets) {
    if (sec < cutoff) rateBuckets.delete(sec);
  }
}

function addRecentEvent(ev) {
  recentEvents.unshift(ev);
  if (recentEvents.length > MAX_RECENT_EVENTS) {
    recentEvents.length = MAX_RECENT_EVENTS;
  }
}

/**
 * Apply a single event to the active-lock set and tombstone map.
 * Implements the tombstone-ahead rule.
 */
function applyEvent(ev) {
  const { kind, ts, lockId, leaseId, holder, expiry } = ev;

  bumpRate(ts, kind);
  addRecentEvent(ev);

  if (kind === "hold") {
    // Check if there is a pending tombstone for this exact (lockId, leaseId).
    const tk = tombstoneKey(lockId, leaseId);
    if (pendingTombstones.has(tk)) {
      // The release already arrived: do NOT add to active set; remove pending.
      pendingTombstones.delete(tk);
      // If this lockId was in activeLocks with this leaseId, remove it.
      const cur = activeLocks.get(lockId);
      if (cur && cur.leaseId === leaseId) {
        activeLocks.delete(lockId);
      }
    } else {
      // Normal acquisition. Also clears any older pending tombstones for this
      // lockId with a lower leaseId (they can never match now).
      for (const [key] of pendingTombstones) {
        if (key.startsWith(`${lockId}:`)) {
          const lid = Number(key.split(":")[1]);
          if (lid < leaseId) pendingTombstones.delete(key);
        }
      }
      activeLocks.set(lockId, {
        leaseId,
        holder,
        expiry,
        acquiredTs: ts,
        renewCount: 0,
      });
    }
  } else if (kind === "renew") {
    const cur = activeLocks.get(lockId);
    if (cur && cur.leaseId === leaseId) {
      cur.expiry = expiry;
      cur.renewCount++;
    }
    // A renew for an unseen lease: check pending tombstone.
    const tk = tombstoneKey(lockId, leaseId);
    if (pendingTombstones.has(tk)) {
      pendingTombstones.delete(tk);
      if (cur && cur.leaseId === leaseId) {
        activeLocks.delete(lockId);
      }
    }
  } else if (kind === "release") {
    const cur = activeLocks.get(lockId);
    if (cur && cur.leaseId === leaseId) {
      // Acquisition was seen: apply tombstone immediately.
      activeLocks.delete(lockId);
    } else {
      // TOMBSTONE-AHEAD: release arrived before its acquisition. Store as
      // pending so it gets applied when the acquisition lands from a file pull.
      pendingTombstones.set(tombstoneKey(lockId, leaseId), { ts, expiry });
    }
  }
}

function flushToStore() {
  pruneRates();

  // Build sorted rate bucket array.
  const buckets = [...rateBuckets.values()].sort((a, b) => a.tsSec - b.tsSec);
  const bucketSec = buckets.length ? buckets[0].tsSec : Math.floor(Date.now() / 1000);

  // Build active locks array, filtering expired.
  const now = Date.now();
  const locks = [];
  for (const [lockId, info] of activeLocks) {
    if (info.expiry > now) {
      locks.push({ lockId, ...info });
    } else {
      activeLocks.delete(lockId);
    }
  }
  locks.sort((a, b) => a.lockId - b.lockId);

  const caughtUp = filesTotal > 0 && filesLoaded >= filesTotal && wsConnected;

  store.set({
    journalLocks: locks,
    journalRates: { bucketSec, buckets },
    journalEvents: recentEvents,
    journalStatus: { filesTotal, filesLoaded, wsConnected, caughtUp },
  });
}

// ---- session storage replay ----

function loadSessionBuffer() {
  try {
    const raw = sessionStorage.getItem(SESSION_KEY);
    if (!raw) return;
    const buf = JSON.parse(raw);
    if (!Array.isArray(buf)) return;
    // Replay through the same apply logic (tombstone-aware).
    for (const ev of buf) {
      applyEvent(ev);
    }
  } catch (_) {}
}

function saveSessionBuffer(events) {
  try {
    // Keep only the latest events in session storage (bounded).
    const existing = (() => {
      try {
        const r = sessionStorage.getItem(SESSION_KEY);
        return r ? JSON.parse(r) : [];
      } catch (_) { return []; }
    })();
    const merged = [...existing, ...events].slice(-MAX_RECENT_EVENTS);
    sessionStorage.setItem(SESSION_KEY, JSON.stringify(merged));
  } catch (_) {}
}

// ---- public API ----

/**
 * Initialize the merge engine: replay session buffer, set up DB catch-up.
 * Returns handlers for worker and WS integration.
 */
export async function initMerge() {
  loadSessionBuffer();

  // Load previously loaded files from IndexedDB so the worker skips them.
  try {
    const names = await db.getLoadedFiles();
    return { loadedNames: names };
  } catch (_) {
    return { loadedNames: [] };
  }
}

/**
 * Ingest a batch of events from a pulled file. Writes to IndexedDB and
 * updates the store.
 */
export async function ingestFileEvents(name, events) {
  if (!events.length) return;

  for (const ev of events) {
    applyEvent(ev);
  }

  // Persist to IndexedDB.
  try {
    await db.putJournalEvents(events);
    await db.markFileLoaded(name);
  } catch (_) {}

  flushToStore();
}

/**
 * Ingest a single live event from the WebSocket. Mirrors to session storage.
 */
export function ingestLiveEvent(ev) {
  applyEvent(ev);
  saveSessionBuffer([ev]);
  flushToStore();
}

/** Update file counts from the worker. */
export function updateFilesStatus(total, loaded) {
  filesTotal = total;
  filesLoaded = loaded;
  flushToStore();
}

/** Update WS connection status. */
export function updateWsStatus(connected) {
  wsConnected = connected;
  flushToStore();
}
