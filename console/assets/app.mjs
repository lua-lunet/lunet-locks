// Lock Admin bootstrap: registers the web components, then runs the polling
// loops that keep the store fresh from /api/v1.

import "./components/la-tree.mjs";
import "./components/la-lock-table.mjs";
import "./components/la-detail.mjs";
import "./components/la-break-dialog.mjs";
import "./components/la-charts.mjs";
import "./components/la-log-view.mjs";
import "./components/la-app.mjs";

import { store, config } from "./lib/state.mjs";
import { api } from "./lib/api.mjs";
import { db } from "./lib/db.mjs";
import { parseClock } from "./lib/util.mjs";

/** @typedef {import("./lib/types.mjs").Bucket} Bucket */
/** @typedef {import("./lib/types.mjs").HttpError} HttpError */
/** @typedef {import("./lib/types.mjs").LocksParams} LocksParams */

// Failure bookkeeping: any failing poller surfaces on the status bar, and
// the message clears once every poller succeeds again.
/** @type {Set<string>} */
const failures = new Set();
/**
 * @param {string} name Poller name, shown in the status bar.
 * @param {unknown} err Falsy clears the poller's failure flag.
 * @returns {void}
 */
function report(name, err) {
  if (err) failures.add(name); else failures.delete(name);
  store.set({ error: failures.size ? `api unreachable: ${[...failures].join(", ")}` : "" });
}

/** @returns {Promise<void>} */
async function refreshCluster() {
  try {
    const cluster = await api.cluster();
    store.set({ cluster });
    report("cluster", null);
  } catch (e) { report("cluster", e); }
}

/** @returns {Promise<void>} */
async function refreshLocksAll() {
  try {
    const r = await api.locks({});
    store.set({ locksAll: r.locks });
    report("locks", null);
  } catch (e) { report("locks", e); }
}

/** @returns {Promise<void>} */
async function refreshLocks() {
  const st = store.state;
  /** @type {LocksParams} */
  const params = { q: st.query };
  if (st.mode === "expiry") {
    const at = parseClock(st.atText, Date.now());
    if (at != null) {
      params.expiringAtMs = at;
      params.toleranceMs = st.tolSec * 1000;
    }
  }
  try {
    const r = await api.locks(params);
    store.set({ locks: r.locks, serverNowMs: r.nowMs });
    report("locks", null);
  } catch (e) { report("locks", e); }
}

/** @returns {Promise<void>} */
async function refreshDetail() {
  const id = store.state.selectedId;
  if (id === null) return;
  try {
    const detail = await api.lock(id);
    store.set({ detail });
  } catch (e) {
    // Only a genuine 404 drops the selection; transient failures keep it.
    if (/** @type {HttpError} */ (e).status === 404) store.set({ selectedId: null, detail: null });
  }
}

/** @returns {Promise<void>} */
async function refreshEvents() {
  const st = store.state;
  if (st.mode !== "log") { report("events", null); return; }
  const fromMs = parseClock(st.fromText, Date.now());
  const toMs = parseClock(st.toText, Date.now());
  try {
    const r = await api.events({ fromMs, toMs, q: st.query, limit: 300 });
    store.set({ events: r.events });
    db.cacheEvents(r.events).catch(() => {});
    report("events", null);
  } catch (e) { report("events", e); }
}

/** @returns {Promise<void>} */
async function refreshSeries() {
  const st = store.state;
  if (st.mode !== "telemetry") { report("series", null); return; }
  const fromMs = Date.now() - config.historyWindowMs;
  try {
    const r = await api.series({ fromMs, toMs: Date.now(), bucketMs: config.telemetryBucketMs });
    db.cacheBuckets(r.buckets).catch(() => {});
    // Cached history fills any gap before the mock's own memory.
    /** @type {Bucket[]} */
    const cached = await db.readBuckets(fromMs).catch(() => []);
    /** @type {Map<number, Bucket>} */
    const merged = new Map(cached.map((b) => [b.tsMs, b]));
    for (const b of r.buckets) merged.set(b.tsMs, b);
    const buckets = [...merged.values()].sort((a, b) => a.tsMs - b.tsMs);
    store.set({ series: { bucketMs: r.bucketMs, buckets } });
    report("series", null);
  } catch (e) { report("series", e); }
}

/** @returns {Promise<void>} */
async function cacheTail() {
  // Keep the local append-only mirror warm regardless of the current view.
  const r = await api.events({ fromMs: Date.now() - 15 * 60e3, limit: 1000 }).catch(() => null);
  if (r) await db.cacheEvents(r.events).catch(() => {});
  await db.prune(Date.now() - 4 * config.historyWindowMs).catch(() => {});
}

window.addEventListener("la:refresh", () => {
  refreshLocks();
  refreshLocksAll();
  refreshDetail();
});

// In-flight guards: a slow tick skips the next one rather than piling up
// requests that could resolve out of order.
/** @type {Set<string>} */
const pending = new Set();
/**
 * @param {string} name
 * @param {() => Promise<void>} fn
 * @returns {void}
 */
function guard(name, fn) {
  if (pending.has(name)) return;
  pending.add(name);
  Promise.resolve(fn()).finally(() => pending.delete(name));
}

const fast = () => {
  guard("cluster", refreshCluster);
  guard("locksAll", refreshLocksAll);
  guard("locks", refreshLocks);
  guard("detail", refreshDetail);
};
fast();
setInterval(fast, config.refreshMs);
setInterval(() => store.set({ now: Date.now() }), 1000);
setInterval(() => guard("events", refreshEvents), 2000);
setInterval(() => guard("series", refreshSeries), 2000);
setInterval(() => guard("cacheTail", cacheTail), 15000);
guard("cacheTail", cacheTail);
