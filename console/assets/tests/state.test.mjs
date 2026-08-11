// Store behaviour and session persistence, including the item43 trailing
// log range. Cases that touch the real store snapshot the session blob and
// the fields they mutate, restoring both in a finally — a run leaves the
// "lock-admin" blob exactly as found, so two runs give identical results.

import { parseClock } from "../lib/util.mjs";
import { store } from "../lib/state.mjs";
import { AssertionError, assertClose, assertEqual, preserveSession } from "./assert.mjs";
import { preserveStore } from "./fixtures.mjs";

let freshN = 0;

/**
 * Load an isolated copy of the store module against a prepared session blob
 * (a cache-busting query makes each import a fresh module instance). The
 * blob is restored before returning; the fresh module has already read it.
 *
 * Between preparing the blob and the fresh module's first line, the app's
 * 1s `now` tick could persist and clobber it — so setItem("lock-admin") is
 * blocked for the duration of the dynamic import. One dropped UI-pref
 * persist is harmless; the next set() rewrites it.
 * @param {Record<string, unknown> | null} blob null removes the key entirely
 * @returns {Promise<typeof import("../lib/state.mjs")>}
 */
async function freshState(blob) {
  const restoreSession = preserveSession("lock-admin");
  const realSetItem = sessionStorage.setItem.bind(sessionStorage);
  try {
    if (blob === null) sessionStorage.removeItem("lock-admin");
    else realSetItem("lock-admin", JSON.stringify(blob));
    sessionStorage.setItem = (k, v) => { if (k !== "lock-admin") realSetItem(k, v); };
    const p = "../lib/state.mjs?fresh=" + ++freshN;
    return /** @type {typeof import("../lib/state.mjs")} */ (await import(p));
  } finally {
    sessionStorage.setItem = realSetItem;
    restoreSession();
  }
}

/** @type {import("./assert.mjs").Suite} */
export const suite = {
  name: "state",
  cases: {
    "set merges the patch and notifies subscribers"() {
      const restoreSession = preserveSession("lock-admin");
      const restoreStore = preserveStore("query");
      try {
        /** @type {string | null} */
        let seen = null;
        const off = store.subscribe((s) => { seen = s.query; });
        store.set({ query: "marker-a" });
        off();
        assertEqual(seen, "marker-a");
        assertEqual(store.state.query, "marker-a");
      } finally {
        restoreStore();
        restoreSession();
      }
    },
    "unsubscribe stops notifications"() {
      const restoreSession = preserveSession("lock-admin");
      const restoreStore = preserveStore("query");
      try {
        let calls = 0;
        const off = store.subscribe(() => { calls++; });
        store.set({ query: "marker-b" });
        off();
        store.set({ query: "marker-c" });
        assertEqual(calls, 1, "listener fired after unsubscribe");
      } finally {
        restoreStore();
        restoreSession();
      }
    },
    "persistence round-trip writes the saved blob"() {
      const restoreSession = preserveSession("lock-admin");
      const restoreStore = preserveStore("query", "logRangePinned");
      try {
        store.set({ query: "rt-marker", logRangePinned: true });
        const blob = JSON.parse(sessionStorage.getItem("lock-admin") ?? "{}");
        assertEqual(blob.query, "rt-marker");
        assertEqual(blob.logRangePinned, true);
        assertEqual(Array.isArray(blob.watched), true, "sets persist as arrays");
        assertEqual("colWidths" in blob, true, "nullable width keys persist");
      } finally {
        restoreStore();
        restoreSession();
      }
    },
    async "a fresh load tolerates unknown saved keys"() {
      const mod = await freshState({ mode: "log", watched: [5], futureField: { x: 1 } });
      assertEqual(mod.store.state.mode, "log");
      assertEqual(mod.store.state.watched.has(5), true, "saved Set members restored");
      assertEqual(mod.store.state.tolSec, mod.config.defaultToleranceSec, "missing keys fall back to config");
    },
    async "a fresh load restores a pinned log range verbatim"() {
      const mod = await freshState({ logRangePinned: true, fromText: "03:04:05", toText: "04:05:06" });
      assertEqual(mod.store.state.fromText, "03:04:05");
      assertEqual(mod.store.state.toText, "04:05:06");
    },
    async "a fresh load ignores an unpinned saved range (trailing window)"() {
      // item43: a persisted from/to with logRangePinned false is a frozen
      // instant from an older session; the store must seed from the clock.
      const t0 = Date.now();
      const mod = await freshState({ logRangePinned: false, fromText: "03:04:05", toText: "04:05:06" });
      const fromMs = parseClock(mod.store.state.fromText, t0);
      const toMs = parseClock(mod.store.state.toText, t0);
      if (fromMs === null || toMs === null) {
        throw new AssertionError("seeded range did not parse",
          { fromText: mod.store.state.fromText, toText: mod.store.state.toText }, "HH:MM:SS");
      }
      assertClose(fromMs, t0 - mod.config.logDefaultWindowMs, 5000, "trailing window start tracks the clock");
      assertClose(toMs, t0, 5000, "trailing window end tracks the clock");
    },
  },
};
