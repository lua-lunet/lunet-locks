// Central store: one mutable state object + pub/sub. UI prefs persist to
// sessionStorage so a refresh restores the console exactly as left.

import { fmtClock } from "./util.mjs";
import { logger } from "./log.mjs";

const log = logger("store");

/** @typedef {import("./types.mjs").Config} Config */
/** @typedef {import("./types.mjs").SavedState} SavedState */
/** @typedef {import("./types.mjs").StoreState} StoreState */

// The blob is inlined by index.html, so a missing #la-config is a deploy bug
// rather than a runtime condition: fail loudly instead of limping on with
// undefined tunables.
const configEl = document.getElementById("la-config");
if (!configEl) throw new Error("missing #la-config: index.html and app.mjs are out of sync");

/** @type {Config} */
export const config = JSON.parse(configEl.textContent ?? "{}");

const SAVED_KEY = "lock-admin";
/** @type {SavedState} */
const saved = JSON.parse(sessionStorage.getItem(SAVED_KEY) ?? "{}");

/**
 * Mirror the user-visible prefs (only those) into sessionStorage.
 * @param {StoreState} state
 * @returns {void}
 */
function persist(state) {
  sessionStorage.setItem(SAVED_KEY, JSON.stringify({
    mode: state.mode,
    query: state.query,
    tolSec: state.tolSec,
    atText: state.atText,
    fromText: state.fromText,
    toText: state.toText,
    logRangePinned: state.logRangePinned,
    watched: [...state.watched],
    collapsed: [...state.collapsed],
    colWidths: state.colWidths,
    logColWidths: state.logColWidths,
    paneWidths: state.paneWidths,
  }));
}

// Keys persist() writes. A patch that touches none of them (the 1s `now`
// tick, poll payloads like locks/events/cluster/series) skips the
// JSON.stringify + sessionStorage write entirely (item47).
/** @type {Set<keyof StoreState>} */
const PERSISTED_KEYS = new Set([
  "mode", "query", "tolSec", "atText", "fromText", "toText", "logRangePinned",
  "watched", "collapsed", "colWidths", "logColWidths", "paneWidths",
]);

export const store = {
  /** @type {StoreState} */
  state: {
    now: Date.now(),
    mode: saved.mode ?? "locks", // locks | expiry | telemetry | log
    query: saved.query ?? "",
    atText: saved.atText ?? fmtClock(Date.now() + config.expiryDefaultOffsetMs),
    tolSec: saved.tolSec ?? config.defaultToleranceSec,
    logRangePinned: saved.logRangePinned ?? false,
    // An unpinned range is a trailing window: any persisted from/to is a
    // frozen instant from an older session (the empty-Logs bug), so ignore
    // it and seed from the clock. Only a pinned range is restored verbatim.
    fromText: (saved.logRangePinned ? saved.fromText : null)
      ?? fmtClock(Date.now() - config.logDefaultWindowMs),
    toText: (saved.logRangePinned ? saved.toText : null)
      ?? fmtClock(Date.now()),
    cluster: null,
    locksAll: [],       // unfiltered — drives the path tree
    locks: [],          // filtered per current mode/search
    serverNowMs: Date.now(),
    selectedId: null,
    detail: null,       // {lock, recentEvents}
    watched: new Set(saved.watched ?? []),
    collapsed: new Set(saved.collapsed ?? ["/tenants", "/jobs", "/index"]),
    colWidths: saved.colWidths ?? null, // lock-table px widths; null = defaults
    logColWidths: saved.logColWidths ?? null, // log-view px widths; null = defaults
    paneWidths: saved.paneWidths ?? null, // shell [tree, detail] px widths; null = defaults
    confirmId: null,    // lock id pending a break confirmation
    events: [],         // log view rows
    series: null,       // {bucketMs, buckets}
    toast: "",          // transient status text
    error: "",
  },
  /** @type {Map<(state: StoreState) => void, Set<keyof StoreState> | null>} */
  _listeners: new Map(),
  /**
   * Merge a patch into the state, persist (only when a persisted key
   * changed), then notify the subscribers whose declared keys intersect the
   * patch. A `now`-only patch therefore reaches only the components that
   * subscribed to `now` (TTL countdowns) and never rebuilds a table body.
   * @param {Partial<StoreState>} patch
   * @returns {void}
   */
  set(patch) {
    Object.assign(this.state, patch);
    const changed = Object.keys(patch);
    // Key names only — the `now` tick fires 1/s and set() runs ~5×/s, so
    // dumping values here would flood the channel.
    log.finest(`set: ${changed.join(", ")}`, { keys: changed });
    if (changed.some((k) => PERSISTED_KEYS.has(/** @type {keyof StoreState} */ (k)))) {
      persist(this.state);
    }
    for (const [fn, keys] of this._listeners) {
      if (keys === null || changed.some((k) => keys.has(/** @type {keyof StoreState} */ (k)))) {
        fn(this.state);
      }
    }
  },
  /**
   * @param {(state: StoreState) => void} fn
   * @param {(keyof StoreState)[]} [keys] When given, fn fires only for patches
   *   touching one of these keys; omitted = every patch (legacy behaviour).
   * @returns {() => void} unsubscribe
   */
  subscribe(fn, keys) {
    this._listeners.set(fn, keys === undefined ? null : new Set(keys));
    return () => this._listeners.delete(fn);
  },
};

/** @type {ReturnType<typeof setTimeout> | undefined} */
let toastTimer;
/**
 * Show a transient status message on the status bar for five seconds.
 * @param {string} text
 * @returns {void}
 */
export function toast(text) {
  clearTimeout(toastTimer);
  store.set({ toast: text });
  toastTimer = setTimeout(() => store.set({ toast: "" }), 5000);
}
