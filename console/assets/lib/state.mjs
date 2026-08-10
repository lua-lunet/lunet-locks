// Central store: one mutable state object + pub/sub. UI prefs persist to
// sessionStorage so a refresh restores the console exactly as left.

import { fmtClock } from "./util.mjs";

export const config = JSON.parse(document.getElementById("la-config").textContent);

const SAVED_KEY = "lock-admin";
const saved = JSON.parse(sessionStorage.getItem(SAVED_KEY) ?? "{}");

function persist(state) {
  sessionStorage.setItem(SAVED_KEY, JSON.stringify({
    mode: state.mode,
    query: state.query,
    tolSec: state.tolSec,
    atText: state.atText,
    fromText: state.fromText,
    toText: state.toText,
    watched: [...state.watched],
    collapsed: [...state.collapsed],
  }));
}

export const store = {
  state: {
    now: Date.now(),
    mode: saved.mode ?? "locks", // locks | expiry | telemetry | log
    query: saved.query ?? "",
    atText: saved.atText ?? fmtClock(Date.now() + config.expiryDefaultOffsetMs),
    tolSec: saved.tolSec ?? config.defaultToleranceSec,
    fromText: saved.fromText ?? fmtClock(Date.now() - config.logDefaultWindowMs),
    toText: saved.toText ?? fmtClock(Date.now()),
    cluster: null,
    locksAll: [],       // unfiltered — drives the path tree
    locks: [],          // filtered per current mode/search
    serverNowMs: Date.now(),
    selectedId: null,
    detail: null,       // {lock, recentEvents}
    watched: new Set(saved.watched ?? []),
    collapsed: new Set(saved.collapsed ?? ["/tenants", "/jobs", "/index"]),
    confirmId: null,    // lock id pending a break confirmation
    events: [],         // log view rows
    series: null,       // {bucketMs, buckets}
    toast: "",          // transient status text
    error: "",
  },
  _listeners: new Set(),
  set(patch) {
    Object.assign(this.state, patch);
    persist(this.state);
    for (const fn of this._listeners) fn(this.state);
  },
  subscribe(fn) {
    this._listeners.add(fn);
    return () => this._listeners.delete(fn);
  },
};

let toastTimer;
export function toast(text) {
  clearTimeout(toastTimer);
  store.set({ toast: text });
  toastTimer = setTimeout(() => store.set({ toast: "" }), 5000);
}
