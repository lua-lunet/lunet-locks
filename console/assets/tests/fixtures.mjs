// Literal fixture shapes matching console/openapi.yaml (transcribed in
// lib/types.mjs). NOW is a fixed instant so no case depends on the wall
// clock; every relative time is derived from it.

import { store } from "../lib/state.mjs";

/** Fixed fixture instant: 2023-11-14T22:13:20Z. */
export const NOW = 1_700_000_000_000;

/** @typedef {import("../lib/types.mjs").Event} Event */
/** @typedef {import("../lib/types.mjs").Lock} Lock */
/** @typedef {import("../lib/types.mjs").StoreState} StoreState */

/**
 * A held lock on /tenants/acme; override any field per case.
 * @param {Partial<Lock>} [overrides]
 * @returns {Lock}
 */
export function lockFixture(overrides = {}) {
  return {
    id: 1,
    name: "/tenants/acme",
    labels: ["prod", "db"],
    state: "held",
    holder: "00000000-0000-0000-0000-0000000000aa",
    fencingToken: 7,
    leaseMs: 15000,
    expiresAtMs: NOW + 30000,
    takenAtMs: NOW - 10000,
    renewCount: 3,
    ...overrides,
  };
}

/**
 * An acquire event for the lockFixture lock; override any field per case.
 * @param {Partial<Event>} [overrides]
 * @returns {Event}
 */
export function eventFixture(overrides = {}) {
  return {
    seq: 1,
    tsMs: NOW - 5000,
    kind: "acquire",
    lockId: 1,
    name: "/tenants/acme",
    actor: "admin@console",
    detail: "acquired; fence 7",
    ...overrides,
  };
}

/**
 * Snapshot store fields; the returned function restores them with one set().
 * Pair with preserveSession (and call it after this restore) — the restoring
 * set() rewrites the session blob.
 * @param {...(keyof StoreState)} keys
 * @returns {() => void}
 */
export function preserveStore(...keys) {
  const snap = keys.map((k) => [k, store.state[k]]);
  return () => {
    store.set(/** @type {Partial<StoreState>} */ (Object.fromEntries(snap)));
  };
}

/**
 * Mount a custom element into a hidden container attached to the document —
 * custom-element lifecycles only fire when connected. The caller removes
 * `host` in a finally, pass or fail, so the DOM is left exactly as found.
 * @param {string} tag
 * @returns {{ host: HTMLDivElement, el: HTMLElement }}
 */
export function mount(tag) {
  const host = document.createElement("div");
  host.hidden = true;
  document.body.appendChild(host);
  const el = document.createElement(tag);
  host.appendChild(el);
  return { host, el };
}
