// DOM smoke: la-lock-table with fixture locks — one row per lock, six cells
// per row, a free lock renders its free state, an empty list renders the
// placeholder. Structure only, never pixels. The store fields and session
// blob are restored in a finally; the container is removed pass or fail.

import { store } from "../lib/state.mjs";
import "../components/la-lock-table.mjs";
import { assertEqual, preserveSession } from "./assert.mjs";
import { NOW, lockFixture, mount, preserveStore } from "./fixtures.mjs";

/** @typedef {import("../lib/types.mjs").Lock} Lock */

/**
 * Set the fixture rows, mount the table, run fn against it, restore all.
 * Fully synchronous so no app poller can interleave.
 * @param {Lock[]} locks
 * @param {(el: HTMLElement) => void} fn
 * @returns {void}
 */
function withLocks(locks, fn) {
  const restoreSession = preserveSession("lock-admin");
  const restoreStore = preserveStore("locks", "mode", "now", "selectedId", "watched");
  try {
    store.set({ mode: "locks", now: NOW, selectedId: null, watched: new Set(), locks });
    const { host, el } = mount("la-lock-table");
    try {
      fn(el);
    } finally {
      host.remove();
    }
  } finally {
    restoreStore();
    restoreSession();
  }
}

/** @type {Partial<Lock>} */
const FREE = { state: "free", holder: null, expiresAtMs: null, takenAtMs: null, renewCount: 0 };

/** @type {import("./assert.mjs").Suite} */
export const suite = {
  name: "lock-table",
  cases: {
    "one row per lock, six cells per row"() {
      withLocks([
        lockFixture({ id: 1 }),
        lockFixture({ id: 2, name: "/jobs/nightly", ...FREE }),
        lockFixture({ id: 3, name: "/index/build", expiresAtMs: null }),
      ], (el) => {
        const rows = el.querySelectorAll(".lock-row");
        assertEqual(rows.length, 3, "row count");
        for (const row of rows) assertEqual(row.children.length, 6, "cell count");
      });
    },
    "a free lock renders the free state"() {
      withLocks([lockFixture({ id: 2, ...FREE })], (el) => {
        const row = el.querySelector(".lock-row");
        assertEqual(row?.textContent?.includes("free"), true, "free expiry cell");
      });
    },
    "an empty list renders the placeholder"() {
      withLocks([], (el) => {
        assertEqual(el.querySelectorAll(".lock-row").length, 0);
        assertEqual(el.textContent?.includes("no locks match"), true, "placeholder text");
      });
    },
  },
};
