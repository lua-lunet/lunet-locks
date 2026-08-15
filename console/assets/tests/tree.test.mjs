// DOM smoke: la-tree path grouping — one row per directory prefix with the
// unfiltered lock count, and a collapsed prefix hiding its descendants.

import { store } from "../lib/state.mjs";
import "../components/la-tree.mjs";
import { assertEqual, preserveSession } from "./assert.mjs";
import { lockFixture, mount, preserveStore } from "./fixtures.mjs";

/** @typedef {import("../lib/types.mjs").Lock} Lock */

// Four locks over two top-level dirs: /tenants/acme has 3 (db ×2, web ×1),
// /jobs has 1. Prefix rows: /tenants, /tenants/acme, /tenants/acme/db,
// /tenants/acme/web, /jobs, /jobs/nightly → 6 rows when nothing collapses.
const LOCKS = [
  lockFixture({ id: 11, name: "/tenants/acme/db/0001" }),
  lockFixture({ id: 12, name: "/tenants/acme/db/0002" }),
  lockFixture({ id: 13, name: "/tenants/acme/web/0001" }),
  lockFixture({ id: 14, name: "/jobs/nightly/0001" }),
];

/**
 * @param {Set<string>} collapsed
 * @param {(el: HTMLElement) => void} fn
 * @returns {void}
 */
function withTree(collapsed, fn) {
  const restoreSession = preserveSession("lock-admin");
  const restoreStore = preserveStore("locksAll", "query", "collapsed");
  try {
    store.set({ locksAll: LOCKS, query: "", collapsed });
    const { host, el } = mount("la-tree");
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

/** @type {import("./assert.mjs").Suite} */
export const suite = {
  name: "tree",
  cases: {
    "paths group into prefix rows with counts"() {
      withTree(new Set(), (el) => {
        assertEqual(el.querySelectorAll(".tree-row").length, 6, "prefix row count");
        const acme = el.querySelector('[data-prefix="/tenants/acme"] .count');
        assertEqual(acme?.textContent, "3", "/tenants/acme count");
        const db = el.querySelector('[data-prefix="/tenants/acme/db"] .count');
        assertEqual(db?.textContent, "2", "/tenants/acme/db count");
      });
    },
    "a collapsed prefix hides its descendants"() {
      withTree(new Set(["/tenants"]), (el) => {
        // Visible: /tenants, /jobs, /jobs/nightly.
        assertEqual(el.querySelectorAll(".tree-row").length, 3, "visible rows");
        assertEqual(el.querySelector('[data-prefix="/tenants/acme"]'), null, "descendant hidden");
      });
    },
  },
};
