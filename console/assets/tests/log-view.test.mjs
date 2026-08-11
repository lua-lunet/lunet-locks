// DOM smoke: la-log-view with fixture events — one row per event, five
// cells per row, an empty range renders the placeholder. Structure only.

import { store } from "../lib/state.mjs";
import "../components/la-log-view.mjs";
import { assertEqual, preserveSession } from "./assert.mjs";
import { eventFixture, mount, preserveStore } from "./fixtures.mjs";

/** @typedef {import("../lib/types.mjs").Event} Event */

/**
 * @param {Event[]} events
 * @param {(el: HTMLElement) => void} fn
 * @returns {void}
 */
function withEvents(events, fn) {
  const restoreSession = preserveSession("lock-admin");
  const restoreStore = preserveStore("events");
  try {
    store.set({ events });
    const { host, el } = mount("la-log-view");
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
  name: "log-view",
  cases: {
    "one row per event, five cells per row"() {
      withEvents([
        eventFixture({ seq: 1 }),
        eventFixture({ seq: 2, kind: "renew", detail: "renewed; fence 7" }),
        eventFixture({ seq: 3, kind: "release", actor: null, detail: "released" }),
      ], (el) => {
        const rows = el.querySelectorAll(".log-row");
        assertEqual(rows.length, 3, "row count");
        for (const row of rows) assertEqual(row.children.length, 5, "cell count");
      });
    },
    "an empty range renders the placeholder"() {
      withEvents([], (el) => {
        assertEqual(el.querySelectorAll(".log-row").length, 0);
        assertEqual(el.textContent?.includes("no events in range"), true, "placeholder text");
      });
    },
  },
};
