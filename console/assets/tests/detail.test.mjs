// DOM smoke: la-detail with a fixture LockDetailResponse — five kv rows,
// one row per recent event, and the Break action gated on the lock being
// held. Structure only; the store and container are restored in a finally.

import { store } from "../lib/state.mjs";
import "../components/la-detail.mjs";
import { assertEqual, preserveSession } from "./assert.mjs";
import { NOW, eventFixture, lockFixture, mount, preserveStore } from "./fixtures.mjs";

/** @typedef {import("../lib/types.mjs").LockDetailResponse} LockDetailResponse */

/**
 * @param {LockDetailResponse} detail
 * @param {(el: HTMLElement) => void} fn
 * @returns {void}
 */
function withDetail(detail, fn) {
  const restoreSession = preserveSession("lock-admin");
  const restoreStore = preserveStore("detail", "selectedId", "now", "watched");
  try {
    store.set({ detail, selectedId: detail.lock.id, now: NOW, watched: new Set() });
    const { host, el } = mount("la-detail");
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
  name: "detail",
  cases: {
    "renders five kv rows and one row per recent event"() {
      withDetail({
        lock: lockFixture(),
        recentEvents: [eventFixture(), eventFixture({ seq: 2, kind: "renew", detail: "renewed; fence 7" })],
      }, (el) => {
        assertEqual(el.querySelectorAll(".kv .k").length, 5, "kv rows");
        assertEqual(el.querySelectorAll(".detail-events .ev").length, 2, "event rows");
        const brk = /** @type {HTMLButtonElement | null} */ (el.querySelector("[data-act=break]"));
        assertEqual(brk?.disabled, false, "held lock: break enabled");
      });
    },
    "a free lock disables the break action"() {
      withDetail({
        lock: lockFixture({ state: "free", holder: null, expiresAtMs: null, takenAtMs: null, renewCount: 0 }),
        recentEvents: [],
      }, (el) => {
        const brk = /** @type {HTMLButtonElement | null} */ (el.querySelector("[data-act=break]"));
        assertEqual(brk?.disabled, true, "free lock: break disabled");
      });
    },
  },
};
