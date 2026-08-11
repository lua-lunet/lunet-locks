// Column-width math in lib/resizable.mjs: drag deltas, min clamping, the
// CSS custom-property string, persistence and dispose. Uses real but
// detached elements (never appended to the document) and synthetic
// PointerEvents; every case snapshots the store field and session blob it
// touches and restores them in a finally.

import { resizableColumns } from "../lib/resizable.mjs";
import { store } from "../lib/state.mjs";
import { assertDeepEqual, assertEqual, preserveSession } from "./assert.mjs";
import { preserveStore } from "./fixtures.mjs";

const KEY = "colWidths";
const VAR = "--test-cols";

/**
 * A detached host/head pair with one handle per default width.
 * @param {number[]} defaults
 * @param {object} [opts]
 * @param {number | number[]} [opts.min]
 * @param {(widths: number[]) => string} [opts.template]
 */
function rig(defaults, opts = {}) {
  const host = document.createElement("div");
  const head = document.createElement("div");
  const handles = defaults.map((_, i) => {
    const h = document.createElement("span");
    h.className = "col-resize";
    h.dataset.col = String(i);
    // Synthetic pointerIds have no active pointer; capture would throw.
    h.setPointerCapture = () => {};
    head.appendChild(h);
    return h;
  });
  const rc = resizableColumns({
    host, headEl: head, cssVar: VAR, key: KEY,
    defaults, min: opts.min, template: opts.template,
  });
  const css = () => host.style.getPropertyValue(VAR);
  return { handles, rc, css };
}

/**
 * @param {HTMLElement} handle
 * @param {number} x
 * @returns {void}
 */
function down(handle, x) {
  handle.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, clientX: x, pointerId: 1 }));
}
/**
 * @param {HTMLElement} handle
 * @param {number} x
 * @returns {void}
 */
function move(handle, x) {
  handle.dispatchEvent(new PointerEvent("pointermove", { bubbles: true, clientX: x, pointerId: 1 }));
}
/** @param {HTMLElement} handle @returns {void} */
function up(handle) {
  handle.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, pointerId: 1 }));
}
/** @param {HTMLElement} handle @returns {void} */
function cancel(handle) {
  handle.dispatchEvent(new PointerEvent("pointercancel", { bubbles: true, pointerId: 1 }));
}

/**
 * Run a case with colWidths nulled and both snapshots restored afterwards.
 * @param {() => void} fn
 * @returns {void}
 */
function withCleanWidths(fn) {
  const restoreSession = preserveSession("lock-admin");
  const restoreStore = preserveStore(KEY);
  try {
    store.set({ [KEY]: null });
    fn();
  } finally {
    restoreStore();
    restoreSession();
  }
}

/** @type {import("./assert.mjs").Suite} */
export const suite = {
  name: "resizable",
  cases: {
    "apply writes the default px track list"() {
      withCleanWidths(() => {
        const r = rig([100, 200, 300]);
        r.rc.dispose();
        assertEqual(r.css(), "100px 200px 300px");
      });
    },
    "a template customises the css var string"() {
      withCleanWidths(() => {
        const r = rig([100, 200, 300], { template: (w) => w.map((x) => x + "px").join(" ") + " minmax(0, 1fr)" });
        r.rc.dispose();
        assertEqual(r.css(), "100px 200px 300px minmax(0, 1fr)");
      });
    },
    "a drag moves the width by the clientX delta"() {
      withCleanWidths(() => {
        const r = rig([100, 200, 300], { min: 48 });
        down(r.handles[1], 500);
        move(r.handles[1], 530);
        assertEqual(r.css(), "100px 230px 300px");
        move(r.handles[1], 480);
        assertEqual(r.css(), "100px 180px 300px");
        up(r.handles[1]);
        r.rc.dispose();
      });
    },
    "a scalar min clamps the drag"() {
      withCleanWidths(() => {
        const r = rig([100, 200, 300], { min: 48 });
        down(r.handles[1], 500);
        move(r.handles[1], 0); // 200 - 500 would be -300 without clamping
        assertEqual(r.css(), "100px 48px 300px");
        up(r.handles[1]);
        r.rc.dispose();
      });
    },
    "a per-column min clamps each column separately"() {
      withCleanWidths(() => {
        const r = rig([100, 200, 300], { min: [10, 99, 10] });
        down(r.handles[1], 500);
        move(r.handles[1], 0);
        assertEqual(r.css(), "100px 99px 300px");
        up(r.handles[1]);
        r.rc.dispose();
      });
    },
    "pointerup persists the widths to the store"() {
      withCleanWidths(() => {
        const r = rig([100, 200, 300], { min: 48 });
        down(r.handles[1], 500);
        move(r.handles[1], 530);
        up(r.handles[1]);
        r.rc.dispose();
        assertDeepEqual(store.state[KEY], [100, 230, 300]);
      });
    },
    "pointercancel reverts to the persisted widths"() {
      const restoreSession = preserveSession("lock-admin");
      const restoreStore = preserveStore(KEY);
      try {
        store.set({ [KEY]: [111, 222, 333] });
        const r = rig([100, 200, 300], { min: 48 });
        assertEqual(r.css(), "111px 222px 333px", "apply reads the store");
        down(r.handles[1], 500);
        move(r.handles[1], 530);
        assertEqual(r.css(), "111px 252px 333px", "drag in progress");
        cancel(r.handles[1]);
        r.rc.dispose();
        assertEqual(r.css(), "111px 222px 333px", "cancel reverts the css var");
        assertDeepEqual(store.state[KEY], [111, 222, 333], "cancel leaves the store alone");
      } finally {
        restoreStore();
        restoreSession();
      }
    },
    "dispose detaches the delegation listener"() {
      withCleanWidths(() => {
        const r = rig([100, 200, 300], { min: 48 });
        r.rc.dispose();
        down(r.handles[1], 500);
        move(r.handles[1], 530);
        assertEqual(r.css(), "100px 200px 300px", "no listener, no drag");
      });
    },
  },
};
