// Right-hand debug dock: a live view of BroadcastChannel("lock-admin-log").
// All filtering and formatting happen in assets/log-worker.mjs (a module
// Worker — the main thread cannot hold the channel AND must stay off the
// formatting path); this component only appends batches to a scroll div.
//
// Run-tests contract for item46: the Run tests button dispatches a cancelable
// CustomEvent("la:run-tests") on `window`. The harness claims the event by
// calling preventDefault(); if dispatchEvent still returns true, no harness
// is loaded and the stub logs a WARNING so this item is testable alone.
//
// Persistence (sessionStorage, survives F5 within the tab):
//   lock-admin-debug-open   "1" while the dock is open (read by la-app)
//   lock-admin-debug-filter raw filter text
//   lock-admin-log-level    level NAME, written via log.mjs setLevel() so the
//                           console mirror and the worker share a threshold

import { Level, getLevel, setLevel, logger } from "../lib/log.mjs";
import { debounce } from "../lib/util.mjs";

const log = logger("debug-panel");

const OPEN_KEY = "lock-admin-debug-open";
const FILTER_KEY = "lock-admin-debug-filter";

/** DOM cost control: at most this many line nodes, dropped from the top. */
const MAX_LINES = 2000;
/** Px of slack when deciding whether the view is pinned to the bottom. */
const PIN_SLACK_PX = 24;

/** Extract the LEVEL token the worker formats at a fixed position. */
const LINE_LEVEL_RE = /^\d{2}:\d{2}:\d{2}\.\d{3}  (\S+)  /;

/** Batch posted by log-worker.mjs (kept in sync with the worker's typedef).
 * @typedef {object} LogBatch
 * @property {string[]} lines
 * @property {number} dropped
 */

/** @returns {boolean} */
export function isDebugOpen() {
  try {
    return sessionStorage.getItem(OPEN_KEY) === "1";
  } catch {
    return false;
  }
}

/** @param {boolean} open @returns {void} */
export function setDebugOpen(open) {
  try {
    if (open) sessionStorage.setItem(OPEN_KEY, "1");
    else sessionStorage.removeItem(OPEN_KEY);
  } catch { /* dock state just won't persist */ }
}

/** Current level as a NAME, reverse-looked-up from log.mjs's numeric.
 * @returns {string} */
function levelName() {
  const n = getLevel();
  for (const [k, v] of Object.entries(Level)) if (v === n) return k;
  return "INFO";
}

/** @returns {string} */
function readFilter() {
  try {
    return sessionStorage.getItem(FILTER_KEY) ?? "";
  } catch {
    return "";
  }
}

class LaDebugPanel extends HTMLElement {
  /** @type {Worker | undefined} */
  _worker;
  /** @type {HTMLElement | undefined} */
  _stream;
  _pinned = true;

  connectedCallback() {
    this.innerHTML = `<div class="dbg-controls">
      <select class="input dbg-level" title="Level threshold">
        ${["OFF", "SEVERE", "WARNING", "INFO", "CONFIG", "FINE", "FINER", "FINEST", "ALL"]
          .map((n) => `<option value="${n}">${n}</option>`).join("")}
      </select>
      <input class="input dbg-filter" spellcheck="false" placeholder="filter logger + msg">
      <button class="btn btn-secondary dbg-clear" title="Clear the view">clear</button>
      <button class="btn btn-secondary dbg-copy" title="Copy all visible lines">copy</button>
      <button class="btn btn-primary dbg-run" title="Run the in-browser test harness">run tests</button>
    </div>
    <div class="dbg-stream"></div>`;

    const levelSel = /** @type {HTMLSelectElement} */ (this.querySelector(".dbg-level"));
    const filterBox = /** @type {HTMLInputElement} */ (this.querySelector(".dbg-filter"));
    const clearBtn = /** @type {HTMLButtonElement} */ (this.querySelector(".dbg-clear"));
    const copyBtn = /** @type {HTMLButtonElement} */ (this.querySelector(".dbg-copy"));
    const runBtn = /** @type {HTMLButtonElement} */ (this.querySelector(".dbg-run"));
    this._stream = /** @type {HTMLElement} */ (this.querySelector(".dbg-stream"));
    const stream = this._stream;

    levelSel.value = levelName();
    filterBox.value = readFilter();

    // The worker owns the channel; this thread never touches BroadcastChannel.
    this._worker = new Worker(new URL("../log-worker.mjs", import.meta.url), { type: "module" });
    const worker = this._worker;
    worker.onmessage = (ev) => this._appendBatch(/** @type {LogBatch} */ (ev.data));
    worker.postMessage({ type: "level", value: levelSel.value });
    worker.postMessage({ type: "filter", value: filterBox.value });

    // Level writes through setLevel() so the sessionStorage threshold that
    // gates log.mjs's console mirror and the worker's threshold stay in
    // agreement; the worker is told explicitly so it need not poll.
    levelSel.onchange = () => {
      setLevel(levelSel.value);
      worker.postMessage({ type: "level", value: levelSel.value });
    };
    filterBox.oninput = debounce(() => {
      try {
        sessionStorage.setItem(FILTER_KEY, filterBox.value);
      } catch { /* live filtering still works for the session */ }
      worker.postMessage({ type: "filter", value: filterBox.value });
    }, 250);
    clearBtn.onclick = () => {
      stream.replaceChildren();
      worker.postMessage({ type: "clear" });
    };
    copyBtn.onclick = () => {
      if (!navigator.clipboard) {
        log.warning("clipboard unavailable (insecure context?)");
        return;
      }
      const text = [...stream.children].map((el) => el.textContent ?? "").join("\n");
      navigator.clipboard.writeText(text).then(
        () => log.info(`copied ${stream.childElementCount} lines`),
        (e) => log.warning("copy failed", { error: e }),
      );
    };
    // item46 listens for this and preventDefault()s it to claim the run.
    runBtn.onclick = () => {
      const ev = new CustomEvent("la:run-tests", { cancelable: true });
      const unclaimed = window.dispatchEvent(ev);
      if (unclaimed) log.warning("test harness not loaded", { event: "la:run-tests" });
    };

    // Pin bookkeeping: appends only auto-scroll while the user is at the
    // bottom; scrolling up suspends the pin until they return.
    stream.addEventListener("scroll", () => {
      this._pinned = stream.scrollTop + stream.clientHeight >= stream.scrollHeight - PIN_SLACK_PX;
    });
  }

  disconnectedCallback() {
    this._worker?.terminate();
    this._worker = undefined;
  }

  /**
   * One DocumentFragment per batch, one append, no innerHTML. The DOM cap
   * drops whole line nodes from the top; when the user is scrolled up the
   * scroll position is compensated so their view does not jump.
   * @param {LogBatch} batch
   * @returns {void}
   */
  _appendBatch(batch) {
    const stream = this._stream;
    if (!stream || !batch || !Array.isArray(batch.lines) || batch.lines.length === 0) return;
    const frag = document.createDocumentFragment();
    for (const line of batch.lines) {
      const div = document.createElement("div");
      const m = LINE_LEVEL_RE.exec(line);
      div.className = "dbg-line" + (m ? ` lv-${m[1]}` : "");
      div.textContent = line;
      frag.appendChild(div);
    }
    const excess = stream.childElementCount + batch.lines.length - MAX_LINES;
    let removedH = 0;
    if (excess > 0) {
      // Lines are `white-space: pre` with a fixed line-height, so every line
      // is the same height: one layout read covers the whole trim.
      const first = stream.firstElementChild;
      const lineH = first instanceof HTMLElement ? first.offsetHeight : 0;
      for (let i = 0; i < excess && stream.firstElementChild; i++) {
        stream.firstElementChild.remove();
      }
      removedH = lineH * excess;
    }
    stream.appendChild(frag);
    if (this._pinned) stream.scrollTop = stream.scrollHeight;
    else if (removedH > 0) stream.scrollTop = Math.max(0, stream.scrollTop - removedH);
  }
}

customElements.define("la-debug-panel", LaDebugPanel);
