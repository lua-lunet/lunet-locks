// Append-only event log view, filterable by time range (header controls) and
// the shared search box. Skeleton built once; only rows are replaced.

import { store } from "../lib/state.mjs";
import { esc, fmtClock } from "../lib/util.mjs";
import { resizableColumns } from "../lib/resizable.mjs";

/** @typedef {import("../lib/resizable.mjs").ResizableColumns} ResizableColumns */

// Column widths in px: time, event, lock, actor. Detail takes the remaining
// width (the trailing minmax() in styles.css). The actor default matches the
// lock table's holder column (a UUID is 36 chars, ~270px at 12px mono).
const DEFAULT_LOG_COLS = [104, 74, 280, 270];
const MIN_COL_PX = 48;

class LaLogView extends HTMLElement {
  /** @type {(() => void) | undefined} */
  _unsub;
  /** @type {ResizableColumns | undefined} */
  _cols;
  /** Skeleton nodes, memoised by selector on first lookup. @type {Map<string, HTMLElement>} */
  _refs = new Map();

  connectedCallback() {
    this.innerHTML = `
      <div class="log-rows">
        <div class="log-head">
          <div class="hcell">time<span class="col-resize" data-col="0"></span></div>
          <div class="hcell">event<span class="col-resize" data-col="1"></span></div>
          <div class="hcell">lock<span class="col-resize" data-col="2"></span></div>
          <div class="hcell">actor<span class="col-resize" data-col="3"></span></div>
          <div>detail</div>
        </div>
        <div class="log-body"></div>
      </div>`;
    // Own markup, so every $() lookup below resolves; results are memoised.
    this._cols = resizableColumns({
      host: this, headEl: this.$(".log-head"),
      cssVar: "--log-cols", key: "logColWidths",
      defaults: DEFAULT_LOG_COLS, min: MIN_COL_PX,
    });
    this._unsub = store.subscribe(() => this.render(), ["events"]);
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); this._cols?.dispose(); }

  /**
   * Look up one of this component's own skeleton nodes, memoising the result
   * so the 1s tick does not re-query the DOM.
   * @param {string} selector
   * @returns {HTMLElement}
   */
  $(selector) {
    const cached = this._refs.get(selector);
    if (cached) return cached;
    const el = this.querySelector(selector);
    if (!(el instanceof HTMLElement)) throw new Error(`la-log-view: missing ${selector}`);
    this._refs.set(selector, el);
    return el;
  }

  /** @returns {void} */
  render() {
    const rows = store.state.events.map((e) => `
      <div class="log-row">
        <div class="t">${fmtClock(e.tsMs)}</div>
        <div class="ev-${esc(e.kind)}">${esc(e.kind)}</div>
        <div class="n">${esc(e.name)}</div>
        <div class="a">${esc(e.actor)}</div>
        <div class="d">${esc(e.detail)}</div>
      </div>`).join("");

    const scrollTop = this.$(".log-rows").scrollTop;
    this.$(".log-body").innerHTML = rows || '<div style="padding:24px;color:var(--color-neutral-500);font-family:var(--font-mono);font-size:12px">no events in range</div>';
    this.$(".log-rows").scrollTop = scrollTop;
  }
}

customElements.define("la-log-view", LaLogView);
