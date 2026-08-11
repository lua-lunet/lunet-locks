// Append-only event log view, filterable by time range (header controls) and
// the shared search box. Skeleton built once; only rows are replaced.

import { store } from "../lib/state.mjs";
import { esc, fmtClock } from "../lib/util.mjs";

// Column widths in px: time, event, lock, actor. Detail takes the remaining
// width. The actor default matches the lock table's holder column (a UUID is
// 36 chars, ~270px at 12px mono).
const DEFAULT_LOG_COLS = [104, 74, 280, 270];
const MIN_COL_PX = 48;

class LaLogView extends HTMLElement {
  /** @type {(() => void) | undefined} */
  _unsub;
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
    this._applyCols(store.state.logColWidths ?? DEFAULT_LOG_COLS);
    this.$(".log-head").onmousedown = (e) => this._startResize(e);
    this._unsub = store.subscribe(() => this.render());
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

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

  /**
   * @param {number[]} widths
   * @returns {void}
   */
  _applyCols(widths) {
    this.style.setProperty("--log-cols", widths.map((px) => px + "px").join(" ") + " minmax(160px, 1fr)");
  }

  /**
   * @param {MouseEvent} e
   * @returns {void}
   */
  _startResize(e) {
    const handle = e.target instanceof Element ? e.target.closest(".col-resize") : null;
    if (!(handle instanceof HTMLElement)) return;
    e.preventDefault();
    const col = Number(handle.dataset.col);
    const startX = e.clientX;
    const widths = [...(store.state.logColWidths ?? DEFAULT_LOG_COLS)];
    const startW = widths[col];
    /** @param {MouseEvent} ev */
    const onMove = (ev) => {
      widths[col] = Math.max(MIN_COL_PX, Math.round(startW + ev.clientX - startX));
      this._applyCols(widths);
    };
    const onUp = () => {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      document.body.style.userSelect = "";
      store.set({ logColWidths: widths });
    };
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
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
