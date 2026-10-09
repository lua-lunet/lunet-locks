// Append-only event log view from the journal data layer. Filtered by time
// range (header controls) client-side over store.journalEvents.

import { store } from "../lib/state.mjs";
import { esc, fmtClock, parseClock } from "../lib/util.mjs";

class LaLogView extends HTMLElement {
  /** @type {(() => void) | undefined} */
  _unsub;
  /** Skeleton nodes, memoised by selector on first lookup. @type {Map<string, HTMLElement>} */
  _refs = new Map();

  connectedCallback() {
    this.innerHTML = `
      <div class="log-head"><div>time</div><div>event</div><div>lock id</div><div>holder</div><div>lease id</div></div>
      <div class="log-rows"></div>`;
    // Own markup, so every $() lookup below resolves; results are memoised.
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

  render() {
    const { journalEvents, now, fromText, toText } = store.state;
    const fromMs = parseClock(fromText, now);
    const toMs = parseClock(toText, now);

    // Client-side filter: journal events are newest-first; reverse for display.
    let filtered = journalEvents;
    if (fromMs != null && toMs != null) {
      filtered = journalEvents.filter((e) => e.ts >= fromMs && e.ts <= toMs);
    } else if (fromMs != null) {
      filtered = journalEvents.filter((e) => e.ts >= fromMs);
    } else if (toMs != null) {
      filtered = journalEvents.filter((e) => e.ts <= toMs);
    }

    // Show oldest first in the log view.
    const sorted = [...filtered].sort((a, b) => a.ts - b.ts);

    const rows = sorted.map((e) => `
      <div class="log-row">
        <div class="t">${fmtClock(e.ts)}</div>
        <div class="ev-${esc(e.kind)}">${esc(e.kind)}</div>
        <div class="n">${e.lockId}</div>
        <div class="a">${esc(e.holder)}</div>
        <div class="d">${e.leaseId}</div>
      </div>`).join("");

    const scrollTop = this.$(".log-rows").scrollTop;
    this.$(".log-rows").innerHTML = rows || '<div style="padding:24px;color:var(--color-neutral-500);font-family:var(--font-mono);font-size:12px">no events in range</div>';
    this.$(".log-rows").scrollTop = scrollTop;
  }
}

customElements.define("la-log-view", LaLogView);
