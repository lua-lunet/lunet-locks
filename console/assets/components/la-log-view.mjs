// Append-only event log view from the journal data layer. Filtered by time
// range (header controls) client-side over store.journalEvents.

import { store } from "../lib/state.mjs";
import { esc, fmtClock, parseClock } from "../lib/util.mjs";

class LaLogView extends HTMLElement {
  connectedCallback() {
    this.innerHTML = `
      <div class="log-head"><div>time</div><div>event</div><div>lock id</div><div>holder</div><div>lease id</div></div>
      <div class="log-rows"></div>`;
    this._rowsEl = this.querySelector(".log-rows");
    this._unsub = store.subscribe(() => this.render());
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

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

    const scrollTop = this._rowsEl.scrollTop;
    this._rowsEl.innerHTML = rows || '<div style="padding:24px;color:var(--color-neutral-500);font-family:var(--font-mono);font-size:12px">no events in range</div>';
    this._rowsEl.scrollTop = scrollTop;
  }
}

customElements.define("la-log-view", LaLogView);
