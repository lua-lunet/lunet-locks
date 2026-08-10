// Append-only event log view, filterable by time range (header controls) and
// the shared search box. Skeleton built once; only rows are replaced.

import { store } from "../lib/state.mjs";
import { esc, fmtClock } from "../lib/util.mjs";

class LaLogView extends HTMLElement {
  connectedCallback() {
    this.innerHTML = `
      <div class="log-head"><div>time</div><div>event</div><div>lock</div><div>actor</div><div>detail</div></div>
      <div class="log-rows"></div>`;
    this._rowsEl = this.querySelector(".log-rows");
    this._unsub = store.subscribe(() => this.render());
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

  render() {
    const rows = store.state.events.map((e) => `
      <div class="log-row">
        <div class="t">${fmtClock(e.tsMs)}</div>
        <div class="ev-${esc(e.kind)}">${esc(e.kind)}</div>
        <div class="n">${esc(e.name)}</div>
        <div class="a">${esc(e.actor)}</div>
        <div class="d">${esc(e.detail)}</div>
      </div>`).join("");

    const scrollTop = this._rowsEl.scrollTop;
    this._rowsEl.innerHTML = rows || '<div style="padding:24px;color:var(--color-neutral-500);font-family:var(--font-mono);font-size:12px">no events in range</div>';
    this._rowsEl.scrollTop = scrollTop;
  }
}

customElements.define("la-log-view", LaLogView);
