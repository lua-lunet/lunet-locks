// Selected-lock side panel: identity, fencing, lease counters, recent events,
// watch toggle and the Break action.

import { store } from "../lib/state.mjs";
import { esc, fmtClock, fmtDur, ICONS } from "../lib/util.mjs";

class LaDetail extends HTMLElement {
  /** @type {(() => void) | undefined} */
  _unsub;

  connectedCallback() {
    this._unsub = store.subscribe(() => this.render());
    this.onclick = (e) => {
      const target = e.target instanceof Element ? e.target : null;
      if (!target) return;
      if (target.closest("[data-act=close]")) store.set({ selectedId: null, detail: null });
      if (target.closest("[data-act=watch]")) {
        const id = store.state.selectedId;
        // The panel only renders with a selection, but a concurrent 404 sweep
        // can clear it between paint and click — never watch a null id.
        if (id !== null) {
          const watched = new Set(store.state.watched);
          if (watched.has(id)) watched.delete(id); else watched.add(id);
          store.set({ watched });
        }
      }
      if (target.closest("[data-act=break]")) {
        store.set({ confirmId: store.state.selectedId });
      }
    };
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

  /** @returns {void} */
  render() {
    const { detail, now, watched } = store.state;
    if (!detail) { this.innerHTML = ""; return; }
    const l = detail.lock;
    const held = l.state === "held";
    const timed = held && l.expiresAtMs != null;

    /** @type {[string, string, string][]} [key, raw value, color] — the raw
     * value is escaped at render time and reused as the title so a truncated
     * cell still exposes the full string on hover. */
    const rows = [
      ["holder", held ? (l.holder ?? "—") : "—", held ? "var(--color-text)" : "var(--color-neutral-600)"],
      ["fence", String(l.fencingToken), ""],
      ["expires", timed ? `${fmtClock(/** @type {number} */ (l.expiresAtMs))} (in ${fmtDur(/** @type {number} */ (l.expiresAtMs) - now)})` : "free",
        timed && /** @type {number} */ (l.expiresAtMs) - now < 12000 ? "var(--color-accent)" : ""],
      ["taken at", held && l.takenAtMs != null ? `${fmtClock(l.takenAtMs)} (${fmtDur(now - l.takenAtMs)} ago)` : "—", ""],
      ["renewals", held ? `${l.renewCount} × ${Math.round(l.leaseMs / 1000)}s` : "—", ""],
    ];
    const kv = rows
      .map(([k, v, c]) => `<span class="k">${k}</span><span title="${esc(v)}"${c ? ` style="color:${c}"` : ""}>${esc(v)}</span>`)
      .join("");

    const events = (detail.recentEvents ?? []).map((e) =>
      `<div class="ev"><span class="t">${fmtClock(e.tsMs)}</span><span class="ev-${esc(e.kind)}">${esc(e.kind)}</span><span class="a" title="${esc(e.actor)}">${esc(e.actor)}</span></div>`
    ).join("");

    // Preserve the detail-body scroll position across the 1s rebuilds.
    const body = this.querySelector(".detail-body");
    const scrollTop = body ? body.scrollTop : 0;
    this.innerHTML = `<div class="detail">
      <div class="detail-head">
        <div class="path">${esc(l.name)}</div>
        <button class="btn btn-ghost" data-act="close" aria-label="close">${ICONS.close}</button>
      </div>
      <div class="detail-body">
        <div class="kv">${kv}</div>
        <div class="labels">${l.labels.map((t) => `<span class="tag">${esc(t)}</span>`).join("")}</div>
        <div class="rule"></div>
        <div class="detail-events">${events}</div>
      </div>
      <div class="detail-actions">
        <button class="btn btn-secondary" data-act="watch">${ICONS.bell}<span>${watched.has(l.id) ? "Watching" : "Watch"}</span></button>
        <button class="btn btn-primary" data-act="break" ${held ? "" : "disabled"}>${ICONS.lockOpen}<span>Break</span></button>
      </div>
    </div>`;
    const rebuilt = this.querySelector(".detail-body");
    if (rebuilt) rebuilt.scrollTop = scrollTop;
  }
}

customElements.define("la-detail", LaDetail);
