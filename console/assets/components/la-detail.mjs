// Selected-lock side panel: identity, fencing, lease counters, recent events,
// watch toggle and the Break action.

import { store } from "../lib/state.mjs";
import { esc, fmtClock, fmtDur, ICONS } from "../lib/util.mjs";

class LaDetail extends HTMLElement {
  connectedCallback() {
    this._unsub = store.subscribe(() => this.render());
    this.onclick = (e) => {
      if (e.target.closest("[data-act=close]")) store.set({ selectedId: null, detail: null });
      if (e.target.closest("[data-act=watch]")) {
        const id = store.state.selectedId;
        const watched = new Set(store.state.watched);
        if (watched.has(id)) watched.delete(id); else watched.add(id);
        store.set({ watched });
      }
      if (e.target.closest("[data-act=break]")) {
        store.set({ confirmId: store.state.selectedId });
      }
    };
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

  render() {
    const { detail, now, watched } = store.state;
    if (!detail) { this.innerHTML = ""; return; }
    const l = detail.lock;
    const held = l.state === "held";

    const kv = [
      ["holder", held ? esc(l.holder) : "—", held ? "var(--color-text)" : "var(--color-neutral-600)"],
      ["session", held ? esc(l.session) : "—", ""],
      ["fence", String(l.fencingToken), ""],
      ["expires", held ? `${fmtClock(l.expiresAtMs)} (in ${fmtDur(l.expiresAtMs - now)})` : "free",
        held && l.expiresAtMs - now < 12000 ? "var(--color-accent)" : ""],
      ["extends", held ? `${l.renewCount} × ${Math.round(l.leaseMs / 1000)}s` : "—", ""],
      ["holder since", held ? `${fmtClock(l.holderSinceMs)} (${fmtDur(now - l.holderSinceMs)})` : "—", ""],
      ["changes", `${l.holderChanges} since boot`, ""],
    ].map(([k, v, c]) => `<span class="k">${k}</span><span${c ? ` style="color:${c}"` : ""}>${v}</span>`).join("");

    const events = (detail.recentEvents ?? []).map((e) =>
      `<div class="ev"><span class="t">${fmtClock(e.tsMs)}</span><span class="ev-${esc(e.kind)}">${esc(e.kind)}</span><span class="a">${esc(e.actor)}</span></div>`
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
    this.querySelector(".detail-body").scrollTop = scrollTop;
  }
}

customElements.define("la-detail", LaDetail);
