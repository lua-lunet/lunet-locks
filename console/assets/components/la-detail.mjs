// Selected-lock detail panel: identity, lease counters, event history from the
// journal data layer. Break action is disabled for journal-derived locks — the
// journal feed is read-only and does not expose a break channel.

import { store } from "../lib/state.mjs";
import { esc, fmtClock, fmtDur, ICONS } from "../lib/util.mjs";

class LaDetail extends HTMLElement {
  connectedCallback() {
    this._unsub = store.subscribe(() => this.render());
    this.onclick = (e) => {
      if (e.target.closest("[data-act=close]")) store.set({ selectedId: null });
      if (e.target.closest("[data-act=watch]")) {
        const id = store.state.selectedId;
        const watched = new Set(store.state.watched);
        if (watched.has(id)) watched.delete(id); else watched.add(id);
        store.set({ watched });
      }
      // Break is intentionally not wired: journal locks have no break path.
    };
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

  render() {
    const { selectedId, journalLocks, journalEvents, now, watched } = store.state;
    if (selectedId === null) { this.innerHTML = ""; return; }

    const l = journalLocks.find((x) => x.lockId === selectedId);
    if (!l) { this.innerHTML = ""; return; }

    const held = l.expiry > now;
    const events = journalEvents
      .filter((e) => e.lockId === selectedId)
      .slice(0, 100); // cap display

    const kv = [
      ["holder", esc(l.holder), held ? "var(--color-text)" : "var(--color-neutral-600)"],
      ["lease id", String(l.leaseId), ""],
      ["acquired", `${fmtClock(l.acquiredTs)} (${fmtDur(now - l.acquiredTs)} ago)`, ""],
      ["renewals", `${l.renewCount}`, ""],
      ["expiry", held ? `${fmtClock(l.expiry)} (in ${fmtDur(l.expiry - now)})` : "expired",
        held && l.expiry - now < 12000 ? "var(--color-accent)" : ""],
    ].map(([k, v, c]) => `<span class="k">${k}</span><span${c ? ` style="color:${c}"` : ""}>${v}</span>`).join("");

    const evRows = events.map((e) =>
      `<div class="ev"><span class="t">${fmtClock(e.ts)}</span><span class="ev-${esc(e.kind)}">${esc(e.kind)}</span><span class="a">${esc(e.holder)}</span></div>`
    ).join("");

    const body = this.querySelector(".detail-body");
    const scrollTop = body ? body.scrollTop : 0;
    this.innerHTML = `<div class="detail">
      <div class="detail-head">
        <div class="path">lock ${l.lockId}</div>
        <button class="btn btn-ghost" data-act="close" aria-label="close">${ICONS.close}</button>
      </div>
      <div class="detail-body">
        <div class="kv">${kv}</div>
        <div class="rule"></div>
        <div class="detail-events">${evRows}</div>
      </div>
      <div class="detail-actions">
        <button class="btn btn-secondary" data-act="watch">${ICONS.bell}<span>${watched.has(l.lockId) ? "Watching" : "Watch"}</span></button>
        <button class="btn btn-primary" data-act="break" disabled title="break unavailable: journal feed is read-only">${ICONS.lockOpen}<span>Break</span></button>
      </div>
      <div style="padding:8px 12px;font-size:11px;color:var(--color-neutral-500);font-family:var(--font-mono)">break unavailable: journal feed is read-only</div>
    </div>`;
    this.querySelector(".detail-body").scrollTop = scrollTop;
  }
}

customElements.define("la-detail", LaDetail);
