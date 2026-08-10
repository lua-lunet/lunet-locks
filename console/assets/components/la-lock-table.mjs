// Lock rows: icon, path, holder, expires, held-for, labels.
// In expiry mode the expires column shows the signed delta from the target time.

import { store, config } from "../lib/state.mjs";
import { esc, fmtDur, parseClock, ICONS } from "../lib/util.mjs";

class LaLockTable extends HTMLElement {
  connectedCallback() {
    this.innerHTML = `
      <div class="grid-head"><div></div><div>lock</div><div>holder</div><div>expires</div><div>held</div><div>labels</div></div>
      <div class="rows"></div>`;
    this._rowsEl = this.querySelector(".rows");
    this._unsub = store.subscribe(() => this.render());
    this.onclick = (e) => {
      const row = e.target.closest(".lock-row");
      if (row) store.set({ selectedId: Number(row.dataset.id) });
    };
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

  render() {
    const { locks, now, selectedId, watched, mode, atText, tolSec } = store.state;
    const atMs = mode === "expiry" ? parseClock(atText, now) : null;

    const rows = locks.map((l) => {
      const held = l.state === "held";
      const rem = held ? l.expiresAtMs - now : 0;
      const urgent = held && rem < 12000;
      const cut = l.name.lastIndexOf("/");
      const dir = l.name.slice(0, cut + 1);
      const leaf = l.name.slice(cut + 1);
      const hot = watched.has(l.id) && held && rem < config.watchWarnMs;

      let ttlText, ttlColor;
      if (held && mode === "expiry" && atMs != null) {
        const d = (l.expiresAtMs - atMs) / 1000;
        ttlText = (d >= 0 ? "+" : "") + d.toFixed(1) + "s";
        ttlColor = Math.abs(d) < tolSec / 2 ? "var(--color-accent)" : "var(--color-neutral-400)";
      } else if (held) {
        ttlText = fmtDur(rem);
        ttlColor = urgent ? "var(--color-accent)" : "var(--color-neutral-400)";
      } else {
        ttlText = "free";
        ttlColor = "var(--color-accent-600)";
      }

      const iconColor = held ? (urgent ? "var(--color-accent)" : "var(--color-neutral-500)") : "var(--color-accent)";
      const classes = ["lock-row",
        selectedId === l.id ? "selected" : "",
        hot ? "watched-hot" : ""].filter(Boolean).join(" ");

      return `<div class="${classes}" data-id="${l.id}">
        <div class="icon" style="color:${iconColor}">${held ? ICONS.lock : ICONS.lockOpen}</div>
        <div class="name">
          <span class="dir">${esc(dir)}</span><span class="leaf">${esc(leaf)}</span>
          ${watched.has(l.id) ? `<span style="color:var(--color-accent);flex:none">${ICONS.bell}</span>` : ""}
        </div>
        <div class="cell" style="color:${held ? "var(--color-text)" : "var(--color-neutral-600)"}">${esc(held ? l.holder : "—")}</div>
        <div class="cell" style="color:${ttlColor}">${ttlText}</div>
        <div class="cell" style="color:var(--color-neutral-500)">${held ? fmtDur(now - l.takenAtMs) : "—"}</div>
        <div class="labels">${l.labels.map((t) => `<span class="tag">${esc(t)}</span>`).join("")}</div>
      </div>`;
    }).join("");

    // Only the row markup is replaced; the scroll container is stable across
    // the 1s ticks, and scrollTop is restored in case the browser resets it.
    const scrollTop = this._rowsEl.scrollTop;
    this._rowsEl.innerHTML = rows || '<div style="padding:24px;color:var(--color-neutral-500);font-family:var(--font-mono);font-size:12px">no locks match</div>';
    this._rowsEl.scrollTop = scrollTop;
  }
}

customElements.define("la-lock-table", LaLockTable);
