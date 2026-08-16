// Lock rows from the journal data layer: lock id, holder (hex), lease id,
// acquired-at, renewals, expiry countdown. Row keys are lockId. Selection
// sets store.selectedId = lockId so the detail panel can look up events.

import { store } from "../lib/state.mjs";
import { esc, fmtDur, fmtClock, ICONS } from "../lib/util.mjs";

class LaLockTable extends HTMLElement {
  connectedCallback() {
    this.innerHTML = `
      <div class="grid-head"><div></div><div>lock id</div><div>holder</div><div>lease id</div><div>acquired</div><div>renewals</div><div>expiry</div></div>
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
    const { journalLocks, now, selectedId, mode, atText, tolSec } = store.state;
    const atMs = mode === "expiry" ? parseClockLocal(atText, now) : null;

    // In expiry mode, filter to locks whose expiry is within tolerance of atMs.
    let rows_data = journalLocks;
    if (mode === "expiry" && atMs != null) {
      const tolMs = tolSec * 1000;
      rows_data = journalLocks.filter((l) => Math.abs(l.expiry - atMs) <= tolMs);
    }

    const rows = rows_data.map((l) => {
      const rem = l.expiry - now;
      const urgent = rem < 12000;
      const held = rem > 0;

      let ttlText, ttlColor;
      if (held && mode === "expiry" && atMs != null) {
        const d = (l.expiry - atMs) / 1000;
        ttlText = (d >= 0 ? "+" : "") + d.toFixed(1) + "s";
        ttlColor = Math.abs(d) < tolSec / 2 ? "var(--color-accent)" : "var(--color-neutral-400)";
      } else if (held) {
        ttlText = fmtDur(rem);
        ttlColor = urgent ? "var(--color-accent)" : "var(--color-neutral-400)";
      } else {
        ttlText = "expired";
        ttlColor = "var(--color-accent-600)";
      }

      const iconColor = held ? (urgent ? "var(--color-accent)" : "var(--color-neutral-500)") : "var(--color-accent)";
      const classes = ["lock-row", selectedId === l.lockId ? "selected" : ""].filter(Boolean).join(" ");

      return `<div class="${classes}" data-id="${l.lockId}">
        <div class="icon" style="color:${iconColor}">${held ? ICONS.lock : ICONS.lockOpen}</div>
        <div class="cell">${l.lockId}</div>
        <div class="cell" style="color:${held ? "var(--color-text)" : "var(--color-neutral-600)"}">${esc(l.holder)}</div>
        <div class="cell" style="color:var(--color-neutral-500)">${l.leaseId}</div>
        <div class="cell" style="color:var(--color-neutral-500)">${fmtClock(l.acquiredTs)}</div>
        <div class="cell" style="color:var(--color-neutral-500)">${l.renewCount}</div>
        <div class="cell" style="color:${ttlColor}">${ttlText}</div>
      </div>`;
    }).join("");

    const scrollTop = this._rowsEl.scrollTop;
    this._rowsEl.innerHTML = rows || '<div style="padding:24px;color:var(--color-neutral-500);font-family:var(--font-mono);font-size:12px">no locks match</div>';
    this._rowsEl.scrollTop = scrollTop;
  }
}

// Local parseClock import (same as util but avoids circular issues).
function parseClockLocal(text, baseMs) {
  const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec((text ?? "").trim());
  if (!m) return null;
  const d = new Date(baseMs);
  d.setHours(+m[1], +m[2], +(m[3] ?? 0), 0);
  return d.getTime();
}

customElements.define("la-lock-table", LaLockTable);
