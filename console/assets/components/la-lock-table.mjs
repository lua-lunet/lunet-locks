// Lock rows: icon, path, holder, expires, held-for, labels.
// In expiry mode the expires column shows the signed delta from the target time.

import { store, config } from "../lib/state.mjs";
import { esc, fmtDur, parseClock, ICONS } from "../lib/util.mjs";

// Column widths in px: icon, lock, holder, expires, held. Labels take the
// remaining width. Defaults are sized to the real data: a holder UUID is 36
// chars (~270px at 12px mono), the longest demo path ~36 chars (~280px).
const DEFAULT_COLS = [20, 280, 270, 104, 68];
const MIN_COL_PX = 48;

class LaLockTable extends HTMLElement {
  connectedCallback() {
    this.innerHTML = `
      <div class="rows">
        <div class="grid-head">
          <div></div>
          <div class="hcell">lock<span class="col-resize" data-col="1"></span></div>
          <div class="hcell">holder<span class="col-resize" data-col="2"></span></div>
          <div class="hcell">expires<span class="col-resize" data-col="3"></span></div>
          <div class="hcell">held<span class="col-resize" data-col="4"></span></div>
          <div>labels</div>
        </div>
        <div class="body"></div>
      </div>`;
    this._rowsEl = this.querySelector(".rows");
    this._bodyEl = this.querySelector(".body");
    this._applyCols(store.state.colWidths ?? DEFAULT_COLS);
    this.querySelector(".grid-head").onmousedown = (e) => this._startResize(e);
    this._unsub = store.subscribe(() => this.render());
    this.onclick = (e) => {
      const row = e.target.closest(".lock-row");
      if (row) store.set({ selectedId: Number(row.dataset.id) });
    };
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

  _applyCols(widths) {
    this.style.setProperty("--cols", widths.map((px) => px + "px").join(" ") + " minmax(120px, 1fr)");
  }

  _startResize(e) {
    const handle = e.target.closest(".col-resize");
    if (!handle) return;
    e.preventDefault();
    const col = Number(handle.dataset.col);
    const startX = e.clientX;
    const widths = [...(store.state.colWidths ?? DEFAULT_COLS)];
    const startW = widths[col];
    const onMove = (ev) => {
      widths[col] = Math.max(MIN_COL_PX, Math.round(startW + ev.clientX - startX));
      this._applyCols(widths);
    };
    const onUp = () => {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      document.body.style.userSelect = "";
      store.set({ colWidths: widths }); // persists to sessionStorage
    };
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }

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

    // Only the row markup is replaced; the header, scroll container, and any
    // in-progress column drag survive the 1s ticks, and scrollTop is restored
    // in case the browser resets it.
    const scrollTop = this._rowsEl.scrollTop;
    this._bodyEl.innerHTML = rows || '<div style="padding:24px;color:var(--color-neutral-500);font-family:var(--font-mono);font-size:12px">no locks match</div>';
    this._rowsEl.scrollTop = scrollTop;
  }
}

customElements.define("la-lock-table", LaLockTable);
