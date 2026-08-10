// Drill-down tree over lock display paths (/cluster/members/0000001 →
// cluster → members). Click a node to filter by that path prefix; the caret
// collapses a subtree. Counts come from the unfiltered lock set.

import { store } from "../lib/state.mjs";
import { esc } from "../lib/util.mjs";

class LaTree extends HTMLElement {
  connectedCallback() {
    this._unsub = store.subscribe(() => this.render());
    this.onclick = (e) => {
      const row = e.target.closest(".tree-row");
      if (!row) return;
      const prefix = row.dataset.prefix;
      if (e.target.closest(".caret")) {
        const collapsed = new Set(store.state.collapsed);
        if (collapsed.has(prefix)) collapsed.delete(prefix); else collapsed.add(prefix);
        store.set({ collapsed });
      } else {
        const q = store.state.query.trim();
        store.set({ query: q === prefix ? "" : prefix });
      }
    };
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

  render() {
    const { locksAll, query, collapsed } = store.state;
    const counts = new Map(); // dir prefix -> lock count
    for (const l of locksAll) {
      const parts = l.name.split("/").filter(Boolean);
      for (let i = 1; i < parts.length; i++) {
        const key = "/" + parts.slice(0, i).join("/");
        counts.set(key, (counts.get(key) ?? 0) + 1);
      }
    }
    const keys = [...counts.keys()].sort();
    const childrenOf = (k) => keys.some((o) => o !== k && o.startsWith(k + "/"));
    const active = query.trim();

    let html = "";
    for (const k of keys) {
      const depth = k.split("/").length - 2; // /cluster → 0
      // hidden if any ancestor (shallower prefix of k) is collapsed
      let hidden = false;
      for (const c of collapsed) {
        if (k !== c && k.startsWith(c + "/")) { hidden = true; break; }
      }
      if (hidden) continue;
      const hasKids = childrenOf(k);
      const isActive = active === k;
      html += `<div class="tree-row${isActive ? " active" : ""}" data-prefix="${esc(k)}" style="padding-left:${8 + depth * 12}px">
        <span class="caret${collapsed.has(k) ? "" : " open"}">${hasKids ? "▸" : ""}</span>
        <span class="label">${esc(k.split("/").pop())}</span>
        <span class="count">${counts.get(k)}</span>
      </div>`;
    }
    this.innerHTML = html;
  }
}

customElements.define("la-tree", LaTree);
