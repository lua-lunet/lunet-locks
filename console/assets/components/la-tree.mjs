// Flat tree over journal lock ids: /locks/<lockId> per active lock. Click a
// node to select that lock in the detail panel. Numeric lock ids have no
// hierarchical display paths, so this is a single-level grouping.

import { store } from "../lib/state.mjs";
import { esc } from "../lib/util.mjs";

class LaTree extends HTMLElement {
  connectedCallback() {
    this._unsub = store.subscribe(() => this.render());
    this.onclick = (e) => {
      const row = e.target.closest(".tree-row");
      if (!row) return;
      const id = Number(row.dataset.id);
      if (!isNaN(id)) store.set({ selectedId: id });
    };
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

  render() {
    const { journalLocks, selectedId } = store.state;

    // Group header + one row per active lock.
    let html = `<div class="tree-row" style="padding-left:8px;font-weight:600;color:var(--color-neutral-400)">
      <span class="label">locks</span>
      <span class="count">${journalLocks.length}</span>
    </div>`;

    for (const l of journalLocks) {
      const isActive = selectedId === l.lockId;
      html += `<div class="tree-row${isActive ? " active" : ""}" data-id="${l.lockId}" style="padding-left:20px">
        <span class="label">${esc(String(l.lockId))}</span>
      </div>`;
    }

    this.innerHTML = html;
  }
}

customElements.define("la-tree", LaTree);
