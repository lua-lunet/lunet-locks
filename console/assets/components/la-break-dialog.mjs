// "Are you really sure" — breaking a lock requires typing the leaf name.
// On confirm: POST /locks/{id}/break, then a full refresh.

import { store, toast } from "../lib/state.mjs";
import { api } from "../lib/api.mjs";
import { esc, fmtDur } from "../lib/util.mjs";

class LaBreakDialog extends HTMLElement {
  /** @type {(() => void) | undefined} */
  _unsub;
  /** The confirmId the current markup was built for. @type {number | null | undefined} */
  _rendered = undefined;

  connectedCallback() {
    this._rendered = undefined;
    this._unsub = store.subscribe(() => this.render());
    this.render();
  }
  disconnectedCallback() { this._unsub?.(); }

  /** @returns {void} */
  render() {
    const { confirmId, locksAll, detail, now } = store.state;
    if (confirmId === this._rendered) return; // don't clobber the input mid-typing
    this._rendered = confirmId;
    if (confirmId === null || confirmId === undefined) { this.innerHTML = ""; return; }

    const l = detail?.lock?.id === confirmId ? detail.lock : locksAll.find((x) => x.id === confirmId);
    if (!l) { this.innerHTML = ""; return; }
    const leaf = l.name.slice(l.name.lastIndexOf("/") + 1);

    this.innerHTML = `
      <div class="dialog-backdrop">
        <div class="dialog">
          <div class="path">${esc(l.name)}</div>
          <div class="warn">Held by ${esc(l.holder ?? "—")} for ${fmtDur(now - (l.takenAtMs ?? now))},
            extended ${l.renewCount} times. Breaking bumps the fence token to
            <b>${l.fencingToken + 1}</b>; the current holder's next write is rejected.
            Type the leaf name to confirm.</div>
          <div class="actions">
            <input class="input" id="la-confirm" placeholder="${esc(leaf)}" spellcheck="false" autocomplete="off">
            <button class="btn btn-secondary" data-act="cancel">Cancel</button>
            <button class="btn btn-primary" data-act="confirm" disabled>Break lock</button>
          </div>
        </div>
      </div>`;

    // Own markup, just written above, so every lookup resolves.
    const input = /** @type {HTMLInputElement} */ (this.$("#la-confirm"));
    const confirmBtn = /** @type {HTMLButtonElement} */ (this.$("[data-act=confirm]"));
    input.focus();
    input.oninput = () => { confirmBtn.disabled = input.value.trim() !== leaf; };
    this.$(".dialog-backdrop").onclick = (e) => {
      const target = e.target instanceof Element ? e.target : null;
      if (!target?.closest(".dialog")) store.set({ confirmId: null });
    };
    this.$("[data-act=cancel]").onclick = () => store.set({ confirmId: null });
    confirmBtn.onclick = async () => {
      try {
        const r = await api.breakLock(l.id);
        toast(`broke ${l.name} — fence now ${r.lock.fencingToken}`);
      } catch (err) {
        toast(`break failed: ${err instanceof Error ? err.message : String(err)}`);
      }
      store.set({ confirmId: null });
      window.dispatchEvent(new Event("la:refresh"));
    };
  }

  /**
   * Look up one of this component's own dialog nodes.
   * @param {string} selector
   * @returns {HTMLElement}
   */
  $(selector) {
    const el = this.querySelector(selector);
    if (!(el instanceof HTMLElement)) throw new Error(`la-break-dialog: missing ${selector}`);
    return el;
  }
}

customElements.define("la-break-dialog", LaBreakDialog);
