// Drag-to-resize grid columns, persisted to the store. The single
// implementation behind every resizable grid in the console: the lock table,
// the log view, and the app shell's pane dividers.
//
// Markup contract: headEl contains resize handles (`.col-resize` by default),
// each carrying a `data-col` index into the widths array; host owns the CSS
// custom property the grid template reads, e.g.
//   grid-template-columns: var(--cols, 20px 280px …) minmax(120px, 1fr);
//
// Components re-render by replacing only their row markup, so the header (and
// these listeners) survive the 1s poll. A drag in progress writes the CSS var
// directly on every move and persists to the store exactly once, on
// pointerup — so a poll can neither clobber the drag nor lose saved widths.

import { store } from "./state.mjs";

/** @typedef {import("./types.mjs").StoreState} StoreState */

/** Store keys that hold an array of persisted px widths.
 * @typedef {"colWidths" | "logColWidths" | "paneWidths"} WidthsKey */

/**
 * @typedef {object} ResizableColumnsOptions
 * @property {HTMLElement} host Element that owns the CSS custom property.
 * @property {HTMLElement} headEl Element containing the handles; the pointerdown delegation target.
 * @property {string} cssVar Custom property receiving the track list, e.g. "--cols".
 * @property {WidthsKey} key Store key the px widths persist under.
 * @property {number[]} defaults Px widths used while the store has none.
 * @property {number | number[]} [min] Min px per column: one number for all, or per-column. Default 40.
 * @property {(widths: number[]) => string} [template] Full custom-property value; default is the px widths joined with spaces (grids with divider tracks or a collapsible trailing pane need this).
 * @property {string} [selector] Handle selector for closest(); default ".col-resize".
 */

/**
 * @typedef {object} ResizableColumns
 * @property {() => void} apply Re-write the CSS var from the store — after a re-render or a layout-mode change.
 * @property {() => void} dispose Detach the delegation listener; call from disconnectedCallback.
 */

/**
 * Install drag-and-persist column resizing on a grid.
 * @param {ResizableColumnsOptions} opts
 * @returns {ResizableColumns}
 */
export function resizableColumns({ host, headEl, cssVar, key, defaults, min = 40, template, selector = ".col-resize" }) {
  /**
   * @param {number} col
   * @returns {number}
   */
  const minFor = (col) => (Array.isArray(min) ? (min[col] ?? 40) : min);

  /** @returns {number[]} */
  const read = () => [...(store.state[key] ?? defaults)];

  /**
   * @param {number[]} widths
   * @returns {void}
   */
  const write = (widths) => {
    host.style.setProperty(cssVar, template ? template(widths) : widths.map((w) => w + "px").join(" "));
  };

  /** @returns {void} */
  const apply = () => write(read());

  /**
   * @param {PointerEvent} e
   * @returns {void}
   */
  const onPointerDown = (e) => {
    const handle = e.target instanceof Element ? e.target.closest(selector) : null;
    if (!(handle instanceof HTMLElement)) return;
    const col = Number(handle.dataset.col);
    const widths = read();
    if (!Number.isInteger(col) || col < 0 || col >= widths.length) return;
    e.preventDefault();
    // Capture on the handle: moves and ups keep targeting it even off-window,
    // and a pointercancel is delivered here too so cleanup cannot be missed.
    handle.setPointerCapture(e.pointerId);
    const startX = e.clientX;
    const startW = /** @type {number} */ (widths[col]);
    /** @param {PointerEvent} ev */
    const onMove = (ev) => {
      widths[col] = Math.max(minFor(col), Math.round(startW + ev.clientX - startX));
      write(widths);
    };
    /** @param {PointerEvent} ev */
    const onUp = (ev) => {
      handle.removeEventListener("pointermove", onMove);
      handle.removeEventListener("pointerup", onUp);
      handle.removeEventListener("pointercancel", onUp);
      document.body.style.userSelect = "";
      if (ev.type === "pointercancel") apply(); // abandoned drag: revert to the persisted widths
      else store.set(/** @type {Partial<StoreState>} */ ({ [key]: widths }));
    };
    document.body.style.userSelect = "none";
    handle.addEventListener("pointermove", onMove);
    handle.addEventListener("pointerup", onUp);
    handle.addEventListener("pointercancel", onUp);
  };

  headEl.addEventListener("pointerdown", onPointerDown);
  apply();
  return { apply, dispose: () => headEl.removeEventListener("pointerdown", onPointerDown) };
}
