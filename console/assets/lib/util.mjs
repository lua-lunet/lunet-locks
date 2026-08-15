// Small pure helpers shared by the components.

/** @type {Record<string, string>} */
const ESC_MAP = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };

/**
 * HTML-escape a value for interpolation into a template string. Nullish
 * becomes "" so `esc(lock.holder)` is safe on a free lock.
 * @param {unknown} s
 * @returns {string}
 */
export const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ESC_MAP[c]);

/**
 * @param {number} n
 * @returns {string}
 */
const pad = (n) => String(n).padStart(2, "0");

/**
 * Epoch ms → local "HH:MM:SS".
 * @param {number} ms
 * @returns {string}
 */
export function fmtClock(ms) {
  const d = new Date(ms);
  return pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
}

/**
 * A duration in ms → a compact "12s" / "3m 4s" / "1h 20m".
 * @param {number} ms
 * @returns {string}
 */
export function fmtDur(ms) {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s / 60) + "m " + (s % 60) + "s";
  return Math.floor(s / 3600) + "h " + Math.floor((s % 3600) / 60) + "m";
}

/**
 * "HH:MM[:SS]" → epoch ms on the same day as baseMs; null when unparsable.
 * @param {string | null | undefined} text
 * @param {number} baseMs
 * @returns {number | null}
 */
export function parseClock(text, baseMs) {
  const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec((text ?? "").trim());
  if (!m) return null;
  const d = new Date(baseMs);
  d.setHours(+m[1], +m[2], +(m[3] ?? 0), 0);
  return d.getTime();
}

/**
 * Trailing-edge debounce that keeps the wrapped function's argument list.
 * @template {unknown[]} A
 * @param {(...args: A) => void} fn
 * @param {number} ms
 * @returns {(...args: A) => void}
 */
export function debounce(fn, ms) {
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

/** Inline SVG glyphs, pre-escaped and safe to interpolate. */
export const ICONS = {
  lock: '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><rect x="3.2" y="7" width="9.6" height="6.6" rx="1.6"/><path d="M5.6 7V5a2.4 2.4 0 0 1 4.8 0v2"/></svg>',
  lockOpen: '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><rect x="3.2" y="7" width="9.6" height="6.6" rx="1.6"/><path d="M5.6 7V5a2.4 2.4 0 0 1 4.8 0"/></svg>',
  bell: '<svg width="11" height="11" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><path d="M4 11.5V7a4 4 0 0 1 8 0v4.5H4Z"/><path d="M6.6 13.4a1.6 1.6 0 0 0 2.8 0"/></svg>',
  search: '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="7" cy="7" r="4.2"/><path d="M10.2 10.2 14 14"/></svg>',
  close: '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M4 4l8 8M12 4l-8 8"/></svg>',
};
