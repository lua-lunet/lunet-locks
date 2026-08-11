// java.util.logging-style shim. Every record goes to BOTH the native console
// (gated by the sessionStorage threshold `lock-admin-log-level`, default INFO)
// AND straight onto BroadcastChannel("lock-admin-log") — ungated, because the
// log panel's worker does its own filtering and must see every level. There is
// deliberately no ring buffer: records are streamed and forgotten.
//
// Fault capture: window "error" and "unhandledrejection" arrive as SEVERE, and
// console.error is wrapped so third-party throws (ECharts from the CDN) also
// reach the channel. The console mirror below calls the ORIGINAL console
// methods captured before wrapping, so a mirrored SEVERE never re-enters the
// wrapper: no recursion, and a direct console.error("x") posts exactly once.

/** @typedef {import("./types.mjs").LogRecord} LogRecord */

/**
 * Numeric levels, java.util.logging values exactly. OFF is Infinity so no
 * record ever reaches it; ALL is 0 so every record does.
 * @type {Record<string, number>}
 */
export const Level = Object.freeze({
  OFF: Infinity,
  SEVERE: 1000,
  WARNING: 900,
  INFO: 800,
  CONFIG: 700,
  FINE: 500,
  FINER: 400,
  FINEST: 300,
  ALL: 0,
});

const LEVEL_KEY = "lock-admin-log-level";

/**
 * Current console-mirror threshold, read fresh on every emit so a setLevel()
 * from the panel takes effect immediately with no emitter coordination.
 * sessionStorage can throw in locked-down contexts; fall back to INFO.
 * @returns {number}
 */
export function getLevel() {
  try {
    const name = (sessionStorage.getItem(LEVEL_KEY) ?? "INFO").toUpperCase();
    return Level[name] ?? Level.INFO;
  } catch {
    return Level.INFO;
  }
}

/**
 * Persist a new console-mirror threshold ("SEVERE" … "ALL"). Unknown names
 * are ignored with a warning so a typo never silently locks the console out.
 * @param {string} name
 * @returns {void}
 */
export function setLevel(name) {
  const upper = String(name).toUpperCase();
  if (Level[upper] === undefined) {
    emit(Level.WARNING, "log", `unknown level ignored: ${name}`);
    return;
  }
  try {
    sessionStorage.setItem(LEVEL_KEY, upper);
  } catch { /* mirroring just stays at the old threshold */ }
}

// The channel is a nice-to-have: if BroadcastChannel is missing or its
// constructor throws, console mirroring alone keeps working.
/** @type {BroadcastChannel | null} */
let channel = null;
try {
  if (typeof BroadcastChannel !== "undefined") channel = new BroadcastChannel("lock-admin-log");
} catch {
  channel = null;
}

// Captured BEFORE the console.error wrapper is installed. The mirror must use
// these, never the live `console.*`, or a mirrored SEVERE would re-enter the
// wrapper and double-post (or worse).
const native = {
  error: console.error.bind(console),
  warn: console.warn.bind(console),
  log: console.log.bind(console),
  debug: console.debug.bind(console),
};

/**
 * Reverse lookup for the level name; unknown numerics stringify.
 * @param {number} level
 * @returns {string}
 */
function nameOf(level) {
  for (const [k, v] of Object.entries(Level)) if (v === level) return k;
  return String(level);
}

const MAX_DEPTH = 6;

/**
 * Reduce `meta` to something structured-clone-safe: Errors become
 * { name, message, stack, status }, DOM nodes and functions are dropped or
 * stringified, bigints stringify, and deep/cyclic graphs are cut at MAX_DEPTH
 * so postMessage can never raise DataCloneError.
 * @param {unknown} value
 * @param {number} [depth]
 * @returns {unknown}
 */
function sanitize(value, depth = 0) {
  if (value === null || value === undefined) return value;
  const t = typeof value;
  if (t === "string" || t === "number" || t === "boolean") return value;
  if (t === "bigint") return String(value);
  if (value instanceof Error) {
    return {
      name: value.name,
      message: value.message,
      stack: value.stack,
      status: /** @type {{ status?: unknown }} */ (value).status,
    };
  }
  if (depth >= MAX_DEPTH) return "[...]";
  if (Array.isArray(value)) return value.map((v) => sanitize(v, depth + 1));
  if (t === "object") {
    if (typeof Node !== "undefined" && value instanceof Node) return "[DOM node]";
    /** @type {Record<string, unknown>} */
    const out = {};
    for (const [k, v] of Object.entries(/** @type {Record<string, unknown>} */ (value))) {
      if (typeof v === "function") continue;
      out[k] = sanitize(v, depth + 1);
    }
    return out;
  }
  return String(value); // functions, symbols
}

/**
 * The one sink every path funnels through. Channel post is UNGATED; the
 * native-console mirror is gated by getLevel().
 * @param {number} level
 * @param {string} loggerName
 * @param {string} msg
 * @param {unknown} [meta]
 * @returns {void}
 */
function emit(level, loggerName, msg, meta) {
  /** @type {LogRecord} */
  const rec = {
    ts: Date.now(),
    level,
    levelName: nameOf(level),
    logger: loggerName,
    msg: String(msg),
  };
  const clean = sanitize(meta);
  if (clean !== undefined) rec.meta = clean;
  if (channel) {
    try {
      channel.postMessage(rec);
    } catch { /* never let logging break the app */ }
  }
  if (level < getLevel() || level === Level.OFF) return;
  const line = `[${rec.levelName}] ${rec.logger}: ${rec.msg}`;
  const args = rec.meta === undefined ? [line] : [line, rec.meta];
  if (level >= Level.SEVERE) native.error(...args);
  else if (level >= Level.WARNING) native.warn(...args);
  else if (level >= Level.CONFIG) native.log(...args);
  else native.debug(...args);
}

/**
 * A named logger. Names are dotted module names: "api", "store", "app", …
 * @typedef {object} Logger
 * @property {(msg: string, meta?: unknown) => void} severe
 * @property {(msg: string, meta?: unknown) => void} warning
 * @property {(msg: string, meta?: unknown) => void} info
 * @property {(msg: string, meta?: unknown) => void} config
 * @property {(msg: string, meta?: unknown) => void} fine
 * @property {(msg: string, meta?: unknown) => void} finer
 * @property {(msg: string, meta?: unknown) => void} finest
 * @property {(level: number, msg: string, meta?: unknown) => void} log
 */

/**
 * @param {string} name Dotted module name, e.g. "api".
 * @returns {Logger}
 */
export function logger(name) {
  return {
    severe: (msg, meta) => emit(Level.SEVERE, name, msg, meta),
    warning: (msg, meta) => emit(Level.WARNING, name, msg, meta),
    info: (msg, meta) => emit(Level.INFO, name, msg, meta),
    config: (msg, meta) => emit(Level.CONFIG, name, msg, meta),
    fine: (msg, meta) => emit(Level.FINE, name, msg, meta),
    finer: (msg, meta) => emit(Level.FINER, name, msg, meta),
    finest: (msg, meta) => emit(Level.FINEST, name, msg, meta),
    log: (level, msg, meta) => emit(level, name, msg, meta),
  };
}

// --- Fault capture (all SEVERE, all ungated onto the channel) -------------

// 1. Synchronous throws that escape to the top (e.g. a throw in setTimeout).
window.addEventListener("error", (ev) => {
  emit(Level.SEVERE, "window", ev.message, {
    source: ev.filename,
    line: ev.lineno,
    col: ev.colno,
    error: ev.error,
  });
});

// 2. Rejected promises nobody caught.
window.addEventListener("unhandledrejection", (ev) => {
  emit(Level.SEVERE, "promise", "unhandled rejection", { reason: ev.reason });
});

// 3. Direct console.error calls — this is how CDN scripts (ECharts) surface
// their failures. Call the ORIGINAL first so the native console sees the call
// exactly as before, then post straight to the channel via emit; emit's own
// mirror uses the captured `native.error`, not this wrapper, so the record
// cannot loop back in here.
console.error = (...args) => {
  native.error(...args);
  const msg = args
    .map((a) => (typeof a === "string" ? a : String(a instanceof Error ? a.message : a)))
    .join(" ");
  emit(Level.SEVERE, "console", msg || "console.error", { args });
};
