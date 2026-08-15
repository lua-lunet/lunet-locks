// Debug-panel log worker. Subscribes to BroadcastChannel("lock-admin-log")
// (posted UNGATED by lib/log.mjs), owns ALL filtering and formatting, and
// posts batched lines back to <la-debug-panel>. The main thread only appends.
//
//   log.mjs → BroadcastChannel → here (filter · format · batch)
//             → postMessage({ lines, dropped }) → la-debug-panel
//
// There is deliberately no ring buffer: at most the current batch is held.
//
// Why this file does NOT import lib/log.mjs: log.mjs touches `window` at
// module evaluation (fault-capture listeners), which throws in a worker.
// The numeric level table is therefore mirrored here — java.util.logging
// values exactly, same as Level in lib/log.mjs; keep the two in sync.

/** @typedef {import("./lib/types.mjs").LogRecord} LogRecord */

/**
 * Control messages accepted from the panel.
 * @typedef {object} LogControl
 * @property {"level" | "filter" | "clear"} type
 * @property {string} [value] Level name for "level", substring for "filter".
 */

/**
 * The one message shape posted back to the panel.
 * @typedef {object} LogBatch
 * @property {string[]} lines Formatted "HH:MM:SS.mmm  LEVEL  logger  msg [meta]".
 * @property {number} dropped Records dropped by the per-tick batch cap.
 */

/** java.util.logging numerics, mirrored from lib/log.mjs (see header note).
 * @type {Record<string, number>} */
const Level = Object.freeze({
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

/** One flush at most this often; records accumulate between flushes. */
const BATCH_MS = 100;
/** Per-tick flood guard: beyond this, records are counted as dropped. */
const BATCH_CAP = 500;

/** Numeric threshold; a record passes when rec.level >= threshold. */
let threshold = Level.INFO;
/** Case-insensitive substring over `logger msg`; empty disables. */
let filter = "";
/** @type {string[]} */
let batch = [];
let dropped = 0;
/** @type {ReturnType<typeof setTimeout> | null} */
let timer = null;

// `self` in a worker is a WorkerGlobalScope, but tsconfig uses lib dom only
// (lib webworker conflicts with it), so `self` types as Window. Narrow it to
// exactly the two members this worker uses instead of silencing the checker.
/**
 * @typedef {object} WorkerScope
 * @property {(msg: LogBatch) => void} postMessage
 * @property {((ev: MessageEvent) => void) | null} onmessage
 */
const scope = /** @type {WorkerScope} */ (/** @type {unknown} */ (self));

/**
 * @param {number} n
 * @param {number} width
 * @returns {string}
 */
const pad = (n, width) => String(n).padStart(width, "0");

/**
 * HH:MM:SS.mmm in local time.
 * @param {number} ts Epoch ms.
 * @returns {string}
 */
function fmtTime(ts) {
  const d = new Date(ts);
  return `${pad(d.getHours(), 2)}:${pad(d.getMinutes(), 2)}:${pad(d.getSeconds(), 2)}.${pad(d.getMilliseconds(), 3)}`;
}

/**
 * Compact one-line meta tail. meta is already sanitised by log.mjs
 * (structured-clone-safe), so stringify can only fail on something exotic;
 * fall back to String rather than lose the record.
 * @param {unknown} meta
 * @returns {string}
 */
function fmtMeta(meta) {
  if (meta === undefined) return "";
  try {
    return "  " + JSON.stringify(meta);
  } catch {
    return "  " + String(meta);
  }
}

/**
 * @param {LogRecord} rec
 * @returns {string}
 */
function format(rec) {
  return `${fmtTime(rec.ts)}  ${rec.levelName}  ${rec.logger}  ${rec.msg}${fmtMeta(rec.meta)}`;
}

/** Post the accumulated batch, if any, and reset the accumulator. */
function flush() {
  timer = null;
  if (batch.length === 0 && dropped === 0) return;
  const lines = batch;
  if (dropped > 0) lines.push(`… ${dropped} line${dropped === 1 ? "" : "s"} dropped (batch cap ${BATCH_CAP})`);
  batch = [];
  const n = dropped;
  dropped = 0;
  scope.postMessage({ lines, dropped: n });
}

const channel = new BroadcastChannel("lock-admin-log");
channel.onmessage = (/** @type {MessageEvent} */ ev) => {
  const rec = /** @type {LogRecord} */ (ev.data);
  // Threshold: ALL=0 passes everything, OFF=Infinity passes nothing.
  if (rec.level < threshold) return;
  if (filter && !(rec.logger + " " + rec.msg).toLowerCase().includes(filter)) return;
  if (batch.length >= BATCH_CAP) {
    dropped++;
  } else {
    batch.push(format(rec));
  }
  if (timer === null) timer = setTimeout(flush, BATCH_MS);
};

// Panel control messages: threshold and filter apply live to the stream;
// "clear" drops any unflushed batch so stale lines cannot appear after the
// panel has emptied its view.
scope.onmessage = (/** @type {MessageEvent} */ ev) => {
  const msg = /** @type {LogControl} */ (ev.data);
  if (!msg || typeof msg !== "object") return;
  if (msg.type === "level") {
    const v = Level[String(msg.value ?? "").toUpperCase()];
    if (v !== undefined) threshold = v;
  } else if (msg.type === "filter") {
    filter = String(msg.value ?? "").toLowerCase();
  } else if (msg.type === "clear") {
    batch = [];
    dropped = 0;
  }
};
