// Frameworkless assertions shared by every suite. A failure throws an
// AssertionError carrying the actual/expected values; la-test-harness.mjs
// catches it and reports — assertions never log themselves.

/** An assertion failure with the values the runner should display. */
export class AssertionError extends Error {
  /**
   * @param {string} message
   * @param {unknown} actual
   * @param {unknown} expected
   */
  constructor(message, actual, expected) {
    super(message);
    this.name = "AssertionError";
    this.actual = actual;
    this.expected = expected;
  }
}

/**
 * A named bundle of cases. The harness runs cases sequentially in object
 * order; a throw fails only that case, never the run.
 * @typedef {object} Suite
 * @property {string} name
 * @property {Record<string, () => void | Promise<void>>} cases
 */

/**
 * @param {unknown} v
 * @returns {string}
 */
function show(v) {
  if (typeof v === "string") return JSON.stringify(v);
  if (v instanceof Set) return `Set(${v.size}) { ${[...v].map(show).join(", ")} }`;
  try {
    return JSON.stringify(v) ?? String(v);
  } catch {
    return String(v);
  }
}

/**
 * First structural mismatch between a and b, or null when deep-equal.
 * Handles primitives, arrays, plain objects and Sets — the shapes the
 * fixtures and the store use.
 * @param {unknown} a
 * @param {unknown} b
 * @param {string} path
 * @returns {string | null}
 */
function diff(a, b, path) {
  if (Object.is(a, b)) return null;
  if (a instanceof Set && b instanceof Set) {
    if (a.size !== b.size) return `${path}: Set size ${a.size} != ${b.size}`;
    for (const v of a) if (!b.has(v)) return `${path}: missing ${show(v)}`;
    return null;
  }
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return `${path}: length ${a.length} != ${b.length}`;
    for (let i = 0; i < a.length; i++) {
      const d = diff(a[i], b[i], `${path}[${i}]`);
      if (d) return d;
    }
    return null;
  }
  if (a !== null && b !== null && typeof a === "object" && typeof b === "object") {
    const oa = /** @type {Record<string, unknown>} */ (a);
    const ob = /** @type {Record<string, unknown>} */ (b);
    const ka = Object.keys(oa);
    const kb = Object.keys(ob);
    if (ka.length !== kb.length) return `${path}: keys [${ka}] != [${kb}]`;
    for (const k of ka) {
      if (!Object.prototype.hasOwnProperty.call(ob, k)) return `${path}: missing key ${k}`;
      const d = diff(oa[k], ob[k], path ? `${path}.${k}` : k);
      if (d) return d;
    }
    return null;
  }
  return `${path || "value"}: ${show(a)} != ${show(b)}`;
}

/**
 * Object.is equality (NaN equals NaN; -0 does not equal 0).
 * @param {unknown} actual
 * @param {unknown} expected
 * @param {string} [msg]
 * @returns {void}
 */
export function assertEqual(actual, expected, msg) {
  if (!Object.is(actual, expected)) {
    throw new AssertionError(msg ?? `expected ${show(expected)}, got ${show(actual)}`, actual, expected);
  }
}

/**
 * Structural equality for plain data (arrays, objects, Sets, primitives).
 * @param {unknown} actual
 * @param {unknown} expected
 * @param {string} [msg]
 * @returns {void}
 */
export function assertDeepEqual(actual, expected, msg) {
  const d = diff(actual, expected, "");
  if (d) throw new AssertionError(msg ?? d, actual, expected);
}

/**
 * @param {() => void} fn
 * @param {string} [msg]
 * @returns {unknown} the thrown error, for further assertions on it
 */
export function assertThrows(fn, msg) {
  try {
    fn();
  } catch (e) {
    return e;
  }
  throw new AssertionError(msg ?? "expected the function to throw, but it returned", undefined, "throw");
}

/**
 * @param {number} actual
 * @param {number} expected
 * @param {number} epsilon
 * @param {string} [msg]
 * @returns {void}
 */
export function assertClose(actual, expected, epsilon, msg) {
  if (!(Math.abs(actual - expected) <= epsilon)) {
    throw new AssertionError(msg ?? `expected ${actual} within ${epsilon} of ${expected}`, actual, expected);
  }
}

/**
 * Snapshot sessionStorage keys and return a function that restores them
 * exactly (missing keys are removed again). Suites wrap their mutations in
 * try/finally so a run leaves the console's persisted state untouched and a
 * second run sees the same world as the first.
 * @param {...string} keys
 * @returns {() => void}
 */
export function preserveSession(...keys) {
  /** @type {[string, string | null][]} */
  const snap = keys.map((k) => [k, sessionStorage.getItem(k)]);
  return () => {
    for (const [k, v] of snap) {
      try {
        if (v === null) sessionStorage.removeItem(k);
        else sessionStorage.setItem(k, v);
      } catch { /* storage unavailable: nothing to restore */ }
    }
  };
}
