// In-browser test harness, imported by app.mjs so it always exists when the
// debug panel does. It claims the panel's cancelable "la:run-tests" event
// (preventDefault, which silences the panel's not-loaded stub), runs every
// manifest suite sequentially, and streams results through the logger so
// they render in the panel's log stream:
//
//   INFO    per suite, with its case count        (logger "test.<suite>")
//   FINE    per passing case
//   SEVERE  per failure, with actual vs expected
//   INFO    final summary: "n passed, m failed, tms"  (logger "test")
//
// A throw inside one case — or one suite module failing to import — never
// aborts the run. After the summary an "la:test-summary" CustomEvent tells
// the panel the outcome so it can pin it on the Run tests button.

import { AssertionError } from "../tests/assert.mjs";
import { manifest } from "../tests/manifest.mjs";
import { logger } from "../lib/log.mjs";

const log = logger("test");

/**
 * @typedef {object} RunSummary
 * @property {number} passed
 * @property {number} failed
 * @property {number} ms
 */

let running = false;

/**
 * Run every suite in manifest order. Reentrant clicks log a warning and
 * return a zero summary rather than stacking a second run.
 * @returns {Promise<RunSummary>}
 */
export async function run() {
  if (running) {
    log.warning("test run already in progress");
    return { passed: 0, failed: 0, ms: 0 };
  }
  running = true;
  const t0 = performance.now();
  let passed = 0;
  let failed = 0;
  try {
    for (const entry of manifest) {
      /** @type {import("../tests/assert.mjs").Suite} */
      let suite;
      try {
        suite = (await entry.load()).suite;
      } catch (e) {
        failed++;
        log.severe(`suite ${entry.file}: module failed to load`, { error: e });
        continue;
      }
      const tlog = logger("test." + suite.name);
      const names = Object.keys(suite.cases);
      tlog.info(`suite ${suite.name}: ${names.length} case(s)`);
      for (const name of names) {
        try {
          await suite.cases[name]();
          passed++;
          tlog.fine(`pass › ${name}`);
        } catch (e) {
          failed++;
          if (e instanceof AssertionError) {
            tlog.severe(`FAIL › ${name}: ${e.message}`, { actual: e.actual, expected: e.expected });
          } else {
            tlog.severe(`FAIL › ${name}: threw ${e instanceof Error ? e.message : String(e)}`, { error: e });
          }
        }
      }
    }
  } finally {
    running = false;
  }
  const ms = Math.round(performance.now() - t0);
  /** @type {RunSummary} */
  const summary = { passed, failed, ms };
  log.info(`test run: ${passed} passed, ${failed} failed, ${ms}ms`, summary);
  window.dispatchEvent(new CustomEvent("la:test-summary", { detail: summary }));
  return summary;
}

window.addEventListener("la:run-tests", (e) => {
  e.preventDefault(); // claim the run; the panel's stub WARNING stays silent
  void run();
});
