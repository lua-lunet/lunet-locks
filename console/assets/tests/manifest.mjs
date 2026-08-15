// The ordered suite list — adding a suite is one line here. The harness
// (../components/la-test-harness.mjs) loads each entry lazily, so a suite
// module that fails to import fails its own entry with a SEVERE instead of
// taking down the whole run.

/** @typedef {import("./assert.mjs").Suite} Suite */

/**
 * @typedef {object} SuiteEntry
 * @property {string} file Short name for logs if the module fails to load.
 * @property {() => Promise<{ suite: Suite }>} load
 */

/** @type {SuiteEntry[]} */
export const manifest = [
  { file: "util", load: () => import("./util.test.mjs") },
  { file: "state", load: () => import("./state.test.mjs") },
  { file: "resizable", load: () => import("./resizable.test.mjs") },
  { file: "api", load: () => import("./api.test.mjs") },
  { file: "lock-table", load: () => import("./lock-table.test.mjs") },
  { file: "log-view", load: () => import("./log-view.test.mjs") },
  { file: "detail", load: () => import("./detail.test.mjs") },
  { file: "tree", load: () => import("./tree.test.mjs") },
  { file: "break-dialog", load: () => import("./break-dialog.test.mjs") },
];
