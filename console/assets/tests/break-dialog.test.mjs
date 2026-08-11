// DOM smoke: la-break-dialog confirm gating — the Break button stays
// disabled until the input matches the lock's leaf name exactly, and Cancel
// clears the pending confirmation. Confirm is never clicked: that would
// call the API, and these suites make no network requests.

import { store } from "../lib/state.mjs";
import "../components/la-break-dialog.mjs";
import { AssertionError, assertEqual, preserveSession } from "./assert.mjs";
import { NOW, lockFixture, mount, preserveStore } from "./fixtures.mjs";

/**
 * Open the dialog for the fixture lock and run fn against it.
 * @param {(el: HTMLElement) => void} fn
 * @returns {void}
 */
function withDialog(fn) {
  const restoreSession = preserveSession("lock-admin");
  const restoreStore = preserveStore("confirmId", "locksAll", "detail", "now");
  try {
    store.set({ confirmId: 1, detail: null, now: NOW, locksAll: [lockFixture({ id: 1, name: "/tenants/acme" })] });
    const { host, el } = mount("la-break-dialog");
    try {
      fn(el);
    } finally {
      host.remove();
    }
  } finally {
    restoreStore();
    restoreSession();
  }
}

/**
 * The dialog's input and confirm button, or an assertion failure.
 * @param {HTMLElement} el
 * @returns {{ input: HTMLInputElement, confirmBtn: HTMLButtonElement }}
 */
function dialogParts(el) {
  const input = /** @type {HTMLInputElement | null} */ (el.querySelector("#la-confirm"));
  const confirmBtn = /** @type {HTMLButtonElement | null} */ (el.querySelector("[data-act=confirm]"));
  if (!input || !confirmBtn) {
    throw new AssertionError("dialog did not render its input and confirm button", el.innerHTML.length, "dialog markup");
  }
  return { input, confirmBtn };
}

/** @type {import("./assert.mjs").Suite} */
export const suite = {
  name: "break-dialog",
  cases: {
    "confirm stays disabled until the leaf name is typed"() {
      withDialog((el) => {
        const { input, confirmBtn } = dialogParts(el);
        assertEqual(confirmBtn.disabled, true, "disabled before typing");
        input.value = "acm";
        input.dispatchEvent(new Event("input"));
        assertEqual(confirmBtn.disabled, true, "partial leaf stays disabled");
        input.value = "acmex";
        input.dispatchEvent(new Event("input"));
        assertEqual(confirmBtn.disabled, true, "wrong leaf stays disabled");
        input.value = " acme ";
        input.dispatchEvent(new Event("input"));
        assertEqual(confirmBtn.disabled, false, "whitespace is trimmed to a match");
        input.value = "acme";
        input.dispatchEvent(new Event("input"));
        assertEqual(confirmBtn.disabled, false, "exact leaf enables confirm");
      });
    },
    "cancel clears the pending confirmation"() {
      withDialog((el) => {
        const cancelBtn = /** @type {HTMLButtonElement | null} */ (el.querySelector("[data-act=cancel]"));
        if (!cancelBtn) throw new AssertionError("dialog did not render its cancel button", el.innerHTML.length, "dialog markup");
        cancelBtn.click();
        assertEqual(store.state.confirmId, null, "confirmId cleared");
        assertEqual(el.querySelector(".dialog"), null, "dialog unrendered");
      });
    },
  },
};
