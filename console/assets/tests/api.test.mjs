// URL/query building in lib/api.mjs against a stubbed globalThis.fetch —
// no case touches the network, and a failing stub still restores the real
// fetch in its finally. The last case proves the restore happened.

import { api } from "../lib/api.mjs";
import { config } from "../lib/state.mjs";
import { AssertionError, assertDeepEqual, assertEqual } from "./assert.mjs";

const REAL_FETCH = globalThis.fetch;

/**
 * @typedef {object} FetchCall
 * @property {string} url Absolute URL the api client built.
 * @property {RequestInit | undefined} init
 */

/**
 * Install a fetch stub that records calls and answers with a fixed
 * status/body. `restore()` puts the real fetch back.
 * @param {number} status
 * @param {unknown} body
 * @returns {{ calls: FetchCall[], restore: () => void }}
 */
function stubFetch(status, body) {
  /** @type {FetchCall[]} */
  const calls = [];
  globalThis.fetch = /** @type {typeof fetch} */ (/** @type {unknown} */ (
    async (/** @type {unknown} */ url, /** @type {RequestInit | undefined} */ init) => {
      calls.push({ url: String(url), init });
      return { ok: status >= 200 && status < 300, status, json: async () => body };
    }
  ));
  return { calls, restore: () => { globalThis.fetch = REAL_FETCH; } };
}

/**
 * @param {FetchCall} call
 * @returns {URL}
 */
function urlOf(call) {
  return new URL(call.url);
}

/**
 * Await fn and return the error it threw (null if it returned normally).
 * @param {() => Promise<unknown>} fn
 * @returns {Promise<unknown>}
 */
async function catchErr(fn) {
  try {
    await fn();
    return null;
  } catch (e) {
    return e;
  }
}

/** @type {import("./assert.mjs").Suite} */
export const suite = {
  name: "api",
  cases: {
    async "cluster builds GET <apiBase>/cluster"() {
      const s = stubFetch(200, { nowMs: 1, nodes: [] });
      try {
        await api.cluster();
        assertEqual(s.calls.length, 1);
        assertEqual(urlOf(s.calls[0]).pathname, config.apiBase + "/cluster");
        assertEqual(s.calls[0].init?.method, "GET");
        assertEqual(s.calls[0].init?.body, undefined);
      } finally { s.restore(); }
    },
    async "locks serialises every query param"() {
      const s = stubFetch(200, { nowMs: 1, locks: [] });
      try {
        await api.locks({ q: "tag:prod", state: "held", expiringAtMs: 123, toleranceMs: 5000 });
        const u = urlOf(s.calls[0]);
        assertEqual(u.pathname, config.apiBase + "/locks");
        assertEqual(u.searchParams.get("q"), "tag:prod");
        assertEqual(u.searchParams.get("state"), "held");
        assertEqual(u.searchParams.get("expiringAtMs"), "123");
        assertEqual(u.searchParams.get("toleranceMs"), "5000");
      } finally { s.restore(); }
    },
    async "locks drops empty and absent params"() {
      const s = stubFetch(200, { nowMs: 1, locks: [] });
      try {
        await api.locks({ q: "" });
        assertEqual(urlOf(s.calls[0]).search, "", "empty q is not sent");
        await api.locks();
        assertEqual(urlOf(s.calls[1]).search, "", "no params, no query string");
      } finally { s.restore(); }
    },
    async "lock(id) builds the member path"() {
      const s = stubFetch(200, { lock: {}, recentEvents: [] });
      try {
        await api.lock(7);
        assertEqual(urlOf(s.calls[0]).pathname, config.apiBase + "/locks/7");
      } finally { s.restore(); }
    },
    async "breakLock posts the actor body as JSON"() {
      const s = stubFetch(200, { lock: {}, event: {} });
      try {
        await api.breakLock(7);
        const c = s.calls[0];
        assertEqual(urlOf(c).pathname, config.apiBase + "/locks/7/break");
        assertEqual(c.init?.method, "POST");
        assertEqual(/** @type {Record<string, string>} */ (/** @type {unknown} */ (c.init?.headers))["content-type"], "application/json");
        assertDeepEqual(JSON.parse(String(c.init?.body)), { actor: "admin@console" });
      } finally { s.restore(); }
    },
    async "events builds the range query and drops nulls"() {
      const s = stubFetch(200, { events: [] });
      try {
        await api.events({ fromMs: 111, toMs: 222, lockId: 7, kind: "break", q: "x", limit: 300 });
        const u = urlOf(s.calls[0]);
        assertEqual(u.pathname, config.apiBase + "/events");
        assertEqual(u.searchParams.get("fromMs"), "111");
        assertEqual(u.searchParams.get("toMs"), "222");
        assertEqual(u.searchParams.get("lockId"), "7");
        assertEqual(u.searchParams.get("kind"), "break");
        assertEqual(u.searchParams.get("q"), "x");
        assertEqual(u.searchParams.get("limit"), "300");
        await api.events({ fromMs: null, toMs: null, limit: 50 });
        const u2 = urlOf(s.calls[1]);
        assertEqual(u2.searchParams.has("fromMs"), false, "null fromMs is not sent");
        assertEqual(u2.searchParams.has("toMs"), false, "null toMs is not sent");
        assertEqual(u2.searchParams.get("limit"), "50");
      } finally { s.restore(); }
    },
    async "series builds the window query"() {
      const s = stubFetch(200, { bucketMs: 60000, buckets: [] });
      try {
        await api.series({ fromMs: 1, toMs: 2, bucketMs: 60000 });
        const u = urlOf(s.calls[0]);
        assertEqual(u.pathname, config.apiBase + "/metrics/series");
        assertEqual(u.searchParams.get("fromMs"), "1");
        assertEqual(u.searchParams.get("toMs"), "2");
        assertEqual(u.searchParams.get("bucketMs"), "60000");
      } finally { s.restore(); }
    },
    async "a non-2xx sets err.status and uses the api message"() {
      const s = stubFetch(404, { error: "no such lock" });
      try {
        const err = await catchErr(() => api.lock(999));
        if (err === null) throw new AssertionError("api.lock did not throw on 404", null, "throw");
        assertEqual(/** @type {{ status?: number }} */ (err).status, 404);
        assertEqual(/** @type {Error} */ (err).message, "no such lock");
      } finally { s.restore(); }
    },
    async "a non-2xx without an error field falls back to the status"() {
      const s = stubFetch(500, {});
      try {
        const err = await catchErr(() => api.cluster());
        if (err === null) throw new AssertionError("api.cluster did not throw on 500", null, "throw");
        assertEqual(/** @type {{ status?: number }} */ (err).status, 500);
        assertEqual(/** @type {Error} */ (err).message, "HTTP 500");
      } finally { s.restore(); }
    },
    "fetch is restored after the stubbed cases"() {
      assertEqual(globalThis.fetch, REAL_FETCH, "globalThis.fetch left stubbed");
    },
  },
};
