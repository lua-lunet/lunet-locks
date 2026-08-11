// Thin fetch client for the admin API (see ../../openapi.yaml).

import { config } from "./state.mjs";
import { logger } from "./log.mjs";

const log = logger("api");

/** @typedef {import("./types.mjs").BreakResponse} BreakResponse */
/** @typedef {import("./types.mjs").ClusterResponse} ClusterResponse */
/** @typedef {import("./types.mjs").EventsParams} EventsParams */
/** @typedef {import("./types.mjs").EventsResponse} EventsResponse */
/** @typedef {import("./types.mjs").HttpError} HttpError */
/** @typedef {import("./types.mjs").LockDetailResponse} LockDetailResponse */
/** @typedef {import("./types.mjs").LocksParams} LocksParams */
/** @typedef {import("./types.mjs").LocksResponse} LocksResponse */
/** @typedef {import("./types.mjs").SeriesParams} SeriesParams */
/** @typedef {import("./types.mjs").SeriesResponse} SeriesResponse */

/**
 * One request against the admin API. Non-2xx throws an Error whose `status`
 * carries the HTTP code (callers key off 404 in particular) and whose message
 * is the API's `error` field when there is one.
 * @template T
 * @param {"GET" | "POST"} method
 * @param {string} path Path below config.apiBase, e.g. "/locks/7/break".
 * @param {Record<string, string | number | null | undefined> | null} [params]
 * @param {unknown} [body]
 * @returns {Promise<T>}
 */
async function call(method, path, params, body) {
  const t0 = performance.now();
  const url = new URL(config.apiBase + path, location.origin);
  for (const [k, v] of Object.entries(params ?? {})) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
  }
  /** @type {Response} */
  let res;
  try {
    res = await fetch(url, {
      method,
      headers: body ? { "content-type": "application/json" } : undefined,
      body: body ? JSON.stringify(body) : undefined,
    });
  } catch (e) {
    log.severe(`${method} ${path} network error`, {
      method, path, elapsedMs: Math.round(performance.now() - t0), error: e,
    });
    throw e;
  }
  const elapsedMs = Math.round(performance.now() - t0);
  /** @type {any} */
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    log.warning(`${method} ${path} -> ${res.status}`, { method, path, status: res.status, elapsedMs });
    /** @type {HttpError} */
    const err = new Error(data.error ?? ("HTTP " + res.status));
    err.status = res.status;
    throw err;
  }
  log.fine(`${method} ${path} -> ${res.status}`, { method, path, status: res.status, elapsedMs });
  return data;
}

export const api = {
  /** @returns {Promise<ClusterResponse>} */
  cluster: () => call("GET", "/cluster"),
  /**
   * @param {LocksParams} [params]
   * @returns {Promise<LocksResponse>}
   */
  locks: (params) => call("GET", "/locks", params),
  /**
   * @param {number} id
   * @returns {Promise<LockDetailResponse>}
   */
  lock: (id) => call("GET", "/locks/" + id),
  /**
   * @param {number} id
   * @returns {Promise<BreakResponse>}
   */
  breakLock: (id) => call("POST", "/locks/" + id + "/break", null, { actor: "admin@console" }),
  /**
   * @param {EventsParams} [params]
   * @returns {Promise<EventsResponse>}
   */
  events: (params) => call("GET", "/events", params),
  /**
   * @param {SeriesParams} [params]
   * @returns {Promise<SeriesResponse>}
   */
  series: (params) => call("GET", "/metrics/series", params),
};
