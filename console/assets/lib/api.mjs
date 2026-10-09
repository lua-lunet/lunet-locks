// Thin fetch client for the admin API (see ../../openapi.yaml).

import { config, isBridge } from "./state.mjs";

/** @typedef {import("./types.mjs").Cluster} Cluster */
/** @typedef {import("./types.mjs").EventsParams} EventsParams */
/** @typedef {import("./types.mjs").EventsResponse} EventsResponse */
/** @typedef {import("./types.mjs").HttpError} HttpError */
/** @typedef {import("./types.mjs").LockDetailResponse} LockDetailResponse */
/** @typedef {import("./types.mjs").LocksParams} LocksParams */
/** @typedef {import("./types.mjs").LocksResponse} LocksResponse */
/** @typedef {import("./types.mjs").BreakResponse} BreakResponse */
/** @typedef {import("./types.mjs").SeriesParams} SeriesParams */
/** @typedef {import("./types.mjs").SeriesResponse} SeriesResponse */
/** @typedef {import("./types.mjs").TelemetryLogParams} TelemetryLogParams */
/** @typedef {import("./types.mjs").TelemetryLogResponse} TelemetryLogResponse */

// The data source decides the base: the nginx edge's /api/v1 (mock or live
// cluster behind it) or the aof-console-bridge's absolute URL. Both speak
// the same OpenAPI shapes.
function base() {
  return isBridge ? config.bridgeBase : config.apiBase;
}

/**
 * @param {string} method
 * @param {string} path
 * @param {Record<string, string | number | null | undefined> | null} [params]
 * @param {object | null} [body]
 * @returns {Promise<any>}
 */
async function call(method, path, params, body) {
  const url = new URL(base() + path, location.origin);
  for (const [k, v] of Object.entries(params ?? {})) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
  }
  const res = await fetch(url, {
    method,
    headers: body ? { "content-type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    // The error carries the HTTP status so callers can distinguish a genuine
    // 404 from a transient failure. The expando rides the response only.
    const err = /** @type {HttpError} */ (Object.assign(
      new Error(data.error ?? ("HTTP " + res.status)),
      { status: res.status },
    ));
    throw err;
  }
  return data;
}

export const api = {
  /** @returns {Promise<Cluster>} */
  cluster: () => call("GET", "/cluster"),
  /** @param {LocksParams} params @returns {Promise<LocksResponse>} */
  locks: (params) => call("GET", "/locks", params),
  /** @param {number} id @returns {Promise<LockDetailResponse>} */
  lock: (id) => call("GET", "/locks/" + id),
  /** @param {number} id @returns {Promise<BreakResponse>} */
  breakLock: (id) => call("POST", "/locks/" + id + "/break", null, { actor: "admin@console" }),
  /** @param {EventsParams} params @returns {Promise<EventsResponse>} */
  events: (params) => call("GET", "/events", params),
  /** @param {SeriesParams} params @returns {Promise<SeriesResponse>} */
  series: (params) => call("GET", "/metrics/series", params),
  // The telemetry panel's data source: the observability contract's JSON
  // log series (the named protocol events plus the tape's slot-frontier
  // records). The mock and the aof-console-bridge serve the same shape.
  /** @param {TelemetryLogParams} params @returns {Promise<TelemetryLogResponse>} */
  telemetryLog: (params) => call("GET", "/telemetry/log", params),
};
