// Thin fetch client for the admin API (see ../../openapi.yaml).

import { config, isBridge } from "./state.mjs";

// The data source decides the base: the nginx edge's /api/v1 (mock or live
// cluster behind it) or the aof-console-bridge's absolute URL. Both speak
// the same OpenAPI shapes.
function base() {
  return isBridge ? config.bridgeBase : config.apiBase;
}

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
    const err = new Error(data.error ?? ("HTTP " + res.status));
    err.status = res.status;
    throw err;
  }
  return data;
}

export const api = {
  cluster: () => call("GET", "/cluster"),
  locks: (params) => call("GET", "/locks", params),
  lock: (id) => call("GET", "/locks/" + id),
  breakLock: (id) => call("POST", "/locks/" + id + "/break", null, { actor: "admin@console" }),
  events: (params) => call("GET", "/events", params),
  series: (params) => call("GET", "/metrics/series", params),
  // The bulk phi trace as raw text so the sessionStorage cache stays
  // lossless (la-charts re-parses on read).
  phiRaw: async () => {
    const res = await fetch(new URL(base() + "/telemetry/phi", location.origin));
    if (!res.ok) throw new Error("HTTP " + res.status);
    return res.text();
  },
};
