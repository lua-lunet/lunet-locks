// Thin fetch client for the admin API (see ../../openapi.yaml).

import { config } from "./state.mjs";

async function call(method, path, params, body) {
  const url = new URL(config.apiBase + path, location.origin);
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
};
