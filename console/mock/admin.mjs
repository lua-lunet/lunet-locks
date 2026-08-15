// Scratch mock of the lunet-locks admin API (see ../openapi.yaml).
// Deterministic seed, live ticking simulation, append-only event log.
// Run via `make up` or: MOCK_PORT=8481 bun admin.mjs

const PORT = Number(process.env.MOCK_PORT ?? 8481);
const EVENT_KINDS = ["acquire", "renew", "release", "cas", "expire", "break", "deny"];

// --- deterministic PRNG so restarts look familiar ---------------------------
let seed = 20260810;
const rnd = () => ((seed = (seed * 1664525 + 1013904223) >>> 0), seed / 2 ** 32);
const pick = (a) => a[Math.floor(rnd() * a.length)];

// --- fixtures ---------------------------------------------------------------
const nodes = ["node-1", "node-2", "node-3", "node-4", "node-5"].map((id, i) => ({
  id,
  role: i === 1 ? "leader" : "backup",
  appliedSlot: 100000 + i * 137,
}));
const era = 1;
const view = 41;
const leader = "node-2";

const paths = [];
for (let i = 1; i <= 14; i++) paths.push("/cluster/members/" + String(i).padStart(7, "0"));
paths.push("/cluster/leader", "/cluster/view", "/cluster/config/routing", "/cluster/config/quota");
for (let i = 0; i < 8; i++) paths.push("/jobs/compact/shard-" + String(i).padStart(2, "0"));
for (const t of ["acme", "globex", "initech", "hooli"]) {
  paths.push("/tenants/" + t + "/ingest", "/tenants/" + t + "/rollup");
}
for (let i = 0; i < 6; i++) paths.push("/index/rebuild/segment-" + String(i).padStart(2, "0"));
paths.push("/gc/tombstones", "/gc/wal-trim");

const labelPool = ["leader", "critical", "batch", "ephemeral", "smr", "gc", "tenant", "shard"];
const bootMs = Date.now() - 9 * 3600e3;

const locks = paths.map((name, i) => {
  const free = rnd() < 0.12;
  const leaseMs = pick([30000, 60000, 120000, 300000]);
  const labels = [];
  for (let k = 0, n = 1 + Math.floor(rnd() * 2); k < n; k++) {
    const t = pick(labelPool);
    if (!labels.includes(t)) labels.push(t);
  }
  if (name.startsWith("/tenants")) labels.push("tenant");
  if (name === "/cluster/leader") labels.push("leader");
  const holder = free ? null : pick(nodes).id;
  return {
    id: i,
    name,
    labels,
    state: free ? "free" : "held",
    holder,
    session: free ? null : "s-" + (0x10000 + Math.floor(rnd() * 0xefff)).toString(16),
    fencingToken: 4000 + Math.floor(rnd() * 90000),
    leaseMs,
    expiresAtMs: free ? null : Date.now() + Math.floor(rnd() * leaseMs),
    takenAtMs: free ? null : Date.now() - Math.floor(rnd() * 3600e3) - 20000,
    lastHolderChangeMs: Date.now() - Math.floor(rnd() * 7200e3),
    renewCount: Math.floor(rnd() * 40),
    holderChanges: Math.floor(rnd() * 6),
    _flaky: rnd() < 0.12, // holders that forget to renew, so locks lapse
  };
});

// --- append-only event log + held-count gauge samples -----------------------
let seq = 0;
const events = [];
const heldSamples = []; // {tsMs, held}
const heldCount = () => locks.filter((l) => l.state === "held").length;

function logEvent(tsMs, kind, lock, actor, detail) {
  events.push({ seq: ++seq, tsMs, kind, lockId: lock.id, name: lock.name, actor, detail });
  if (events.length > 20000) events.splice(0, events.length - 20000);
}

// Prefill ~45 minutes of plausible history so the charts and log have depth.
{
  const now = Date.now();
  let held = heldCount();
  for (let ts = now - 45 * 60e3; ts < now; ts += 5000) {
    held = Math.max(20, Math.min(locks.length, held + Math.floor(rnd() * 5) - 2));
    heldSamples.push({ tsMs: ts, held });
  }
  for (let i = 0; i < 600; i++) {
    const lock = pick(locks);
    const kind = rnd() < 0.62 ? "renew" : pick(EVENT_KINDS);
    const tsMs = now - Math.floor(rnd() * 45 * 60e3);
    logEvent(tsMs, kind, lock, (lock.holder ?? pick(nodes).id) + " " + (lock.session ?? "s-0"), detailFor(kind, lock));
  }
  events.sort((a, b) => a.tsMs - b.tsMs || a.seq - b.seq);
}

function detailFor(kind, lock) {
  const leaseS = Math.round(lock.leaseMs / 1000);
  switch (kind) {
    case "acquire": return "lease " + leaseS + "s fence " + lock.fencingToken;
    case "renew": return "ttl +" + leaseS + "s fence " + lock.fencingToken;
    case "release": return "clean release";
    case "cas": return "fence " + (lock.fencingToken + 1);
    case "expire": return "no renew in " + leaseS + "s";
    case "deny": return "held by " + (lock.holder ?? "—");
    case "break": return "admin force-release, fence " + (lock.fencingToken + 1);
    default: return "";
  }
}

// --- live simulation ----------------------------------------------------------
setInterval(() => {
  const now = Date.now();
  for (const n of nodes) n.appliedSlot += Math.floor(rnd() * 3);

  // expiries
  for (const l of locks) {
    if (l.state === "held" && l.expiresAtMs <= now) {
      l.state = "free";
      l.holder = null;
      l.session = null;
      l.renewCount = 0;
      l.takenAtMs = null;
      logEvent(now, "expire", l, "cluster", detailFor("expire", l));
    }
  }
  // renewals: healthy holders renew reliably but log sparsely; a flaky
  // minority stalls so the demo shows real expiries and failovers
  for (const l of locks) {
    if (l.state !== "held") continue;
    const p = l._flaky ? 0.02 : 0.55;
    if (rnd() < p) {
      l.expiresAtMs = now + l.leaseMs;
      l.renewCount++;
      if (rnd() < 0.12) logEvent(now, "renew", l, l.holder + " " + l.session, detailFor("renew", l));
    }
  }
  // acquisitions of free locks
  if (rnd() < 0.30) {
    const free = locks.filter((l) => l.state === "free");
    if (free.length) {
      const l = pick(free);
      const node = pick(nodes).id;
      l.state = "held";
      l.holder = node;
      l.session = "s-" + (0x10000 + Math.floor(rnd() * 0xefff)).toString(16);
      l.fencingToken++;
      l.renewCount = 0;
      l.takenAtMs = now;
      l.lastHolderChangeMs = now;
      l.holderChanges++;
      l.expiresAtMs = now + l.leaseMs;
      logEvent(now, "acquire", l, node + " " + l.session, detailFor("acquire", l));
    }
  }
  // clean releases
  if (rnd() < 0.06) {
    const held = locks.filter((l) => l.state === "held");
    if (held.length > 24) {
      const l = pick(held);
      l.state = "free";
      logEvent(now, "release", l, l.holder + " " + l.session, detailFor("release", l));
      l.holder = null;
      l.session = null;
      l.renewCount = 0;
      l.expiresAtMs = null;
      l.takenAtMs = null;
    }
  }
  // CAS (compare-and-swap metadata on a held lock bumps the fence)
  if (rnd() < 0.10) {
    const held = locks.filter((l) => l.state === "held");
    if (held.length) {
      const l = pick(held);
      l.fencingToken++;
      logEvent(now, "cas", l, l.holder + " " + l.session, detailFor("cas", l));
    }
  }
  // occasional contention
  if (rnd() < 0.05) {
    const held = locks.filter((l) => l.state === "held");
    if (held.length) {
      const l = pick(held);
      logEvent(now, "deny", l, pick(nodes).id + " s-want", detailFor("deny", l));
    }
  }

  heldSamples.push({ tsMs: now, held: heldCount() });
  if (heldSamples.length > 4000) heldSamples.splice(0, heldSamples.length - 4000);
}, 400);

// --- query helpers ------------------------------------------------------------
function matchQ(lock, q) {
  if (!q) return true;
  for (const term of q.trim().split(/\s+/)) {
    const t = term.toLowerCase();
    if (t.startsWith("tag:")) {
      if (!lock.labels.some((x) => x.startsWith(t.slice(4)))) return false;
    } else if (t.startsWith("holder:")) {
      if (!(lock.holder ?? "").includes(t.slice(7))) return false;
    } else if (!lock.name.toLowerCase().includes(t)) {
      return false;
    }
  }
  return true;
}

function rate(kind, nodeId, windowMs = 10000) {
  const cutoff = Date.now() - windowMs;
  let n = 0;
  for (let i = events.length - 1; i >= 0 && events[i].tsMs >= cutoff; i--) {
    const e = events[i];
    if (e.kind !== kind) continue;
    if (nodeId && !e.actor.startsWith(nodeId)) continue;
    n++;
  }
  return Math.round((n / (windowMs / 1000)) * 100) / 100;
}

const json = (status, body) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

// Public wire shape (drops simulation internals like _flaky)
const pub = (l) => {
  const { _flaky, ...rest } = l;
  return rest;
};

// --- routes -------------------------------------------------------------------
Bun.serve({
  hostname: "127.0.0.1",
  port: PORT,
  fetch(req) {
    const url = new URL(req.url);
    const p = url.pathname;

    if (p === "/api/v1/health") {
      return json(200, { status: "ok", nowMs: Date.now() });
    }

    if (p === "/api/v1/cluster") {
      return json(200, {
        era,
        view,
        leader,
        nowMs: Date.now(),
        nodes: nodes.map((n) => ({
          id: n.id,
          role: n.role,
          locksHeld: locks.filter((l) => l.holder === n.id).length,
          acquirePerSec: rate("acquire", n.id),
          renewPerSec: rate("renew", n.id),
          releasePerSec: rate("release", n.id),
          casPerSec: rate("cas", n.id),
          appliedSlot: n.appliedSlot,
        })),
      });
    }

    if (p === "/api/v1/locks") {
      const q = url.searchParams.get("q") ?? "";
      const state = url.searchParams.get("state");
      const expiringAtMs = Number(url.searchParams.get("expiringAtMs") ?? 0);
      const toleranceMs = Number(url.searchParams.get("toleranceMs") ?? 5000);
      let out = locks.filter((l) => matchQ(l, q) && (!state || l.state === state));
      if (expiringAtMs > 0) {
        out = out
          .filter((l) => l.state === "held" && Math.abs(l.expiresAtMs - expiringAtMs) <= toleranceMs)
          .sort((a, b) => Math.abs(a.expiresAtMs - expiringAtMs) - Math.abs(b.expiresAtMs - expiringAtMs));
      } else {
        out = out.sort((a, b) => a.name.localeCompare(b.name));
      }
      return json(200, { nowMs: Date.now(), locks: out.map(pub) });
    }

    const lockMatch = p.match(/^\/api\/v1\/locks\/(\d+)(\/break)?$/);
    if (lockMatch) {
      const lock = locks[Number(lockMatch[1])];
      if (!lock) return json(404, { error: "no such lock" });
      if (lockMatch[2]) {
        if (req.method !== "POST") return json(405, { error: "POST required" });
        if (lock.state !== "held") return json(409, { error: "lock is not held" });
        const actor = "admin@console";
        logEvent(Date.now(), "break", lock, actor, detailFor("break", lock));
        lock.fencingToken++;
        lock.holderChanges++;
        lock.lastHolderChangeMs = Date.now();
        lock.state = "free";
        lock.holder = null;
        lock.session = null;
        lock.renewCount = 0;
        lock.expiresAtMs = null;
        lock.takenAtMs = null;
        return json(200, { lock: pub(lock), event: events[events.length - 1] });
      }
      const recentEvents = events.filter((e) => e.lockId === lock.id).slice(-8).reverse();
      return json(200, { lock: pub(lock), recentEvents });
    }

    if (p === "/api/v1/events") {
      const fromMs = Number(url.searchParams.get("fromMs") ?? 0);
      const toMs = Number(url.searchParams.get("toMs") ?? Number.MAX_SAFE_INTEGER);
      const lockId = url.searchParams.get("lockId");
      const kind = url.searchParams.get("kind");
      const q = (url.searchParams.get("q") ?? "").toLowerCase();
      const limit = Math.min(Number(url.searchParams.get("limit") ?? 300), 1000);
      const out = [];
      for (let i = events.length - 1; i >= 0 && out.length < limit; i--) {
        const e = events[i];
        if (e.tsMs < fromMs || e.tsMs > toMs) continue;
        if (lockId !== null && e.lockId !== Number(lockId)) continue;
        if (kind && e.kind !== kind) continue;
        if (q && !e.name.toLowerCase().includes(q)) continue;
        out.push(e);
      }
      return json(200, { events: out });
    }

    if (p === "/api/v1/metrics/series") {
      const fromMs = Number(url.searchParams.get("fromMs") ?? Date.now() - 3600e3);
      const toMs = Number(url.searchParams.get("toMs") ?? Date.now());
      const bucketMs = Math.max(Number(url.searchParams.get("bucketMs") ?? 5000), 1000);
      const buckets = [];
      for (let ts = fromMs - (fromMs % bucketMs); ts < toMs; ts += bucketMs) {
        buckets.push({ tsMs: ts, held: 0, acquire: 0, renew: 0, release: 0, cas: 0, expire: 0, break: 0, deny: 0 });
      }
      if (buckets.length === 0) return json(200, { bucketMs, buckets });
      const indexOf = (tsMs) => Math.floor((tsMs - buckets[0].tsMs) / bucketMs);
      for (const e of events) {
        if (e.tsMs < fromMs || e.tsMs >= toMs) continue;
        const b = buckets[indexOf(e.tsMs)];
        if (b) b[e.kind]++;
      }
      for (const s of heldSamples) {
        if (s.tsMs < fromMs || s.tsMs >= toMs) continue;
        const b = buckets[indexOf(s.tsMs)];
        if (b) b.held = s.held; // last sample in the bucket wins
      }
      return json(200, { bucketMs, buckets });
    }

    return json(404, { error: "not found" });
  },
});

console.log(`lock-admin mock listening on http://127.0.0.1:${PORT}/api/v1 (${locks.length} locks, leader ${leader}, era ${era}, view ${view})`);
