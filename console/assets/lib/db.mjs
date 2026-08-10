// IndexedDB cache for telemetry history. The mock keeps minutes; the browser
// keeps hours — buckets and events survive reloads and mock restarts.

const DB_NAME = "lock-admin";
const DB_VERSION = 1;

function open() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains("events")) db.createObjectStore("events", { keyPath: "seq" });
      if (!db.objectStoreNames.contains("buckets")) db.createObjectStore("buckets", { keyPath: "tsMs" });
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function tx(db, store, mode, fn) {
  return new Promise((resolve, reject) => {
    const t = db.transaction(store, mode);
    const out = fn(t.objectStore(store));
    t.oncomplete = () => resolve(out?.result ?? undefined);
    t.onerror = () => reject(t.error);
  });
}

export const db = {
  async cacheEvents(events) {
    if (!events.length) return;
    const d = await open();
    await tx(d, "events", "readwrite", (s) => {
      for (const e of events) s.put(e);
    });
    d.close();
  },
  async cacheBuckets(buckets) {
    if (!buckets.length) return;
    const d = await open();
    await tx(d, "buckets", "readwrite", (s) => {
      for (const b of buckets) s.put(b);
    });
    d.close();
  },
  async readBuckets(fromMs) {
    const d = await open();
    const out = await new Promise((resolve, reject) => {
      const req = d.transaction("buckets").objectStore("buckets")
        .getAll(IDBKeyRange.lowerBound(fromMs));
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    d.close();
    return out;
  },
  async prune(olderThanMs) {
    const d = await open();
    await tx(d, "events", "readwrite", (s) => {
      const req = s.openCursor();
      req.onsuccess = () => {
        const c = req.result;
        if (!c) return;
        if (c.value.tsMs < olderThanMs) c.delete();
        c.continue();
      };
    });
    await tx(d, "buckets", "readwrite", (s) => s.delete(IDBKeyRange.upperBound(olderThanMs)));
    d.close();
  },
};
