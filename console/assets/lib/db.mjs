// IndexedDB cache for telemetry history. The mock keeps minutes; the browser
// keeps hours — buckets and events survive reloads and mock restarts.

const DB_NAME = "lock-admin";
const DB_VERSION = 2;

function open() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains("events")) db.createObjectStore("events", { keyPath: "seq" });
      if (!db.objectStoreNames.contains("buckets")) db.createObjectStore("buckets", { keyPath: "tsMs" });
      // Journal stores (v2).
      if (!db.objectStoreNames.contains("journalEvents")) {
        db.createObjectStore("journalEvents", { keyPath: ["ts", "lockId", "leaseId"] });
      }
      if (!db.objectStoreNames.contains("loadedFiles")) {
        db.createObjectStore("loadedFiles", { keyPath: "name" });
      }
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

  // ---- Journal stores (v2) ----

  /** Upsert journal events (idempotent by [ts, lockId, leaseId] key). */
  async putJournalEvents(events) {
    if (!events.length) return;
    const d = await open();
    await tx(d, "journalEvents", "readwrite", (s) => {
      for (const e of events) s.put(e);
    });
    d.close();
  },

  /** Mark a rolled file as fully ingested. */
  async markFileLoaded(name) {
    const d = await open();
    await tx(d, "loadedFiles", "readwrite", (s) => {
      s.put({ name, loadedAt: Date.now() });
    });
    d.close();
  },

  /** Get all previously loaded file names. */
  async getLoadedFiles() {
    const d = await open();
    const rows = await new Promise((resolve, reject) => {
      const req = d.transaction("loadedFiles").objectStore("loadedFiles").getAll();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    d.close();
    return rows.map((r) => r.name);
  },

  /** Read all journaled events, ordered by their [ts, lockId, leaseId] key. */
  async readJournalEvents() {
    const d = await open();
    const rows = await new Promise((resolve, reject) => {
      const req = d.transaction("journalEvents").objectStore("journalEvents").getAll();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    d.close();
    return rows;
  },
};
