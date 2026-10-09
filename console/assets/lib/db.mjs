// IndexedDB cache for telemetry history. The mock keeps minutes; the browser
// keeps hours — buckets and events survive reloads and mock restarts.

/** @typedef {import("./types.mjs").Bucket} Bucket */
/** @typedef {import("./types.mjs").Event} Event */
/** @typedef {import("./types.mjs").JournalEvent} JournalEvent */

const DB_NAME = "lock-admin";
const DB_VERSION = 2;

/** @returns {Promise<IDBDatabase>} */
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

/**
 * @param {IDBDatabase} db
 * @param {string} store
 * @param {IDBTransactionMode} mode
 * @param {(s: IDBObjectStore) => IDBRequest | void} fn
 * @returns {Promise<any>}
 */
function tx(db, store, mode, fn) {
  return new Promise((resolve, reject) => {
    const t = db.transaction(store, mode);
    const out = fn(t.objectStore(store));
    t.oncomplete = () => resolve(out?.result ?? undefined);
    t.onerror = () => reject(t.error);
  });
}

export const db = {
  /**
   * @param {Event[]} events
   * @returns {Promise<void>}
   */
  async cacheEvents(events) {
    if (!events.length) return;
    const d = await open();
    await tx(d, "events", "readwrite", (s) => {
      for (const e of events) s.put(e);
    });
    d.close();
  },
  /**
   * @param {Bucket[]} buckets
   * @returns {Promise<void>}
   */
  async cacheBuckets(buckets) {
    if (!buckets.length) return;
    const d = await open();
    await tx(d, "buckets", "readwrite", (s) => {
      for (const b of buckets) s.put(b);
    });
    d.close();
  },
  /**
   * @param {number} fromMs
   * @returns {Promise<Bucket[]>}
   */
  async readBuckets(fromMs) {
    const d = await open();
    const out = await new Promise((resolve, reject) => {
      const req = d.transaction("buckets").objectStore("buckets")
        .getAll(IDBKeyRange.lowerBound(fromMs));
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    d.close();
    return /** @type {Bucket[]} */ (out);
  },
  /**
   * @param {number} olderThanMs
   * @returns {Promise<void>}
   */
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

  /**
   * Upsert journal events (idempotent by [ts, lockId, leaseId] key).
   * @param {JournalEvent[]} events
   * @returns {Promise<void>}
   */
  async putJournalEvents(events) {
    if (!events.length) return;
    const d = await open();
    await tx(d, "journalEvents", "readwrite", (s) => {
      for (const e of events) s.put(e);
    });
    d.close();
  },

  /**
   * Mark a rolled file as fully ingested.
   * @param {string} name
   * @returns {Promise<void>}
   */
  async markFileLoaded(name) {
    const d = await open();
    await tx(d, "loadedFiles", "readwrite", (s) => {
      s.put({ name, loadedAt: Date.now() });
    });
    d.close();
  },

  /**
   * Get all previously loaded file names.
   * @returns {Promise<string[]>}
   */
  async getLoadedFiles() {
    const d = await open();
    const rows = await new Promise((resolve, reject) => {
      const req = d.transaction("loadedFiles").objectStore("loadedFiles").getAll();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    d.close();
    return /** @type {{name: string}[]} */ (rows).map((r) => r.name);
  },

  /**
   * Read all journaled events, ordered by their [ts, lockId, leaseId] key.
   * @returns {Promise<JournalEvent[]>}
   */
  async readJournalEvents() {
    const d = await open();
    const rows = await new Promise((resolve, reject) => {
      const req = d.transaction("journalEvents").objectStore("journalEvents").getAll();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    d.close();
    return /** @type {JournalEvent[]} */ (rows);
  },
};
