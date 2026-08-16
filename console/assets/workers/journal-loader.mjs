// Journal loader Web Worker: polls GET /feed/files for rolled .bin files,
// fetches unpulled ones, parses records, and posts {type:"fileEvents", name, events}
// back to the main thread. The open file (open:true) is NOT pulled here — its
// tail arrives via WS on the main thread.

import { parseRecords } from "../lib/journal/parse.mjs";

/** Set of rolled file names already fetched in this worker session. */
const loaded = new Set();

let polling = false;

async function poll() {
  if (polling) return;
  polling = true;
  try {
    const resp = await fetch("/feed/files");
    if (!resp.ok) return;
    const files = await resp.json();
    // Process rolled files only (open:false), sorted by opMin ascending.
    const rolled = files
      .filter((f) => !f.open)
      .sort((a, b) => (a.opMin ?? 0) - (b.opMin ?? 0));

    for (const f of rolled) {
      if (loaded.has(f.name)) continue;
      try {
        const r = await fetch(`/feed/files/${encodeURIComponent(f.name)}`);
        if (!r.ok) continue;
        const buf = await r.arrayBuffer();
        const events = parseRecords(buf);
        self.postMessage({ type: "fileEvents", name: f.name, events });
        loaded.add(f.name);
      } catch (_) {
        // Skip this file on error; will retry next poll.
      }
    }

    // Report total vs loaded counts so the merge engine can compute caughtUp.
    self.postMessage({
      type: "filesStatus",
      filesTotal: rolled.length,
      filesLoaded: loaded.size,
    });
  } catch (_) {
    // Network error; will retry on next trigger.
  } finally {
    polling = false;
  }
}

self.onmessage = (e) => {
  const msg = e.data;
  if (msg.type === "start") {
    poll();
  } else if (msg.type === "rolled") {
    // A new file was rolled; re-poll to pick it up.
    poll();
  } else if (msg.type === "markLoaded") {
    // Main thread tells us a file was already in IndexedDB.
    if (msg.name) loaded.add(msg.name);
  }
};
