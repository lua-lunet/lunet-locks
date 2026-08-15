// Pure helpers: clock parsing, HTML escaping, duration formatting. No DOM,
// no storage, no wall clock — every "now" is an injected fixture instant.

import { esc, fmtClock, fmtDur, parseClock } from "../lib/util.mjs";
import { AssertionError, assertEqual } from "./assert.mjs";

// Mid-January UTC noon: no DST transition within a day of it in any zone,
// so the +24h arithmetic in the staleness case is exact everywhere.
const BASE = Date.UTC(2024, 0, 15, 12, 0, 0);

/** @type {import("./assert.mjs").Suite} */
export const suite = {
  name: "util",
  cases: {
    "parseClock accepts HH:MM and HH:MM:SS"() {
      const a = parseClock("10:30", BASE);
      const b = parseClock("10:30:45", BASE);
      if (a === null || b === null) throw new AssertionError("parseClock returned null", { a, b }, "number");
      assertEqual(fmtClock(a), "10:30:00");
      assertEqual(fmtClock(b), "10:30:45");
    },
    "parseClock rejects null, empty and malformed text"() {
      for (const bad of [null, undefined, "", "noon", "9:5", "10:30:5", "10:30x", "10"]) {
        assertEqual(parseClock(bad, BASE), null, `parseClock(${JSON.stringify(bad)}) should be null`);
      }
    },
    "parseClock anchors to the base day (item43 staleness)"() {
      // The log view re-resolves the same HH:MM text on every poll; the
      // anchor must track the day of the base instant, not freeze at the
      // first resolution — that freeze was the empty-Logs bug.
      const d1 = parseClock("10:30", BASE);
      const d2 = parseClock("10:30", BASE + 86_400_000);
      if (d1 === null || d2 === null) throw new AssertionError("parseClock returned null", { d1, d2 }, "number");
      assertEqual(d2 - d1, 86_400_000, "same text, next day");
      assertEqual(fmtClock(d1), "10:30:00");
      assertEqual(fmtClock(d2), "10:30:00");
    },
    "fmtClock round-trips through parseClock to the second"() {
      for (const t of [BASE, BASE + 3_599_000, BASE + 86_399_000]) {
        assertEqual(parseClock(fmtClock(t), t), Math.floor(t / 1000) * 1000, `round-trip at +${t - BASE}ms`);
      }
    },
    "esc escapes every markup metacharacter"() {
      assertEqual(esc(`<a href="x">&'</a>`), "&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;");
    },
    "esc maps nullish to the empty string"() {
      assertEqual(esc(null), "");
      assertEqual(esc(undefined), "");
      assertEqual(esc(0), "0");
    },
    "fmtDur clamps negatives and formats below a minute"() {
      assertEqual(fmtDur(-5000), "0s");
      assertEqual(fmtDur(0), "0s");
      assertEqual(fmtDur(59_499), "59s");
    },
    "fmtDur boundaries at one minute and one hour"() {
      assertEqual(fmtDur(60_000), "1m 0s");
      assertEqual(fmtDur(3_599_000), "59m 59s");
      assertEqual(fmtDur(3_600_000), "1h 0m");
      assertEqual(fmtDur(5_400_000), "1h 30m");
    },
  },
};
