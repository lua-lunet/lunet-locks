// Shared JSDoc typedefs. This module exports nothing at runtime; it exists so
// the other modules can pull types in with
//   /** @typedef {import("./types.mjs").Lock} Lock */
//
// Every wire shape below is transcribed from ../../openapi.yaml, which is the
// single source of truth. Optional properties here mirror the yaml `required`
// lists; `| null` mirrors `nullable: true`. All times are epoch milliseconds.

/**
 * `#/components/schemas/Lock`.
 * @typedef {object} Lock
 * @property {number} id
 * @property {string} name Non-unique display path, e.g. `/cluster/members/0000001` (≤128 bytes).
 * @property {string[]} labels ≤8 lowercase/digit/hyphen tags, ≤32 bytes each.
 * @property {"held" | "free"} state
 * @property {string | null} [holder] Null while the lock is free.
 * @property {string | null} [session] Null while the lock is free.
 * @property {number} fencingToken The protocol's lease id (u64 on the wire); BREAK bumps it.
 * @property {number} leaseMs
 * @property {number | null} [expiresAtMs] Null on a released or expired lock.
 * @property {number | null} [takenAtMs] Epoch ms the current holder took the lock (u64 on the wire); null on release/expire/break.
 * @property {number} lastHolderChangeMs
 * @property {number} renewCount u32 on the wire; same-holder renewals, reset on a holder change or release/expire/break.
 * @property {number} holderChanges Holder transitions since boot.
 */

/**
 * `#/components/schemas/Event` — a row of the append-only event log.
 * @typedef {object} Event
 * @property {number} seq
 * @property {number} tsMs
 * @property {EventKind} kind
 * @property {number} lockId
 * @property {string} name
 * @property {string} actor
 * @property {string} detail
 */

/**
 * The `kind` enum shared by /events and `#/components/schemas/Bucket`.
 * @typedef {"acquire" | "renew" | "release" | "cas" | "expire" | "break" | "deny"} EventKind
 */

/**
 * `#/components/schemas/Bucket` — held gauge plus per-kind counts for one slot.
 * @typedef {object} Bucket
 * @property {number} tsMs
 * @property {number} held
 * @property {number} acquire
 * @property {number} renew
 * @property {number} release
 * @property {number} cas
 * @property {number} expire
 * @property {number} break
 * @property {number} deny
 */

/**
 * `#/components/schemas/Node` — cluster membership plus per-node lock counters.
 * @typedef {object} ClusterNode
 * @property {string} id
 * @property {"leader" | "backup"} role
 * @property {number} locksHeld
 * @property {number} acquirePerSec
 * @property {number} renewPerSec
 * @property {number} releasePerSec
 * @property {number} casPerSec
 * @property {number} appliedSlot
 */

/**
 * GET /cluster. `era`/`view` widen to `string` for the aof-console-bridge
 * data source, which replays one standby's committed stream and cannot see
 * live membership — it reports the "—" placeholder instead of a number
 * (see app.mjs's bridge branch and the header rendering in la-app.mjs).
 * @typedef {object} Cluster
 * @property {number | string} era
 * @property {number | string} view
 * @property {string} leader
 * @property {number} nowMs
 * @property {ClusterNode[]} nodes
 */

/**
 * GET /locks
 * @typedef {object} LocksResponse
 * @property {number} nowMs
 * @property {Lock[]} locks
 */

/**
 * GET /locks/{id}
 * @typedef {object} LockDetailResponse
 * @property {Lock} lock
 * @property {Event[]} recentEvents
 */

/**
 * POST /locks/{id}/break
 * @typedef {object} BreakResponse
 * @property {Lock} lock
 * @property {Event} event
 */

/**
 * GET /events
 * @typedef {object} EventsResponse
 * @property {Event[]} events
 */

/**
 * GET /metrics/series
 * @typedef {object} SeriesResponse
 * @property {number} bucketMs
 * @property {Bucket[]} buckets
 */

/**
 * `#/components/schemas/LogLine` — one observability-contract JSON log line,
 * the event's own fields flattened into the root object beside `ts`, `level`
 * and `event`. `era` and `view` ride every protocol-concerned line; the
 * telemetry tape's slot-frontier records carry the injected `ts` and no
 * `level`. The event-specific measurements (`silence_ms` on
 * leader-timeout-detect, `wait_ms` on election-wait-fire) ride the same
 * flattening.
 * @typedef {object} LogLine
 * @property {number} ts The host's millis clock.
 * @property {string} event The named event.
 * @property {"INFO" | "WARN" | "ERROR" | "DEBUG" | "TRACE"} [level]
 * @property {number} [node] The emitting node's id.
 * @property {number} [era]
 * @property {number} [view]
 * @property {number} [slot]
 * @property {number} [leader]
 * @property {string} [state]
 * @property {string} [message]
 * @property {number} [silence_ms] leader-timeout-detect only.
 * @property {number} [wait_ms] election-wait-fire only.
 */

/**
 * `#/components/schemas/TelemetrySpan`
 * @typedef {object} TelemetrySpan
 * @property {number | null} [first_ms] The served series' first ts; null when empty.
 * @property {number | null} [last_ms] The served series' last ts; null when empty.
 */

/**
 * GET /telemetry/log
 * @typedef {object} TelemetryLogResponse
 * @property {LogLine[]} lines
 * @property {number} unparsed
 * @property {TelemetrySpan} span
 */

/**
 * `#/components/schemas/Error` — the body of a 401/404/405/409.
 * @typedef {object} ApiError
 * @property {string} error
 */

/**
 * An Error carrying the HTTP status that produced it (see lib/api.mjs).
 * @typedef {Error & { status: number }} HttpError
 */

/**
 * Query parameters for GET /locks.
 * @typedef {object} LocksParams
 * @property {string} [q] Space-separated terms: `tag:`, `holder:`, or a name substring.
 * @property {"held" | "free"} [state]
 * @property {number} [expiringAtMs]
 * @property {number} [toleranceMs]
 */

/**
 * Query parameters for GET /events.
 * @typedef {object} EventsParams
 * @property {number | null} [fromMs]
 * @property {number | null} [toMs]
 * @property {number} [lockId]
 * @property {EventKind} [kind]
 * @property {string} [q]
 * @property {number} [limit]
 */

/**
 * Query parameters for GET /metrics/series.
 * @typedef {object} SeriesParams
 * @property {number} [fromMs]
 * @property {number} [toMs]
 * @property {number} [bucketMs]
 */

/**
 * Query parameters for GET /telemetry/log.
 * @typedef {object} TelemetryLogParams
 * @property {number | null} [fromMs]
 * @property {number | null} [toMs]
 */

/**
 * The `#la-config` JSON blob baked into index.html.
 * @typedef {object} Config
 * @property {string} apiBase
 * @property {"mock" | "bridge"} dataSource
 * @property {string} bridgeBase
 * @property {number} refreshMs
 * @property {number} expiryDefaultOffsetMs
 * @property {number[]} toleranceOptionsSec
 * @property {number} defaultToleranceSec
 * @property {number} historyWindowMs
 * @property {number} telemetryBucketMs
 * @property {number} logDefaultWindowMs
 * @property {number} watchWarnMs
 */

/**
 * One LKE1 journal record (lib/journal/parse.mjs) — the .bin series the
 * journal data layer replays. `holder` is the hex-encoded 16-byte holder id.
 * @typedef {object} JournalEvent
 * @property {"hold" | "renew" | "release"} kind
 * @property {number} ts
 * @property {number} lockId
 * @property {number} leaseId
 * @property {string} holder
 * @property {number} expiry
 */

/**
 * The active-lock projection the merge engine keeps (store.journalLocks).
 * @typedef {object} JournalLock
 * @property {number} lockId
 * @property {number} leaseId
 * @property {string} holder
 * @property {number} expiry
 * @property {number} acquiredTs
 * @property {number} renewCount
 */

/**
 * One per-second rate bucket (store.journalRates.buckets).
 * @typedef {object} RateBucket
 * @property {number} tsSec
 * @property {number} acquire
 * @property {number} renew
 * @property {number} release
 */

/**
 * The journal feed's health (store.journalStatus).
 * @typedef {object} JournalStatus
 * @property {number} filesTotal
 * @property {number} filesLoaded
 * @property {boolean} wsConnected
 * @property {boolean} caughtUp
 */

/**
 * @typedef {"locks" | "expiry" | "telemetry" | "log"} ViewMode
 */

/**
 * The one mutable object behind lib/state.mjs.
 * @typedef {object} StoreState
 * @property {number} now Client wall clock, ticked once a second.
 * @property {ViewMode} mode
 * @property {string} query
 * @property {string} atText Expiry-mode target time, "HH:MM[:SS]".
 * @property {number} tolSec
 * @property {string} fromText Log-mode range start, "HH:MM[:SS]".
 * @property {string} toText Log-mode range end, "HH:MM[:SS]".
 * @property {Cluster | null} cluster
 * @property {Lock[]} locksAll Unfiltered — drives the path tree.
 * @property {Lock[]} locks Filtered per the current mode/search.
 * @property {number} serverNowMs
 * @property {number | null} selectedId
 * @property {LockDetailResponse | null} detail
 * @property {Set<number>} watched
 * @property {Set<string>} collapsed
 * @property {number | null} confirmId Lock id pending a break confirmation.
 * @property {Event[]} events Log view rows.
 * @property {SeriesResponse | null} series
 * @property {string} toast Transient status text.
 * @property {string} error
 * @property {JournalLock[]} journalLocks Active-lock set.
 * @property {{bucketSec: number, buckets: RateBucket[]}} journalRates Per-second rate buckets.
 * @property {JournalEvent[]} journalEvents Recent events (newest first).
 * @property {JournalStatus} journalStatus
 */

/**
 * The sessionStorage-persisted subset of StoreState. Sets travel as arrays and
 * anything may be missing, because the blob was written by an older build.
 * @typedef {object} SavedState
 * @property {ViewMode} [mode]
 * @property {string} [query]
 * @property {number} [tolSec]
 * @property {string} [atText]
 * @property {string} [fromText]
 * @property {string} [toText]
 * @property {number[]} [watched]
 * @property {string[]} [collapsed]
 */

/**
 * One file row of GET /feed/files (the journal loader worker's listing).
 * @typedef {object} FeedFile
 * @property {string} name
 * @property {boolean} open
 * @property {number} [opMin]
 */

export {};
