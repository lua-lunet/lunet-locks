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
 * @property {string | null} [holder] Absent or null while no holder is recorded.
 * @property {number} fencingToken The protocol's lease_id (u64 on the wire); BREAK bumps it.
 * @property {number} leaseMs
 * @property {number | null} [expiresAtMs]
 * @property {number | null} [takenAtMs] Epoch ms the current holder took the lock; null while unheld.
 * @property {number} renewCount Same-holder renewals; reset on a holder change, release, expire or break.
 */

/**
 * `#/components/schemas/Event` — a row of the append-only follower journal.
 * @typedef {object} Event
 * @property {number} seq
 * @property {number} tsMs
 * @property {EventKind} kind
 * @property {number} lockId
 * @property {string} name
 * @property {string | null} actor The core does not record who broke a lock, so this is often null.
 * @property {string} detail
 */

/**
 * The `kind` enum shared by /events and #/components/schemas/Bucket.
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
 * `#/components/schemas/Node` — telemetry derived from that node's local log
 * segments. The edge cannot see replication internals, so there is no term or
 * leaderId anywhere in the cluster view.
 * @typedef {object} ClusterNode
 * @property {string} id
 * @property {number} locksHeld Latest held_gauge record: the node's own observed held-lock count.
 * @property {number} acquirePerSec
 * @property {number} renewPerSec
 * @property {number} releasePerSec
 * @property {number} casPerSec
 * @property {number} breakPerSec
 * @property {number} denyPerSec
 * @property {number} expirePerSec
 * @property {number} segmentCount
 * @property {number} segmentBytes
 * @property {number | null} lastRecordMs
 */

/**
 * Numeric per-node telemetry keys, i.e. every ClusterNode field that can be
 * summed across the cluster.
 * @typedef {"locksHeld" | "acquirePerSec" | "renewPerSec" | "releasePerSec"
 *   | "casPerSec" | "breakPerSec" | "denyPerSec" | "expirePerSec"
 *   | "segmentCount" | "segmentBytes"} NodeMetric
 */

/**
 * GET /cluster
 * @typedef {object} ClusterResponse
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
 * `#/components/schemas/Error` — the body of a 401/404/405/409.
 * @typedef {object} ApiError
 * @property {string} error
 */

/**
 * An Error carrying the HTTP status that produced it (see lib/api.mjs).
 * @typedef {Error & { status?: number }} HttpError
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
 * The `#la-config` JSON blob baked into index.html.
 * @typedef {object} Config
 * @property {string} apiBase
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
 * The one mutable object behind lib/state.mjs.
 * @typedef {object} StoreState
 * @property {number} now Client wall clock, ticked once a second.
 * @property {ViewMode} mode
 * @property {string} query
 * @property {string} atText Expiry-mode target time, "HH:MM[:SS]".
 * @property {number} tolSec
 * @property {string} fromText Log-mode range start, "HH:MM[:SS]".
 * @property {string} toText Log-mode range end, "HH:MM[:SS]".
 * @property {ClusterResponse | null} cluster
 * @property {Lock[]} locksAll Unfiltered — drives the path tree.
 * @property {Lock[]} locks Filtered per the current mode/search.
 * @property {number} serverNowMs
 * @property {number | null} selectedId
 * @property {LockDetailResponse | null} detail
 * @property {Set<number>} watched
 * @property {Set<string>} collapsed
 * @property {number[] | null} colWidths Lock-table px widths; null = defaults.
 * @property {number[] | null} logColWidths Log-view px widths; null = defaults.
 * @property {number[] | null} paneWidths Shell [tree, detail] px widths; null = defaults.
 * @property {number | null} confirmId Lock id pending a break confirmation.
 * @property {Event[]} events Log view rows.
 * @property {SeriesResponse | null} series
 * @property {string} toast Transient status text.
 * @property {string} error
 */

/**
 * @typedef {"locks" | "expiry" | "telemetry" | "log"} ViewMode
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
 * @property {number[] | null} [colWidths]
 * @property {number[] | null} [logColWidths]
 * @property {number[] | null} [paneWidths]
 */

export {};
