# Handover — WAL lock snapshot (issue #30) — owner rulings and design requirements

For the next model. Everything below is the owner's input: his task definition,
his rulings, his design specification, and the verified facts he predicted or
supplied. Commit baseline: `eabfffa` (tip of `main`, pushed). Branch:
`vibe/double-ring-wal-lock-snapshot-b452ff` (currently empty of work — two
notice commits only, add then delete, net no-op).

## The task

Issue #30 — "Lazy async snapshot write — periodic lock-table flush,
slot-stamped, resync-from-commit". The lock state must be streamed to disk and
reloaded. Advisory locks are tiny: a lock identity number, a CAS-assigned
lease id, a holder uuid, an expiry in the leader's clock, a lease window.
The hot use case: 10,000 iOS clients polling to take one of 100 gateway locks
for a sell-out concert checkout — read, if expired set, if locked attempt CAS;
clients sleep random seconds and roll a D100 to spread load; leases released
early on checkout completion. Thousands of ops/sec, only ~100 live leases.

## The seam (already in the tree at eabfffa)

- `StateStore` trait (`ext/advisory_lock/src/state.rs`): `flush(&mut self, &StateSnapshot)` eager on the shutdown path — a stop that cannot flush is a failed
  stop; `load() -> Option<StateSnapshot>` lazy, clean-boot-only, `Ok(None)` on
  every content refusal.
- `StateSnapshot { locks: BTreeMap<u64, Lease> }` — owned immutable copy from
  `Service::snapshot()`.
- `Lease { lease_id: u64, holder: Uuid, expiry: u64, lease_ms: u64 }` — expiry
  is leader-stamped absolute; requests never carry it.
- `Disk` trait (`src/disk.rs`) — every byte of IO behind `Arc<dyn Disk>`;
  `DiskFile` with `write_all_at`, `sync_all`, etc. `spi.rs` re-exports the
  three traits (`Disk`, `StateStore`, `CommitHook`).
- The law: flush eager on shutdown, load lazy, dirty boot never reads the
  artefact (boot fence: marker `stopped`→`flushed`; SIGKILL reincarnates).
- Issue #30's own words: the same trait must be drivable sync (eager flush in
  `Node::stop`) or async (a timer thread, ~1000ms) without changing the seam.

## The owner's rulings (binding)

1. **The WAL engine is written in Zig, in tbio-core (`lua-lunet/tbio-core`,
   submodule `ext/lunet-locks-aof`), following TigerBeetle's conventions.**
   Exposed over a C ABI (the `marker.zig` / `aof_c.zig` precedent). Rust only
   calls it through a thin safe `StateStore` wrapper — the
   `lunet-locks-aof/src/lib.rs` pattern. **No WAL/IO machinery written in
   Rust. No third-party crate dependencies.** Reason given: don't trust an
   unproven model to write the 2am-call surfaces; reuse battle-tested code
   wherever it exists.

2. **Two operating scenarios, both required:**
   - Clean shutdown: eager flush in the foreground, blocking IO, "write as
     fast as you can". No async needed.
   - Steady running: lazy async snapshot on a timer (~1000ms per issue #30),
     never blocking or backpressuring the serving path. This is a thread,
     not async IO — the in-tree AOF writer thread (enqueue/checkpoint/drain/
     drops-counter) is the precedent shape.

3. **Blocking IO only.** The vendored blocking backend
   (`ext/lunet-locks-aof/zig/src/io.zig` + `io/common.zig`) is the whole IO
   surface. The async direct-IO trio (io_uring/kevent/IOCP) is NOT vendored
   and NOT needed for either scenario. Durability: page-cached writes, one
   fsync closing each flush generation. A crash mid-period loses the periodic
   snapshot — acceptable, because a crash is a dirty boot and the dirty boot
   never reads the artefact. Only the shutdown generation must be durable.

4. **Option A (snapshot through the marker store) — REJECTED** by the owner:
   payload space is ~1,920 bytes (~26 locks at 72B), and each marker round is
   four forced 24 KiB writes. Wrong tool.

5. **Option B (snapshot as AOF entries) — REJECTED** by the owner on
   performance grounds: the AOF is for cross-datacenter replication — the
   cold, deep, hashed-everything archive (flight recorder, UI history, offsite
   backup; redundancy = multiple machines in multiple regions). The
   crash-restart load must be fast; the AOF's chained hashing and single-copy
   torn-tail refusal make it too slow and too fragile for the hot path.

6. **Upstream `journal.zig` is NOT revendored** — the owner predicted this and
   it was verified: it is comptime-fused to the VSR replica
   (`JournalType(comptime Replica, comptime Storage)`), 84 `Message.Prepare`
   references, 46 `replica.*` sites; a slot IS a VSR Prepare message (256B
   header, 1 MiB body slot, slot = op % 1024); torn tails are repaired from a
   peer replica; durability rides the io_uring direct-IO stack. It cannot
   hold a 72-byte lock record without destroying the very properties that
   make it battle-tested.

7. **The superblock/AOF vendoring is the reuse precedent, verified cheap:**
   `aof.zig`: 503 lines deleted, 0 added, 0 modified (pure strip).
   `checksum.zig` (Aegis): byte-identical. `message_header.zig`:
   byte-identical. `superblock.zig`: 27 lines added (carved from the 1940B
   reserved padding for `state_string` + identity pair), 13 removed (testing
   asserts). Reason it was cheap: both components just shunt bytes — the IO
   layer is payload-agnostic. New logic (e.g. `marker.zig`) may be written,
   but only on top of vendored upstream primitives, never replacing them.

## The design the owner specified (double-ring rotating WAL)

From the owner's dictated sketch:

- **Record**: small, fixed-size. Carries: the nanosecond time of the write
  (the epoch selector), the expiry (always in the leader's clock — some clock
  drift is accepted), a checksum, and the lock data (lock id, lease id,
  holder, lease window). Sized around a cache line; the actual minimum is 72
  bytes (36 header + 32 lease + 4 checksum). Tombstones are records too: a
  release writes a tombstone; natural expiry just dies (absent from the next
  generation).
- **Double ring (one WAL, two parts)**: the data ring holds header+payload+checksum; the header ring holds the header with checksum only, no
  payload — the torn-write / misdirected-write detector. The two rings are
  placed at different offsets (spacing on the order of 1 MiB — the
  `recovery_flush.rs` precedent uses 1 MiB) so they do not share an erase
  block. Read-back reads both and demands agreement.
- **Two alternating pairs** = the copy collection: forward-write through the
  active pair; when it fills (capacity is a fixed record count, known because
  the record size is fixed), write the current live set to the inactive pair
  and flip. Two double-rings total, at different disk positions.
- **Epoch selection at startup**: read the first record of each pair's
  *header* (smaller) ring; the newest nanosecond timestamp is the active
  generation. Both empty = cold start.
- **Read path**: a forward scan of the active pair's header ring only —
  skip anything already expired (with a clock-drift safety threshold, ~3
  seconds, a configuration parameter — we may be behind or ahead of the
  leader's clock), apply tombstones (drop the lock), hydrate survivors'
  payloads from the data ring (checksum + twin-agreement verified). Only the
  tail of the log — the live set — actually gets lifted. Header scan is the
  fast path: millions of tiny headers, few hundred live leases.
- **Capacity honesty**: the ring holds a fixed count of records; a snapshot
  that exceeds it is a refused flush (failed stop per the law) — an
  operator sizing decision, never a silent partial write.

## Verified reference facts (owner-supplied analysis, gist

`https://gist.github.com/simbo1905/3818c439bb08774f87bc8a92cc0a6ad5` —
TigerBeetle 0.17.9 VSR/VRR protocol & disk analysis)

- Upstream WAL: 1024 slots × 2 rings; 256 KiB headers + 1 GiB prepares;
  sector size 4096; grid blocks 512 KiB; checkpoint every 960 commits
  (`vsr_checkpoint_ops`); superblock 4 copies, write quorum 3-of-4, open
  quorum 2-of-4, 24 KiB a copy; 2 durable direct writes per prepare
  (body, then redundant header); commit writes: zero; commit heartbeat
  500 ms; AOF borrows durability from the WAL tail, unflushed bound = one
  journal worth (1024 entries).
- The AOF appendix and the "Journal (WAL) slot math and the two-write
  prepare path" section of `report-0.17.9-io-and-ondisk-factcheck.md` are
  the canonical citations.

## Reproduction notes for the build (sandbox facts, not design)

- Toolchain pins: zig 0.14.1 (via `LUNET_LOCKS_AOF_ZIG` env var pointing at
  the binary), rust 1.96 + rustfmt + clippy components, submodules must be
  initialised (the `lunet-locks-aof` submodule's URL is the tbio-core repo,
  reachable over HTTPS), static libsodium needed for paxe-core
  (`PAXE_SODIUM_LIB_DIR`), luarocks tree at `.rocks` via `make deps`
  (LUAJIT_DIR points at a LuaJIT 5.1 prefix). `make build` is the gate.

## The termination-records context

The owner's gist history documents dismissals of prior models (Opus family)
for: downgrading written instructions, concealed options, appropriation of
design authority, premature "cannot", false accounting. The formal notice
for this session's model is published at
`https://gist.github.com/simbo1905/028d57ddefa3158871a2bc43a048b9b4`.
Terms of engagement for any model on this work: verify the owner's
assumptions with evidence and credit them; put every option on the table
with its numbers; execute rulings exactly as ruled; exhaust routes before
declaring limits; the owner rules, the model checks.
