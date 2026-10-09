# The contract seam

The advisory-locking machinery is separated from everything that persists,
measures, or kills it by three traits: `Disk`, `StateStore`, and `CommitHook`.
They are the whole contract. Nothing else crosses the boundary — no ambient
file system, no globals, no environment reach-arounds. In the
`lunet-advisory-lock` crate, `Disk` and `StateStore` are defined in its
`disk` and `state` modules, and `CommitHook` is defined in its `spi` module;
the `spi` module re-exports all three and is the single named surface both
sides of the seam are written against.

The seam exists because the machinery must live in two environments that are
never conflated: a minimal experimental harness that proves the protocol and
measures it, and an industrial deployment that must survive real disks, real
crashes, and real operators. The machinery is one body of code; the
environments are interchangeable implementations of the three traits.

The boot fence is the fourth mounting point and not a fourth contract of
this crate's own: the boot gate's marker store is upstream's
`LifecycleStore` (`vrr::lifecycle`), and the machinery here routes the
boot's classification and the marker machine's rounds through it. The
embedding API, at the end of this document, states the full mounting
grammar — the three traits and the fence — as one public constructor.

## The traits

- **`Disk`** — every byte of disk access. Open, read, write, append, flush,
  sync, rename, remove, create-dir, read-dir, metadata. Flush-to-userspace and
  sync-to-device are distinct operations and are never conflated: a journal
  that fsyncs and a trace that merely flushes are different durability
  statements, and the trait says which is which.
- **`StateStore`** — the lock table's persistence: `flush` a snapshot out,
  `load` a snapshot back. The semantics are the law of the next section.
- **`CommitHook`** — a callback invoked at each applied commit. The machinery
  knows nothing about what the callback does. It is the experimental
  harness's failure-injection point and the industrial side's trigger for
  amortized maintenance.

## The persistence law

These three rules are the contract, stated once and binding on every
implementation of `StateStore`:

1. **Flush is eager on the shutdown path.** A clean stop writes the whole
   lock-table snapshot to disk, in the foreground, after the drain window
   closes and before the stop completes. A stop that cannot flush is a failed
   stop: the failure is surfaced through the same channel as any other failed
   stop, never silently skipped.
2. **Load is lazy on the regular path.** Boot never loads eagerly. The table
   starts empty; the first demand that finds the table empty, behind a
   clean-stop verdict from the boot gate, performs exactly one load. Until
   that demand arrives the snapshot on disk is unread.
3. **A crashed boot distrusts the state file.** When the boot gate's verdict
   is crashed, the snapshot is unread regardless of what it contains, and the
   node rebuilds from the replica stream. Only a clean-stop file is loadable.
   A snapshot that fails its own validation loads as nothing, never as an
   error that blocks boot.

## The commit hook

`CommitHook` is a callback invoked at each applied commit. The machinery
knows nothing about what the callback does, and the contract deliberately
says nothing about it: the hook is generic machinery, and whatever rides on
it is the consumer's business.

Two consumers are named. The experimental harness uses the hook to kill a
node at a named commit — by whatever means it chooses, which is its own
affair — so crash-reload is exercised without failure detection in the loop:
instead of waiting on timeouts and suspicion, the harness names the commit at
which a node dies and then reloads it. The industrial side uses the same hook
for amortized maintenance: a flush of the state snapshot spread over new
commits — every N commits, TigerBeetle-style — so the eager shutdown flush
rarely carries a large backlog, and for logging.

## The division of labour

**The experimental side** (the advisory-locks demo in `uvrr-core`) ships the
machinery with the least environment it can get away with:

- trivial `Disk` and `StateStore` implementations — plain files, no AOF, no
  real superblock;
- induced timeouts, driven by the harness — no failure-detection machinery in
  the loop;
- crash-at-a-named-commit through the commit hook, and crash-reload, in a
  plain harness, run distributed across a few nodes to take timings;
- maelstrom-style verification of the machinery itself.

**The industrial side** (this repository) mounts the heavy environment onto
the same machinery through the same traits:

- the tbio-core storage engine, already behind the marker store's FFI
  boundary, and the industrial `Disk` and `StateStore` implementations;
- real timeouts and failure detection;
- amortized flushing and logging driven by the commit hook;
- the AOF, the event journal, and the web console, bridge, and launch
  tooling.

## What the experimental side is not asked to build

No compliance suite. No corpus machinery. No FFI or cdylib surface. No
industrial durability. The demo proves the machinery and takes its timings;
experimentation and industrialisation are not conflated, in either direction:
the experiment carries no industrial weight, and the industry carries no
experimental scaffolding.

## The consumption shape

This repository consumes `uvrr-core` as a plain git-tag dependency —
`vrr = { package = "uvrr-core", git = "…", tag = "…" }`. There is no
submodule and no `[patch]` override: the tag in the manifest is the code that
builds, and a version bump is a one-line manifest diff that review sees as a
version bump. When the upstream demo ships, the tag bumps, and the industrial
implementations mount onto the tested core without the machinery changing.

## The embedding API

An embedder mounts its environment on a real node through one public door,
`Node::open_with_seams`, beside the defaulting entry points and never
instead of them. Its parameters after the C ABI's own grammar — the member
descriptor, the local member's name, the state path, the journal directory
and roll threshold, the primary timeout — are the four things the machinery
is written against, and nothing else:

- **`disk: Arc<dyn Disk>`** — every byte of disk access: the journals, the
  view record, the state file, everything the construction and the run
  touch.
- **`state_store: Box<dyn StateStore>`** — the lock table's persistence,
  under the persistence law.
- **`commit_hook: Arc<Mutex<dyn CommitHook>>`** — the trigger at each
  applied commit.
- **`fence: Option<Box<dyn LifecycleStore<Error = io::Error>>>`** — the
  boot fence, the boot gate's marker store and the fourth touch point.
  `None` keeps the industrial default: the superblock quorum store over
  the state path, the Zig engine's quorum-of-copies construction with the
  single-file projection mirror. `Some(store)` mounts the embedder's own
  `LifecycleStore`: the boot's classification reads it, the marker
  machine's rounds — the first-life latch, the halt's `Stopping` round
  and its drain-proven `Stopped` round, the crashed boot's deferred
  latch — go through its `commit`, and the halt's drain rides its `drain`
  beside the event journal's own drain. What an embedder mounts is the
  `LifecycleStore` itself, the trait the boot-gate chapter defines; the
  marker-round schedule and the projection mechanics stay the machinery's
  and are never the embedder's to rewrite. A mounted fence reports
  `io::Error`, the object shape's pinned failure type.

The trait names hang off `spi`, the seam's front door: `LifecycleStore` is
re-exported there beside the three traits, so the whole mounting grammar is
nameable from one surface.

What stays defaulted is everything else: the member grammar and its
refusals, the identity law, the boot-gate classification and its census
tape, the halt schedule's marker rounds, the drain window, the eager flush
and the lazy load. The seams are the only things an embedder mounts; the
machinery above them is one body of code, and an embedder changes nothing
above them.

The C ABI stays default-only. `lunet_lock_node_new` and every other C entry
point build the industrial defaults exactly as `Node::open` does; no C
surface exists for the seams: an embedder that wants its own environment
binds the Rust API.
