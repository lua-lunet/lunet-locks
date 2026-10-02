# The Compliance ABI and the information console

A production build of the native library exposes one thing beyond the
lock's own client protocol: a read-only console that says what the build
is. Everything else that could write protocol state is compiled out of
the production library entirely, and a symbol-table gate proves it on
every test run.

## The read-only console

`lunet_lock_version_properties` returns the build's own identity facts
as a Maven `version.properties` payload: LF-terminated `key=value`
lines, NUL-terminated for the host, written on the same pull-style
buffer contract as every other vector-returning export (`TOO_LARGE` plus
the needed length, never a heap allocation across the boundary).

| key | value |
|---|---|
| `version` | the release tag naming this build, or `unknown` |
| `sha` | twelve hex digits of the commit this build was made from, or `unknown` |
| `dirty` | `true` when the working tree carried uncommitted changes at build time |
| `feature_shape` | `production`, or `production` plus each development feature the build carries (`+compatibility_suite`, `+flight-recorder`) |

Every value is stamped at BUILD time by the crate's build script from
git facts, so the answer is identical on every host that loaded the
library and needs no build host at runtime.

`version` names the tag this build **is**, not the nearest tag it can
reach: a build whose HEAD is not exactly at a tag reports `unknown`.
That is what a development tree reports, and it is what the Docker build
context reports — the committed-tree snapshot ships without `.git`, so
the gate hands the commit it ships through `LUNET_LOCKS_HEAD` and the tag
is not recoverable. A build made in a checkout at a tag — a tagged CI
build, a release build — reports the tag. An unavailable fact is
reported `unknown` and never guessed; a build whose git facts cannot be
read at all is stamped `dirty`, because an unproven clean tree is never
reported clean.

`GET /info` is exactly that payload. The crate renders the complete
HTTP/1.1 response (status line, `Content-Type`, `Content-Length`,
`Connection: close`, body) as a pure function over the stamps, so there
is exactly one derivation of the text; the socket that carries it belongs
to the host and is separate from the lock's client port.

### What the console deliberately does not do

The console is read-only **by construction**, not by convention:

- the export takes no node handle, so no path through it can reach a
  `Node` at all;
- `src/info.rs` holds no state and no reference to anything mutable —
  the values are compile-time constants and the two functions over them
  are pure;
- there is no request parsing, so there is no verb but `GET` and no
  request that mutates anything.

There is no path through the console that writes protocol state, arms
the compliance rules, mutates the marker store, or reaches the
filesystem.

## The locked door: the Compliance ABI

The upstream compliance corpus drives the node through an abstract host
interface that the production boundary deliberately does not offer: the
corpus's raw payloads commit without a Service decode, its clock is the
executor's logical tick rather than the wall clock, and it reads the
journal, the folded configuration and the boot-gate marker schedule
directly. Those nine entry points are the **Compliance ABI**, and every
one of them is named `unsafe_*`.

They live behind the `compatibility_suite` cargo feature, which is OFF in
`default`. With the feature off they **do not compile**: a production
library carries no such symbol at all. That is an absence, not a runtime
refusal — there is no production code path that checks a flag and says
no.

| export | wraps |
|---|---|
| `lunet_lock_node_unsafe_open_compliance` | `Node::open_compliance` |
| `lunet_lock_node_unsafe_set_compliance_clock` | `Node::set_compliance_clock` |
| `lunet_lock_node_unsafe_propose_opaque` | `Node::propose_opaque` |
| `lunet_lock_node_unsafe_reconfigure_opaque` | `Node::reconfigure_opaque` |
| `lunet_lock_node_unsafe_frontiers` | `Node::frontiers` |
| `lunet_lock_node_unsafe_journal_entries` | `Node::journal_entries` |
| `lunet_lock_node_unsafe_membership` | `Node::membership` |
| `lunet_lock_node_unsafe_witnesses` | `Node::witnesses` |
| `lunet_lock_node_unsafe_marker_log` | `Node::marker_log` |

Four of the nine write protocol state (`open_compliance` creates a node
and its durable markers; `set_compliance_clock` moves the node's clock
source and through it every subsequent drive's timeout arithmetic;
`propose_opaque` and `reconfigure_opaque` are proposals into the
replicated log, and a committed reconfiguration moves the folded
configuration). The other five read the state the corpus's expectations
are written against: the frontiers, the journal entries, the folded
configuration's record, the witness list and the marker-round schedule.

The opaque reconfiguration crosses the ABI as the core's own JSON
encoding of `SystemOperation`; the journal crosses as the core's own
JSON encoding of its entries. `compatibility_suite` enables the
`vrr/serde` feature that carries those derives, and the default shape
does not pull it in.

### The door is proven, not asserted

`ext/advisory_lock/tests/abi_door_test.rs` builds BOTH feature shapes,
reads each cdylib's symbol table with the platform's `nm`, and fails if

- the default (production) shape exports **any** `unsafe_` symbol;
- the `compatibility_suite` shape is missing any of the nine; or
- either shape is missing `lunet_lock_version_properties`, the console a
  production build is entitled to.

The gate runs on every `make ext-test`, in both feature shapes (see
[build and tests](build-and-tests.md)).

### A `compatibility_suite` build is never booted in production

The shape is a development shape and says so twice:

- the build script holds the clean-commit guard. A `compatibility_suite`
  build must come from a clean commit — a compliance verdict is evidence
  about a commit, and a dirty tree names no code state — and the guard is
  overridden with `COMPATIBILITY_SUITE_ALLOW_DIRTY=1`, which still stamps
  the build `dirty: true` and prints a build warning naming the
  exposure.
- every node boot announces the exposure at error severity, before
  anything is constructed, naming the nine exports and stating that a
  production boot of this shape is a misconfiguration. The announcement
  fires once per process on every construction path.

The released build is the default shape. In that shape the exports do not
exist, and the only added surface is the read-only console.
