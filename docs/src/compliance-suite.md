# The compliance corpus suite

The upstream uVRR compliance corpus is a machine-checkable statement of
the protocol's obligations: 73 cases across ten families, each a setup
script, one input, and the expected post-state. This tree replays the
whole corpus over the transport of `docs/uvrr-host-compliance.md` §8,
from its own host, and the run is a gate on every `make check`.

The corpus is upstream data at its pinned tag (`ext/uvrr-core`); the
`hurl` suite beside it is its own driver. Neither is copied here, and
neither is edited: the case arrives as the request body and the host
answers with what its own replica did.

## What runs

```
make compliance-suite
```

boots the compliance host, waits for its health probe, drives the suite
against it, and stops the host. It is two shapes, in this order:

| shape | what it proves |
|---|---|
| `production` | the production library's symbol table carries **zero** `unsafe_` symbols — the locked door of [the Compliance ABI](compliance-abi.md) is still shut |
| `compatibility_suite` | the gated library carries all nine `unsafe_*` exports, and the whole corpus replays over the transport |

`make check` runs both, so the production door is re-proved on every
gate run and the corpus is replayed on every gate run. The two shapes
build into different target directories: the gated library is never
written over the production cdylib under `target/release`, so the door
check reads the library a production boot would load.

`contract.hurl` runs first — it covers the endpoints that replay no
case, so a transport fault names itself before any case is reached — and
the ten `corpus-*.hurl` files follow.

## The host

`build/compliance_server.lua` is the host, a `compatibility_suite`
build's own process. It boots its own in-process cluster of adapter
nodes and speaks HTTP/1.1 + JSON to a client, one request per
connection. Its port is separate by construction from every lock's
client port: no lock client protocol passes through it, and it holds no
client listener of its own.

It answers the §8 endpoints exactly:

| endpoint | method | answer |
|---|---|---|
| `/health` | `GET` | `200` with the corpus host's identity and this build's own stamp and feature shape |
| `/session` | `POST` | `200 {"session": "<id>"}` — the session is a provisioned cluster |
| `/session/<id>/op` | `POST` | `200 {"ok": true}`, or `{"ok": false, "error": <reason>}` |
| `/session/<id>/capture` | `GET` | `200` with the full capture, drained to quiet |
| `/case` | `POST` | `200` with `id`, `family`, `verdict`, the mismatch when there is one, and the capture |
| `/case`, `/` | `GET` | `405` — the case endpoint takes POST |
| an unknown session | any | `404 {"error": "no such session"}` |
| an unparseable body | any | `400` with the codec's own reason |

A session holds its own cluster and its own marker-store names, so one
session never observes another's state.

## The verdict

`pass` comes from comparing the host's own capture against the
`expect` the request carries, field by field, and from nothing else.
That is §8.3 and it is how the host is written: `run_case` replays the
case against real adapter nodes through the Compliance ABI and captures
what they did; `assert_expectation` then compares the delivery sequence
exactly and each named post field exactly, in the order the reference
executor compares them. There is no path from a case id to a verdict, no
special-cased case, and no regenerated fixture — a codec divergence
surfaces as a `fail` with a mismatch naming the field, never as a
rewritten expectation.

An absent field is unconstrained on either side: the capture of a node
that is down carries its identity and its marker schedule and nothing
else.

## The executor

`src/compliance_executor.tl` is the abstract host interface of §2 over
the Compliance ABI, and it is held to the reference executor
(`ext/advisory_lock/tests/compliance/mod.rs`) operation for operation:
the same fourteen operations, the same FIFO wire, the same bounded
settle and drain, the same per-node post record, the same rendering
grammars, and the same field-by-field comparison order. Where the
reference reaches a `Node` method directly, this reaches the same method
across the C ABI; everything else is the same derivation.

The determinism is the executor's own: a logical clock advanced once per
tick sweep and carried by every drive between those advances, no wall
clock anywhere, and one marker-store file per identity per case. The
case's own run directory names the run and the case number inside it
names the case, so a case never opens a marker store a previous run left
behind.

## What the suite proves, and what it does not

It proves that this tree's compliance host, over the Compliance ABI,
reaches the same verdict as the reference executor on every case in the
upstream corpus, on the upstream driver's own transport, with the
upstream expectations.

It does not prove that the protocol is correct: the corpus is a
statement of obligations, and a host that agrees with it has agreed
with a corpus, not with the world. It does not run in production: it is
the one lane that boots the shape carrying write-capable protocol
entries, and it is gated behind the same feature the [Compliance
ABI](compliance-abi.md) documents.