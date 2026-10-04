# RFC: an application protocol over uVRR

## Status

This document is the specification. It defines the terms and the normative
obligations for an application protocol carried over `uvrr-core`, and it is
written to be built against.

## The problem this solves

`uvrr-core` orders opaque operations and nothing else. It does not model
clients, sockets, retries, deduplication, forwarding, or replies
(`ext/uvrr-core/docs/uvrr-durability-model.md:686`); its own architecture
decision B2 removed the VRR-2012 per-client table and made duplicate policy,
result caching and reply delivery the host's concern
(`ext/uvrr-core/docs/architecture.md:610-652`, and
`ext/uvrr-core/docs/uvrr-durability-model.md:733`). Everything above that
line is unspecified, and a host that invents it without naming its roles
invents the same confusion twice. The core names the **host** — the system
above it that it cannot know (`ext/uvrr-core/docs/architecture.md:3,34,151`)
— but has no name for the role within a host that terminates alien
application traffic. This RFC supplies both.

The upstream note now names it too: `docs/uvrr-application-protocol.md`
(carried by uvrr-core v0.14.0, landed with this document as its worked
example) adopts the same terminology and obligations 1 through 5 below, and
states the balancer rule and the datagram shape as this document's balancer
and datagram sections do. Two obligations this document carries are not in
the upstream note and remain in force here: the sweeper and the release of
every nexus entry and socket on every exit path, and the join's audit
metadata committed under the same `session_id` that the bearer derives from.

## Terminology

The key words MUST, MUST NOT, REQUIRED, SHALL, SHOULD, SHOULD NOT, RECOMMENDED,
MAY and OPTIONAL are to be interpreted as described in RFC 2119.

**application** — the party on the far side of a network that is unknown to
uVRR and unknown to this service: a phone application, a browser, another
machine. An application is never referred to as a client. The service never
holds an application identity.

**gateway** — the role a node holds when it terminates alien application
traffic. A gateway accepts an application connection, holds the session that
connection carries, and forwards that session's commands to the replication
protocol. Every node is a gateway in turn; the role is not a process, a
binary, or a deployment unit.

**host** — the distributed system that runs an application protocol over
uVRR. A host is one or more processes and is not a single thing.

**session** — the state a gateway holds for one application connection's
command stream, identified by a `session_id`.

**session_id** — the unsigned 64-bit identity of a session, unique across the
cluster for the lifetime of the cluster. A session's `session_id` IS the log
slot committed by the command that established it.

**counter** — the unsigned 64-bit per-session sequence number, owned by the
gateway, incremented once per command accepted on that session.

**uuid** — the 128-bit correlation identity of one command:
`pack_128(session_id, counter)`. The low 64 bits are the counter, the high 64
bits the `session_id`.

**bearer** — the unforgeable token derived from a `session_id` that an
application presents to prove which session it holds. A `session_id` never
travels to an application in the clear.

**nexus** — a gateway's map from a command's `uuid` to the opaque socket
handle the command arrived on, together with the command in flight.

## Normative obligations

A gateway MUST satisfy all of the following.

1. A gateway MUST offer at most one outstanding command per session. The
   reason is ordering, not throughput: a command written before its reply has
   been read risks being reordered past it. A second, different command
   arriving on a session that already holds one MUST be refused by name, never
   queued behind it. A re-send of the command that IS outstanding — the same
   `uuid` — is not a second outstanding command; it attaches to the command
   already in flight.
2. A gateway MUST assign `uuid = pack_128(session_id, counter)` and MUST
   increment `counter` exactly once per accepted command. The `uuid` is
   therefore unique by construction, and two nodes can never compute the same
   one.
3. A gateway MUST NOT let an application observe a `session_id` in the clear.
   A `session_id` MUST reach an application only as a bearer, which an
   attacker cannot invert to enumerate adjacent sessions.
4. A gateway MUST remove a nexus entry before writing the command's result,
   and MUST NOT write a command's result twice. This is the at-most-once
   property and it is structural: the result is written from the nexus entry,
   the entry is taken, uVRR commits each operation exactly once, and a crash
   between the take and the write destroys the session — leaving nothing to
   write the result from. A gateway MUST NOT attempt to retransmit a result.
5. A gateway MUST treat a timeout, a closed socket, or an I/O error as
   UNKNOWN. It is not evidence that the command did not happen, only that the
   outcome was not learned. The application re-drives by establishing a new
   session; a re-drive is a new command, never a resurrection of the old one.
6. A gateway MUST time out and drop inactive sessions, and MUST release every
   nexus entry and socket it owns on shutdown, on a refused membership change,
   and on any error path, so that no entry outlives its socket.
7. A gateway MUST record the audit metadata an application supplies — a
   client-info header or its equivalent — into the committed record that
   establishes the session, keyed by the same `session_id`. The audit metadata
   MUST NOT be a substitute for the bearer.

An application MAY be written against any transport: TCP, HTTP/1.1, HTTP/2 or
HTTP/3. Where the transport is a datagram transport, a gateway MUST
re-establish the ordering the session depends on, by allocating a counter per
session under that session and forwarding that session's commands to the
leader as one unit. A datagram gateway has no per-session socket; the nexus
then maps the `uuid` to whatever handle the gateway holds, and the same
obligations 1 through 7 apply unchanged.

A gateway MAY be placed behind a layer-4 load balancer. The balancer
terminates nothing: it distributes connections, and it MUST NOT be relied on
for session affinity. A session's authority is the leader's, not the
connection's. If the balancer may route an application to a different
gateway, that gateway will not hold the session, the application MUST
establish a new one, and the old session is gone with the gateway that held
it.

## The session lifecycle

A session is established by a join command whose commit records the
application's audit metadata. The committed slot is the `session_id`. The
gateway returns the bearer for that `session_id` to the application; the
`session_id` itself never leaves the cluster.

While the session lives, the gateway holds it in memory. One outstanding
command per session; the nexus carries the command in flight and the handle to
answer on.

When the connection drops, whether by the application, by the balancer, or by
the gateway's own crash, the session is over. An application that
re-establishes connectivity presents its bearer. If the same gateway still
holds the session, the session continues; if it lands on another gateway,
there is no session to find, and the application runs the join again to
obtain a new `session_id` and a new bearer. Nothing about that is a failure
and nothing about it is retried inside the cluster: a new `session_id` cannot
repeat a used one.

## What the replication protocol owes the session

uVRR commits each opaque operation exactly once and in one order. That is what
makes obligation 4 structural rather than defended: the commit is the
commit, and the reply is a courier for it that may be lost but never
duplicated. The gateway supplies everything the core deliberately does not —
identity, ordering, deduplication, delivery — and the session is the unit it
supplies them in.

## Why not one request per connection

A connection is not a session. Connections terminate; the balancer terminates
them; the network drops them. The session is the thing that survives a
re-drive, and the `session_id` is what makes a re-drive distinguishable from
the original. A gateway that treated a connection as its identity would make
every reconnect a new identity and lose the audit trail that links an
application's commands across reconnects.