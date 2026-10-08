---
title: "VRR-2012 reading group: what was said, mapped onto what the paper says"
name: vrr-2012-reading-group-summary
description: >-
  Sentence-by-sentence mapping of two passages from a Viewstamped Replication
  Revisited reading-group talk onto the paper's own text, with a page and
  layout-line citation for every claim, a verdict per sentence, and a closing
  statement of what the paper does and does not say. Covers client-request
  idempotency and reconnection, and host restart / crash durability.
type: reference
tags:
  - viewstamped-replication
  - vr
  - vrr-2012
  - client-table
  - idempotency
  - reconnection
  - durability
  - recovery
  - checkpointing
  - view-change
  - tigerbeetle
sources:
  talk: "https://charap.co/reading-group-viewstamped-replication-revisited"
  paper: "https://dspace.mit.edu/entities/publication/80846d94-fcd3-40e6-87fb-8d91fe99a5d1"
  paper_local: "uvrr-core v0.14.0: research/primary-sources/papers/liskov-cowling-vr-revisited-2012.pdf"
  paper_sha256: 1b16284a0a443d08992bd0fd0f032587e34e3e81f61f1871d39a4e4a6e22cfa6
attribution:
  presenter: "Joran Dirk Greef (TigerBeetle)"
  presenter_claim: >-
    presented the protocol along with bits of his engineering experience using
    the protocol in practice; the source does not claim authorship
  paper_authors: "Barbara Liskov, James Cowling — MIT-CSAIL-TR-2012-021"
  talk_record: "Aleksey Charapko, \"Reading Group. Viewstamped Replication Revisited\", 8 October 2021"
method:
  extraction: "pdftotext -layout (layout-preserving) and pdftotext (reading order)"
  line_numbers: >-
    against vrr2012-layout.txt, sha256
    50faadfe230bdae252883c930c9f3125b2dbe0e01426d57f1cd31b3fadbac87e
  read: "whole paper, block by block, in document order"
---

# VRR-2012 — Sentence-by-sentence mapping of the two quoted passages

## 0. Source of the two passages, and the presenter's name

Both quoted passages are verbatim from the talk record named in the pass brief:

> Aleksey Charapko, "Reading Group. Viewstamped Replication Revisited", 8 October 2021,
> <https://charap.co/reading-group-viewstamped-replication-revisited>

The presenter's name, **as the source document states it**, is **Joran Dirk Greef**
(TigerBeetle). The source document's own sentence:

> "Our 74th paper was a foundational one — we looked at Viestamped Replication protocol
> through the lens of the '[Viewstamped Replication Revisited]'
> paper. **Joran Dirk Greef** presented the protocol along with bits of his engineering
> experience using the protocol in practice."

Page fetched and read for this pass to confirm the two passages verbatim; both appear word
for word, in the order given below, in the two consecutive paragraphs that follow the
view-change schematic image.

Note on framing, stated plainly because the brief's framing and the document's own text do not
quite line up: the source document names the presenter as Joran Dirk Greef. It does not claim
he is an author of the paper; the paper is by Barbara Liskov and James Cowling (layout
L3). The source document's claim about the presenter is about his engineering practice with
the protocol, not authorship. That is what this mapping treats the passages as: the record of
what was said in the room about this paper, as written down by the session's author.

The paper side of the mapping is the extraction named in
`.tmp/margin/vrr2012-block-read.md`:

```sh
pdftotext -layout …/liskov-cowling-vr-revisited-2012.pdf /Users/Shared/lua-lunet/lunet-locks/.tmp/margin/vrr2012-layout.txt
# sha256 50faadfe230bdae252883c930c9f3125b2dbe0e01426d57f1cd31b3fadbac87e
pdftotext        …/liskov-cowling-vr-revisited-2012.pdf /Users/Shared/lua-lunet/lunet-locks/.tmp/margin/vrr2012-plain.txt
# sha256 6380087df7b362adb4551ba2a21168aeb2e8adc41a2f653cffc3103b185d152e
```

Verdict vocabulary: **FAITHFUL** — the paper states the substance, in the paper's own words,
at the cited location, with no material added or dropped. **LOOSER** — the paper says
something that covers the substance but with different wording, different scope, a different
mechanism, or a stated caveat the passage leaves out. **NOT IN THE PAPER** — no passage in
the paper supports it. **GOES BEYOND THE PAPER** — the paper supports part of it and the rest
is the presenter's own addition (a claim, a mechanism, or an inference the paper does not
make).

---

## 1. Passage 1 — the "idempotence of client requests" passage

As quoted, in full:

> "There is a bit more stuff going on in the view change procedure. For example, VR ensures
> the idempotence of client requests. If some client request times out, the client can resend
> the operation to the cluster, and the new leader will either perform the operation if it has
> not been done before or return the outcome if the operation was successful at the old
> primary."

Six sentences.

### 1.1 — "There is a bit more stuff going on in the view change procedure."

**Verdict: NOT IN THE PAPER** (as content; it is a framing remark, not a claim about VR).

The paper makes no such remark. What it does say immediately after describing the view-change
protocol is the expense note, and it is specific rather than gesturing:

> "The protocol as described is expensive because the log is big, and therefore messages can
> be large. The approach we use to reduce the expense of view changes is described in
> Section 5." — p. 5, §4.2, layout L335–340 col L (plain L485–487)

So the "more stuff" the sentence gestures at is real and is in the paper, but the paper's
"more stuff" in §4.2 is about *expense*, not about client requests. The presenter moved the
client-idempotence material under the view-change heading; the paper files it in §4.1
(Normal Operation), the first of its three sub-protocols (p. 3, §3, layout L155–163 col L,
plain L228–234: "Normal case processing of user requests. • View changes to select a new
primary. • Recovery of a failed replica so that it can rejoin the group.").

### 1.2 — "For example, VR ensures the idempotence of client requests."

**Verdict: FAITHFUL in substance; NOT IN THE PAPER as worded.**

The substance is in the paper, stated as a purpose of the request-number rather than as a
named property:

> "The request number is used by the replicas to avoid running requests more than once; it is
> also used by the client to discard duplicate responses to its requests." — p. 3, §4
> (intro), layout L141–143 col R (plain L191–194)

and, as the mechanism:

> "The client-table. This records for each client the number of its most recent request, plus,
> if the request has been executed, the result sent for that request." — p. 4, Figure 2,
> layout L189–192 col L (plain L292–293)

The word is the discrepancy, and it is a real one. `rg -i idempot` over both extractions of
the whole 14-page paper returns exactly one hit:

> "(Re-running operations can cause the application state to be incorrect unless the
> operations are idempotent.)" — p. 7, §5.1, layout L416 col R (plain L570–571)

That sentence is about the *application's* operations being re-executed after a recovery —
Topic B, not Topic A. The paper never writes "idempotence of client requests" or "idempotent"
of client requests. So: the presenter's word is the presenter's; the mechanism he is pointing
at is the paper's, and the paper describes it without the word.

### 1.3 — "If some client request times out, the client can resend the operation to the cluster"

**Verdict: FAITHFUL**, with one word of difference ("the cluster" for "all replicas").

> "If a client doesn't receive a timely response to a request, it re-sends the request to all
> replicas. This way if the group has moved to a later view, its message will reach the new
> primary." — p. 4–5, §4.1 closing, layout L226–228 col R continuing at L232 col L (plain
> L337–341)

The paper's version is stronger in one respect and narrower in another. Stronger: the paper
says the resend goes to *all* replicas and explains the routing consequence, whereas "the
cluster" is vaguer. Narrower: the paper conditions the resend on the client not receiving a
*timely response* — a timeout — exactly as the presenter says. The sentence immediately after
supplies the reason the target is "the cluster" rather than a remembered primary:

> "Backups ignore client requests; only the primary processes them." — p. 5, §4.1, layout
> L228–232 (plain L341–342)

### 1.4 — "and the new leader will either perform the operation if it has not been done before"

**Verdict: FAITHFUL** (as the new primary's behaviour on a request it has not seen).

> "3. The primary advances op-number, adds the request to the end of the log, and updates the
> information for this client in the client-table to contain the new request number, s." — p. 4,
> §4.1 step 3, layout L206–210 col L (plain L313–316)

> "4. The new primary starts accepting client requests." — p. 6, §4.2 step 4, layout L289 col L
> (plain L431)

The paper is more precise about the condition than the presenter: a request is performed if
its request-number is *bigger* than the one in the client-table, not merely "if it has not
been done before". A request-number can be absent from the table yet have been performed and
then lost from the table only if the table were reset — which the paper does not permit, since
the table is carried in the log and shipped in `STARTVIEW` (see 1.5). The presenter's phrasing
is a fair informal rendering of the paper's rule.

### 1.5 — "or return the outcome if the operation was successful at the old primary."

**Verdict: FAITHFUL in substance; GOES BEYOND THE PAPER in one detail** (the new-leader half is
the presenter's addition; the "old primary" attribution is not the paper's).

The "return the outcome" half is stated twice by the paper, and it is a *re-send of a cached
response*, not a recomputation:

> "2. When the primary receives the request, it compares the request-number in the request
> with the information in the client table. If the request-number s isn't bigger than the
> information in the table it drops the request, but it will re-send the response if the
> request is the most recent one from this client and it has already been executed." — p. 4,
> §4.1 step 2, layout L200–207 col L (plain L298–302)

> "The primary also updates the client's entry in the client-table to contain the result." —
> p. 4, §4.1 step 5, layout L199–200 col R (plain L289–290)

The new-leader half — a *new* primary answering from the state it inherited rather than
executing afresh — is the presenter's, but the paper has exactly the sentence he is
paraphrasing, one line long:

> "4. The new primary starts accepting client requests. It also executes (in order) any
> committed operations that it hadn't executed previously, updates its client table, and sends
> the replies to the clients." — p. 6, §4.2 step 4, layout L289–292 col L (plain L431–434)

And the paper is explicit that the client table, not merely the log, is what travels:

> "5. When other replicas receive the S TART V IEW message, they replace their log with the one
> in the message … and update the information in their client-table." — p. 6, §4.2 step 5,
> layout L294–305 col L (plain L465–472)

The one detail that goes beyond: "at the old primary". The paper never says the prior
outcome was produced at the *old* primary specifically. It says the client-table records the
result "if the request has been executed", and it requires the *primary* to write the result
into the table (layout L199–200 col R) — but backups also write it (layout L221–222 col R:
"increments its commit-number, updates the client's entry in the client-table, but does not
send the reply to the client"), and §3 says the primary waits for f+1 `PREPAREOK`s before
executing (layout L136–142 col L). So the paper guarantees the outcome is in a quorum's
tables, and which of them the client talks to next is not fixed. "at the old primary" is a
reasonable reading of the common case and is not the paper's guarantee.

### 1.6 — (the passage's own subordinate clause, folded into 1.5)

For completeness, the paper's liveness-side restatement of the same rule, which the passage
does not cite:

> "Additionally, clients send their requests to all replicas if they don't hear from the
> primary, and thus cause requests to be executed in a later view if necessary." — p. 12, §8.1
> Liveness, layout L739–742 col R (plain L1052–1055)

And the client-restart rule, which is the reconnection case the passage does not mention:

> "If a client crashes and recovers it must start up with a request-number larger than what it
> had before it failed. It fetches its latest number from the replicas and adds 2 to this value
> to be sure the new request-number is big enough. Adding 2 ensures that its next request will
> have a unique number even in the odd case where the latest request it sent before it failed
> is still in transit…" — p. 7, §4.5, layout L421–427 col L continuing L371–373 col R (plain
> L606–612)

---

## 2. Passage 2 — the recovery and view-change optimisation passage

As quoted, in full:

> "Another important part of the paper's recovery and view change discussion involves practical
> optimizations. For example, recovering a crashed in-memory replica requires learning an entire
> log, which can be slow. The proposed solution involves asynchronously persisting log and
> checkpoint to disk to optimize the recovery process. Similarly, view change requires log
> exchange, which is not practical. An easy solution would involve sending a small log suffix
> instead since it is likely that the new primary is mostly up-to-date."

Six sentences.

### 2.1 — "Another important part of the paper's recovery and view change discussion involves practical optimizations."

**Verdict: FAITHFUL** — and it is very nearly the paper's own sentence.

> "The description of the protocols presented in the previous section ignores a number of
> important issues that must be resolved in a practical system. In this section we discuss how
> to provide good performance for node recovery, state transfer, and view changes. In all three
> cases, the key issue is efficient log management." — p. 7, §5 opening, layout L376–383 col R
> (plain L540–544)

The paper's §5 is titled "Pragmatics" (layout L376 col R) and covers exactly recovery, state
transfer, and view change. "In all three cases, the key issue is efficient log management" is
the paper's own diagnosis, and it is the causal link the passage leaves implicit: the same
oversized log causes both the recovery cost and the view-change cost.

### 2.2 — "For example, recovering a crashed in-memory replica requires learning an entire log, which can be slow."

**Verdict: FAITHFUL in substance; one word is the presenter's ("in-memory").**

> "When a replica recovers from a crash it needs to recover its log. The question is how to do
> this efficiently. Sending it the entire log, as described in Section 4.3, isn't a practical
> way to proceed, since the log can get very large in a long-lived system." — p. 7, §5.1,
> layout L388–391 col R (plain L550–553)

> "The protocol is expensive because logs are big and therefore the messages are big. A way to
> reduce this expense is discussed in Section 5." — p. 7, §4.3, layout L371–373 col L (plain
> L522–523)

The paper says "isn't a practical way to proceed"; the presenter says "can be slow". Same
object, and the paper's is the stronger claim. The word "in-memory" is the presenter's: `rg
-i 'in-memory|in memory'` over the extraction returns no hit describing a replica. The
paper's words are "volatile" — "the protocol uses the volatile state at f +1 replicas as
stable state" (p. 13, §8.2, layout L764–766 col L, plain L1133–1135) — and "forgotten", in
the passage that explains why a crashed node cannot simply resume:

> "When a replica recovers after a crash it cannot participate in request processing and view
> changes until it has a state at least as recent as when it failed. If it could participate
> sooner than this, the system can fail. For example, if it forgets that it prepared some
> operation, this operation might then be known to fewer than a quorum of replicas even though
> it committed, which could cause the operation to be forgotten in a view change." — p. 6,
> §4.3, layout L346–360 col L (plain L501–506)

"In-memory" is an accurate gloss on the paper's "volatile", not a word the paper uses.

### 2.3 — "The proposed solution involves asynchronously persisting log and checkpoint to disk to optimize the recovery process."

**Verdict: FAITHFUL in substance; two words are the presenter's ("asynchronously",
"persisting"); the paper is more specific about *what* is persisted and in *which order* the
two mechanisms are presented.**

The log side:

> "A way to reduce expense is to keep a prefix of the log on disk. The log can be pushed to
> disk in the background; there is no need to do this while running the protocol. When the
> replica recovers it can read the log from disk and then fetch the suffix from the other
> replicas. This reduces the cost of recovery protocol substantially." — p. 7, §5.1, layout
> L394–397 col R (plain L554–558)

The paper's word is **"in the background"**, and it is emphatic that this is off the hot path.
"Asynchronously" is the presenter's rendering; the paper's three uses of "asynchronous" are
all "an asynchronous network" (plain L35, L92, L121).

The paper also states the cost of the log-prefix approach before superseding it:

> "However the replica will then need to execute all the requests in the log (or at least those
> that modify the state), which can take a very long time if the log is big." — p. 7, §5.1,
> layout L397–402 col R (plain L558–560)

> "Therefore a better approach is to take advantage of the application state at the recovering
> replica: if this state is on disk, the replica doesn't need to fetch the prefix of the log
> that has already been applied to the application state and it needn't execute the requests in
> that prefix either. Note that this does not mean that the application is writing to disk in
> the foreground; background writes are sufficient here too." — p. 7, §5.1, layout L403–410
> col R (plain L561–567)

The checkpoint side:

> "Our solution to this problem uses checkpoints and is based on our later work on
> Byzantine-fault tolerance [2, 1]. Every O operations the replication code makes an upcall to
> the application, requesting it to take a checkpoint; here O is a system parameter, on the
> order of 100 or 1000. To take a checkpoint the application must record a snapshot of its state
> on disk; additionally it records a checkpoint number, which is simply the op-number of the
> latest operation included in the checkpoint." — p. 7, §5.1, layout L417–427 col R (plain
> L573–576)

So "log and checkpoint to disk" is right, and both are background writes in the paper's
framing. Two things the passage flattens that the paper does not: the paper presents the
*log prefix* as the first idea and then explicitly calls the *application snapshot* "a better
approach" (layout L403), and the checkpoint number is not merely something persisted — it is
the handoff value that the recovering node puts in its `RECOVERY` message so the primary sends
the log from that point on (p. 8, §5.1, layout L453–459 col L, plain L632–637). In other
words the paper's optimisation *shrinks* the log the recovering node must learn; the
"learning an entire log" of sentence 2.2 is the problem the checkpoint number removes.

### 2.4 — "Similarly, view change requires log exchange, which is not practical."

**Verdict: FAITHFUL** — near-verbatim in substance; the paper states it twice, once in each
of the two relevant sections.

> "The protocol as described is expensive because the log is big, and therefore messages can be
> large. The approach we use to reduce the expense of view changes is described in Section 5." —
> p. 5, §4.2, layout L335–340 col L (plain L485–487)

> "To complete a view change, the primary of the new view must obtain an up-to-date log, and we
> would like the protocol to be efficient: we want to have small messages, and we want to
> avoid adding steps to the protocol. The protocol described in Section 4.2 has a small number
> of steps, but big messages. We can make these messages smaller, but if we do, there is always
> a chance that more messages will be required." — p. 8, §5.3, layout L459–462 col R (plain
> L678–682)

Two notes. First, the word "Similarly" is the presenter's, and the paper earns it: §5 is
introduced as covering "node recovery, state transfer, and view changes" with "the key issue
[being] efficient log management" in all three cases (p. 7, §5, layout L376–383 col R, plain
L540–544) — the same oversized log is the cause in both. Second, the paper's version of
"not practical" includes a trade-off the passage omits: making the messages smaller "always a
chance that more messages will be required" (layout L461–462 col R). The paper is describing
a trade, not a free win.

### 2.5 — "An easy solution would involve sending a small log suffix instead"

**Verdict: FAITHFUL in substance; "easy" understates it relative to the paper.**

> "A reasonable way to get good behavior most of the time is for replicas to include a suffix of
> their log in their D OV IEW C HANGE messages. The amount sent can be small since the most
> likely case is that the new primary is up to date. Therefore sending the latest log entry, or
> perhaps the latest two entries, should be sufficient. Occasionally, this information won't
> be enough; in this case the primary can ask for more information, and it might even need to
> first use application state to bring itself up to date." — p. 8, §5.3, layout L468–478 col R
> (plain L683–691)

The paper's phrase is "A reasonable way to get good behavior **most of the time**" — not "an
easy solution". The "latest log entry, or perhaps the latest two entries" is the paper being
concrete about the suffix size, which the passage does not give.

The paper also has a second, separate appearance of the same quantity — deliberate *retention*
of a log suffix, so that a recovering node does not have to fall back on transferring
application state:

> "A large enough suffix of the log should be retained to avoid this problem." — p. 8, §5.1,
> layout L477–479 col L (plain L647–648)

### 2.6 — "since it is likely that the new primary is mostly up-to-date."

**Verdict: LOOSER** — the paper states this as a most-likely-case observation, not as an
assumption the design relies on, and the sentence the passage draws on is immediately followed
by a caveat the passage drops.

The paper's sentence is the one quoted at 2.5:

> "The amount sent can be small since the most likely case is that the new primary is up to
> date. Therefore sending the latest log entry, or perhaps the latest two entries, should be
> sufficient. **Occasionally, this information won't be enough; in this case the primary can
> ask for more information, and it might even need to first use application state to bring
> itself up to date.**" — p. 8, §5.3, layout L472–478 col R (plain L686–691)

So the paper's structure is: suffix is the common case, and the fallback is application state
— the same mechanism §5.1 uses for recovery. The passage states the premise and stops. The
word "assumption" is not the paper's either; the paper nowhere says the design *assumes* the
new primary is mostly up to date. The paper *relies* on f+1 replicas' logs for correctness
(p. 5, §4.2, layout L277–283 col L: "the view change protocol obtains information from the
logs of at least f + 1 replicas") and treats the mostly-up-to-date case as a performance
observation about which optimisation pays off most of the time.

---

## 3. Verdict summary

| Passage sentence | Verdict |
|---|---|
| 1.1 "There is a bit more stuff going on in the view change procedure." | NOT IN THE PAPER (framing; the "more stuff" is in the paper but is about expense, and the paper files idempotence under §4.1, not §4.2) |
| 1.2 "VR ensures the idempotence of client requests." | FAITHFUL in substance; the word is NOT IN THE PAPER (paper: "avoid running requests more than once", layout L141–143) |
| 1.3 "If some client request times out, the client can resend the operation to the cluster" | FAITHFUL (layout L226–228 + L232; paper says "to all replicas") |
| 1.4 "the new leader will either perform the operation if it has not been done before" | FAITHFUL (layout L289; L206–210) |
| 1.5 "or return the outcome if the operation was successful at the old primary." | FAITHFUL in substance; "at the old primary" GOES BEYOND THE PAPER (paper guarantees a quorum's client-tables, not the old primary specifically: layout L200–207, L199–200, L289–292) |
| 2.1 "…recovery and view change discussion involves practical optimizations." | FAITHFUL (layout L376–383, §5 "Pragmatics") |
| 2.2 "recovering a crashed in-memory replica requires learning an entire log, which can be slow." | FAITHFUL in substance; "in-memory" is the presenter's word (paper: "volatile", L764–766) |
| 2.3 "asynchronously persisting log and checkpoint to disk to optimize the recovery process." | FAITHFUL in substance; "asynchronously" is the presenter's word (paper: "in the background", L394–397, L403–410, L417–427) |
| 2.4 "Similarly, view change requires log exchange, which is not practical." | FAITHFUL (layout L335–340; L459–462) |
| 2.5 "An easy solution would involve sending a small log suffix instead" | FAITHFUL in substance; "easy" is stronger than the paper's "a reasonable way … most of the time" (layout L468–474) |
| 2.6 "since it is likely that the new primary is mostly up-to-date." | LOOSER — the paper states it as a most-likely-case observation and immediately adds the fallback the passage omits (layout L472–478) |

Twelve sentences mapped. Eight FAITHFUL, one FAITHFUL-with-a-beyond-the-paper-detail, one
FAITHFUL-in-substance-but-the-word-is-absent, one LOOSER, one NOT IN THE PAPER (a framing
remark).

---

## 4. What is in the paper and what is not

### In the paper, in the paper's own words

**Topic A — client request idempotency and reconnection.** The mechanism is fully specified,
in six places, and the specification is precise:

1. The client holds a client-id and a strictly increasing request-number, with one outstanding
   request at a time — p. 3, §4 intro, layout L136–139 col R.
2. The stated purpose of the request-number: "used by the replicas to avoid running requests
   more than once", and "used by the client to discard duplicate responses to its requests" —
   p. 3, §4 intro, layout L141–143 col R.
3. The state: the client-table records "the number of its most recent request, plus, if the
   request has been executed, the result sent for that request" — p. 4, Figure 2, layout
   L189–192 col L.
4. The decision: a request whose number is not bigger than the table's is *dropped*, and if
   it is the most recent and already executed, "it will re-send the response" — p. 4, §4.1
   step 2, layout L200–207 col L. This is detect-and-return, not re-execute.
5. The result is cached at execution (p. 4, §4.1 step 5, layout L199–200 col R) and the
   client-table is shipped to every replica in `STARTVIEW` (p. 6, §4.2 steps 4–5, layout
   L289–305 col L), which is why a *new* primary can answer a resent request from prior state
   rather than executing it again.
6. The timeout-and-resend rule, and the reason it goes to all replicas: "If a client doesn't
   receive a timely response to a request, it re-sends the request to all replicas… Backups
   ignore client requests; only the primary processes them." — p. 4–5, §4.1, layout L226–232.

The client-restart case is also in the paper, as its own subsection: fetch the latest number
from the replicas, add 2 — p. 7, §4.5, layout L421–427 col L.

**Topic B — host application restart / crash durability.** Also fully specified:

1. Five separate statements of the no-disk property — p. 1 §1 (L47–48), p. 1 §1 (L33–34),
   p. 4 §4.1 (L239–242), p. 6 §4.3 (L310–320), p. 14 §9 (L868–871) — and the crispest form of
   the thesis, "the protocol uses the volatile state at f +1 replicas as stable state" — p. 13,
   §8.2, layout L764–766 col L.
2. The recovery correctness condition and its motivation: a crashed node has *forgotten*, not
   merely fallen behind — p. 6, §4.3, layout L346–360 col L.
3. The recovery protocol itself, three steps: `RECOVERY` with a nonce, `RECOVERYRESPONSE`
   from normal-status replicas, f+1 nonce-matched responses including the primary of the
   latest view heard of, then wholesale state replacement from that primary and status back
   to normal — p. 6, §4.3, layout L336–368 col R.
4. Learning an entire log is impractical — p. 7, §5.1, layout L388–391 col R — and the log is
   big because view change and recovery both move it wholesale — p. 5 §4.2 L335–340, p. 7
   §4.3 L371–373.
5. Background persistence of a log prefix, then the better approach of persisting the
   application snapshot, and the checkpoint on disk with a checkpoint number — p. 7, §5.1,
   layout L394–397, L403–410, L417–427 col R. The paper's phrase throughout is "in the
   background" / "background writes are sufficient here too".
6. The view-change log exchange is expensive because the log is big — p. 5 §4.2 L335–340, p. 8
   §5.3 L459–462 — and the fix is to send a suffix of the log in `DOVIEWCHANGE`, small "since
   the most likely case is that the new primary is up to date", with the fallback to asking for
   more or to using application state — p. 8, §5.3, layout L468–478 col R.
7. State transfer, defined by the paper as being for a node that "has gotten behind (but
   hasn't crashed)" — p. 8, §5.2, layout L484–485 — with the backtracking step ("sets its
   op-number to its commit-number and removes all entries after this from its log", layout
   L488–494) and the catch-up procedure (application state to a recent checkpoint, then the
   log forward, then a view-number adjustment, layout L446–452 + L664–672).

### Not in the paper

1. **The word.** "The idempotence of client requests" is not the paper's phrase. The paper's
   single occurrence of any form of "idempot" is layout L416 col R, and it is about the
   *application's* operations being re-executed after recovery, in §5.1 — a durability
   concern, not a client-request concern.
2. **"Asynchronously."** The paper says "in the background" and "background writes are
   sufficient here too" (layout L394–397, L403–410 col R). Its three uses of "asynchronous"
   are all "an asynchronous network".
3. **"In-memory."** Not the paper's word for a replica. The paper says "volatile" and
   "forgotten".
4. **"The new primary is mostly up-to-date" as an assumption.** The paper says it is "the
   most likely case", and designs the fallback (ask for more; fall back to application state)
   into the same paragraph. It is a performance observation, not a correctness premise.
5. **"Easy solution."** The paper says "A reasonable way to get good behavior most of the
   time", and separately notes the trade-off that smaller messages "always" risk more messages.
6. **"At the old primary."** The paper guarantees the prior outcome is in a quorum's
   client-tables (it is written on the primary's path *and* the backups' path, layout
   L199–200 and L221–222 col R). Which replica the client reaches is not fixed.
7. **"Reconnect" and "backtrack" as words.** Neither appears. The behaviours are specified:
   resending to all replicas on no timely response (layout L226–228), re-basing the
   request-number by +2 after a client crash (layout L421–427), and rewinding op-number to
   commit-number and discarding later entries on a slow node hearing of a later view (layout
   L488–494).

### The load-bearing citations, in short

Passage 1: p. 3 layout L141–143 (why), p. 4 layout L189–192 (the table), p. 4 layout L200–207
(the decision), p. 4–5 layout L226–232 (timeout and resend), p. 6 layout L289–292 (the new
primary answers from inherited state).

Passage 2: p. 5 layout L335–340 and p. 7 layout L371–373 (the cost, twice), p. 7 layout
L388–391 (the whole log), p. 7 layout L394–397 and L403–410 (background persistence of log,
then of application state), p. 7 layout L417–427 (checkpoint on disk, checkpoint number), p. 8
layout L459–462 (view-change cost), p. 8 layout L468–478 (log suffix, "most likely case", and
the omitted fallback).
