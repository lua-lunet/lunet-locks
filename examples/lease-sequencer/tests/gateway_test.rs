//! The RFC's obligations, as tests (`docs/src/rfc-application-protocol.md`):
//! the session loop driven end to end against a real committing core
//! in-process — three adapter `Node`s over scratch marker stores, the
//! wire in memory, the logical clock. Every test states one obligation
//! and proves it on the application-visible surface: the bytes the
//! nexus wrote to the opaque handles, and the handles' own release.

use lease_sequencer::gateway::{
    AcceptOutcome, AppSink, Gateway, GatewayConfig, OUTCOME_UNKNOWN, REFUSAL_BEARER_UNKNOWN,
    REFUSAL_OUTSTANDING, pack_128, unpack_128,
};
use lease_sequencer::gateway_harness::{Cluster, Committed, MemSink};
use serde_json::{Value, json};

/// The gateway key every test plays by.
fn key() -> [u8; 32] {
    [0xA5; 32]
}

/// A test's gateway knobs: the in-flight command deadline and the idle
/// session lifetime, in the logical clock's ticks.
fn config(command_deadline: u64, session_ttl: u64) -> GatewayConfig {
    GatewayConfig {
        key: key(),
        command_deadline,
        session_ttl,
    }
}

/// The audit metadata an application supplies with its join.
fn audit() -> Value {
    json!({"app": "console", "user": "u-1"})
}

/// A plain command body.
fn body() -> Value {
    json!({"op": "put", "key": "k"})
}

/// The reply bytes a committed command carries.
fn committed_reply(result: &[u8]) -> Value {
    json!({"committed": serde_json::from_slice::<Value>(result).expect("the committed payload is JSON")})
}

/// One socket's received replies, parsed.
fn replies(sink: &MemSink) -> Vec<Value> {
    sink.bytes()
        .iter()
        .map(|bytes| serde_json::from_slice(bytes).expect("a reply is JSON"))
        .collect()
}

/// The bearer a join reply carries.
fn bearer_of(sink: &MemSink) -> String {
    replies(sink)[0]["bearer"]
        .as_str()
        .expect("the join reply carries the bearer")
        .to_string()
}

/// Every number a JSON value carries, recursively — the scan the raw
/// session_id absence is proved over.
fn walk_numbers(value: &Value, out: &mut Vec<u64>) {
    match value {
        Value::Number(number) => {
            if let Some(n) = number.as_u64() {
                out.push(n);
            }
        }
        Value::Array(items) => items.iter().for_each(|item| walk_numbers(item, out)),
        Value::Object(map) => map.values().for_each(|item| walk_numbers(item, out)),
        _ => {}
    }
}

/// The gateway loop over one cluster: the reads from the abstract socket
/// (the application's calls), and the committed half — the cluster
/// pumped, every new committed entry upcalled by uuid with its result
/// as committed at the node.
struct Loop {
    cluster: Cluster,
    gateway: Gateway,
}

impl Loop {
    fn boot(name: &str, config: GatewayConfig) -> Loop {
        let cluster = Cluster::boot(name);
        Loop {
            cluster,
            gateway: Gateway::new(config),
        }
    }

    fn join(&mut self, audit: &Value, sink: &MemSink) -> AcceptOutcome {
        self.gateway.join(
            audit,
            Box::new(sink.handle()),
            self.cluster.now(),
            &mut self.cluster,
        )
    }

    fn command(
        &mut self,
        bearer: &str,
        request_num: u64,
        body: &Value,
        sink: &MemSink,
    ) -> AcceptOutcome {
        self.gateway.command(
            bearer,
            request_num,
            body,
            Box::new(sink.handle()),
            self.cluster.now(),
            &mut self.cluster,
        )
    }

    fn command_handle(
        &mut self,
        bearer: &str,
        request_num: u64,
        body: &Value,
        handle: Box<dyn AppSink>,
    ) -> AcceptOutcome {
        self.gateway.command(
            bearer,
            request_num,
            body,
            handle,
            self.cluster.now(),
            &mut self.cluster,
        )
    }

    /// The committed half of the loop: pump the cluster until the
    /// proposal's commit has crossed every seat, then upcall each new
    /// committed entry — the uuid of the command and the command result
    /// as committed at the node.
    fn run(&mut self) -> Vec<Committed> {
        self.cluster.advance(1);
        let committed = self.cluster.committed_since();
        for record in &committed {
            self.gateway.committed(
                self.cluster.now(),
                record.slot,
                record.uuid,
                &record.payload,
            );
        }
        committed
    }
}

/// Obligation 1: at most one outstanding command per session. A second,
/// different command is refused by name; a re-send of the same command
/// attaches to the one already in flight.
#[test]
fn obligation_1_one_outstanding_refused_by_name_resend_attaches() {
    let mut l = Loop::boot("obligation-1", config(30, 1_000));

    let join_sink = MemSink::new();
    assert_eq!(l.join(&audit(), &join_sink), AcceptOutcome::Accepted);
    assert_eq!(l.run().len(), 1);
    let bearer = bearer_of(&join_sink);

    let first = MemSink::new();
    assert_eq!(
        l.command(&bearer, 1, &body(), &first),
        AcceptOutcome::Accepted
    );

    // A second, different command while one holds the session: refused
    // by name, on its own socket, never queued behind it.
    let second = MemSink::new();
    assert_eq!(
        l.command(&bearer, 2, &body(), &second),
        AcceptOutcome::Refused(REFUSAL_OUTSTANDING)
    );
    assert_eq!(
        replies(&second),
        vec![json!({"refused": REFUSAL_OUTSTANDING})]
    );

    // The re-send of the command that IS outstanding — the same
    // application request number — attaches, and answers on the handle
    // it arrived on.
    let resend = MemSink::new();
    assert_eq!(
        l.command(&bearer, 1, &body(), &resend),
        AcceptOutcome::Attached
    );
    assert!(first.dropped());

    // The commit answers the attached handle, exactly once, and nothing
    // new was proposed for the re-send.
    let committed = l.run();
    assert_eq!(committed.len(), 1);
    assert_eq!(
        replies(&resend),
        vec![committed_reply(&committed[0].payload)]
    );
    assert_eq!(
        replies(&second),
        vec![json!({"refused": REFUSAL_OUTSTANDING})]
    );
}

/// Obligation 2: the uuid is `pack_128(session_id, counter)` — high 64
/// bits the session_id, low 64 the counter — and the counter increments
/// exactly once per accepted command, so two sessions' interleaved
/// counters never collide.
#[test]
fn obligation_2_uuid_halves_and_counter_exactly_once() {
    let mut l = Loop::boot("obligation-2", config(30, 1_000));

    let join_a = MemSink::new();
    assert_eq!(l.join(&audit(), &join_a), AcceptOutcome::Accepted);
    let slot_a = l.run()[0].slot;
    let join_b = MemSink::new();
    assert_eq!(l.join(&audit(), &join_b), AcceptOutcome::Accepted);
    let slot_b = l.run()[0].slot;
    assert_ne!(slot_a, slot_b);
    let bearer_a = bearer_of(&join_a);
    let bearer_b = bearer_of(&join_b);

    // Two sessions, one command each, both in flight at once.
    let first_a = MemSink::new();
    assert_eq!(
        l.command(&bearer_a, 1, &body(), &first_a),
        AcceptOutcome::Accepted
    );
    let first_b = MemSink::new();
    assert_eq!(
        l.command(&bearer_b, 1, &body(), &first_b),
        AcceptOutcome::Accepted
    );

    // The identity's halves, as the bytes cross the core: high 64 the
    // session_id, low 64 the counter.
    let uuid_a = l.gateway.inspect(&bearer_a).unwrap().outstanding.unwrap().0;
    let uuid_b = l.gateway.inspect(&bearer_b).unwrap().outstanding.unwrap().0;
    assert_eq!(&uuid_a[..8], &slot_a.to_be_bytes());
    assert_eq!(&uuid_a[8..], &1u64.to_be_bytes());
    assert_eq!(&uuid_b[..8], &slot_b.to_be_bytes());
    assert_eq!(&uuid_b[8..], &1u64.to_be_bytes());
    assert_eq!(unpack_128(uuid_a), (slot_a, 1));
    assert_eq!(pack_128(slot_b, 7)[..8], slot_b.to_be_bytes());
    assert_eq!(pack_128(slot_b, 7)[8..], 7u64.to_be_bytes());

    // Interleave: the second command of each session, after the first
    // commits. Four identities, no collision.
    l.run();
    let second_a = MemSink::new();
    assert_eq!(
        l.command(&bearer_a, 2, &body(), &second_a),
        AcceptOutcome::Accepted
    );
    let second_b = MemSink::new();
    assert_eq!(
        l.command(&bearer_b, 2, &body(), &second_b),
        AcceptOutcome::Accepted
    );
    let uuid_a2 = l.gateway.inspect(&bearer_a).unwrap().outstanding.unwrap().0;
    let uuid_b2 = l.gateway.inspect(&bearer_b).unwrap().outstanding.unwrap().0;
    let uuids = [uuid_a, uuid_b, uuid_a2, uuid_b2];
    for (index, uuid) in uuids.iter().enumerate() {
        for other in &uuids[index + 1..] {
            assert_ne!(uuid, other);
        }
    }
    assert_eq!(unpack_128(uuid_a2), (slot_a, 2));
    assert_eq!(unpack_128(uuid_b2), (slot_b, 2));
    assert_eq!(l.gateway.inspect(&bearer_a).unwrap().counter, 2);
    assert_eq!(l.gateway.inspect(&bearer_b).unwrap().counter, 2);

    // A re-send is not a new accepted command: while A's second command
    // is in flight, its re-send attaches and the counter does not move
    // for it.
    let again = MemSink::new();
    assert_eq!(
        l.command(&bearer_a, 2, &body(), &again),
        AcceptOutcome::Attached
    );
    assert_eq!(l.gateway.inspect(&bearer_a).unwrap().counter, 2);
    l.run();
}

/// Obligation 3: a session_id never reaches the application in the
/// clear. The join reply carries the bearer and only the bearer; no
/// written response names or carries the raw slot.
#[test]
fn obligation_3_session_id_never_reaches_the_application() {
    let mut l = Loop::boot("obligation-3", config(30, 1_000));

    let join_sink = MemSink::new();
    assert_eq!(l.join(&audit(), &join_sink), AcceptOutcome::Accepted);
    let slot = l.run()[0].slot;
    let bearer = bearer_of(&join_sink);

    let command_sink = MemSink::new();
    assert_eq!(
        l.command(&bearer, 1, &body(), &command_sink),
        AcceptOutcome::Accepted
    );
    l.run();

    // The join reply is the bearer, only the bearer.
    let join_reply = replies(&join_sink)[0].clone();
    assert_eq!(join_reply.as_object().unwrap().len(), 1);
    assert!(join_reply.get("bearer").and_then(Value::as_str).is_some());
    assert_eq!(join_reply["bearer"], Value::String(l.gateway.bearer(slot)));

    // The whole application-visible surface: no response names the
    // session_id, and no response carries the slot as a number.
    let sockets = [join_sink.bytes(), command_sink.bytes()];
    for bytes in sockets.into_iter().flatten() {
        let text = std::str::from_utf8(&bytes).expect("every reply is UTF-8 JSON");
        assert!(!text.contains("session_id"));
        let value: Value = serde_json::from_slice(&bytes).expect("a reply is JSON");
        let mut numbers = Vec::new();
        walk_numbers(&value, &mut numbers);
        assert!(!numbers.contains(&slot));
    }

    // The bearer is a keyed hash of the session_id, not the slot in
    // any plain encoding, and adjacent sessions do not enumerate from
    // one another's bearers.
    assert!(!bearer.contains(&format!("{slot:016x}")));
    assert_ne!(bearer, l.gateway.bearer(slot + 1));
}

/// Obligation 4: at most once. The nexus entry is taken before the
/// response is written, so a second upcall for the same uuid writes
/// nothing; a crash between the take and the write leaves nothing to
/// write the result from.
#[test]
fn obligation_4_take_then_write_at_most_once() {
    let mut l = Loop::boot("obligation-4", config(30, 1_000));

    let join_sink = MemSink::new();
    assert_eq!(l.join(&audit(), &join_sink), AcceptOutcome::Accepted);
    l.run();
    let bearer = bearer_of(&join_sink);

    let first = MemSink::new();
    assert_eq!(
        l.command(&bearer, 1, &body(), &first),
        AcceptOutcome::Accepted
    );
    let uuid = l.gateway.inspect(&bearer).unwrap().outstanding.unwrap().0;
    let record = l.run().remove(0);
    assert_eq!(record.uuid, uuid);

    // The take: the entry is gone from the nexus.
    assert!(!l.gateway.nexus_holds(record.uuid));
    // Exactly one write; a second upcall for the same uuid is a no-op.
    assert_eq!(replies(&first), vec![committed_reply(&record.payload)]);
    l.gateway
        .committed(l.cluster.now(), record.slot, record.uuid, &record.payload);
    assert_eq!(first.bytes().len(), 1);

    // The crash-between-take-and-write shape: the upcall takes the
    // entry, the write dies with the process. The nexus holds the
    // crashing handle; the committed record is upcalled by the test
    // itself.
    let crash_sink = MemSink::new();
    assert_eq!(
        l.command_handle(&bearer, 2, &body(), Box::new(crash_sink.crash_handle())),
        AcceptOutcome::Accepted
    );
    let record = {
        l.cluster.advance(1);
        let mut records = l.cluster.committed_since();
        assert_eq!(records.len(), 1);
        records.remove(0)
    };
    let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        l.gateway
            .committed(l.cluster.now(), record.slot, record.uuid, &record.payload);
    }));
    assert!(crashed.is_err());
    // Nothing reached the application, the entry is gone, and nothing
    // can be written from it any more: the re-upcall is a no-op.
    assert!(crash_sink.bytes().is_empty());
    assert!(!l.gateway.nexus_holds(record.uuid));
    let recovered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        l.gateway
            .committed(l.cluster.now(), record.slot, record.uuid, &record.payload);
    }));
    assert!(recovered.is_ok());
    assert!(crash_sink.bytes().is_empty());
}

/// Obligation 5: a timeout is surfaced as UNKNOWN — never as "did not
/// happen". The command that committed late answers nothing the second
/// time: the unknown was the reply, and the re-drive is a new command.
#[test]
fn obligation_5_timeout_is_unknown_not_did_not_happen() {
    let mut l = Loop::boot("obligation-5", config(5, 1_000));

    let join_sink = MemSink::new();
    assert_eq!(l.join(&audit(), &join_sink), AcceptOutcome::Accepted);
    l.run();
    let bearer = bearer_of(&join_sink);

    let command_sink = MemSink::new();
    assert_eq!(
        l.command(&bearer, 1, &body(), &command_sink),
        AcceptOutcome::Accepted
    );

    // The wire is held and the clock runs on: the outcome is not
    // learned by the deadline. The gateway surfaces UNKNOWN, by name,
    // and frees the session's slot.
    l.cluster.elapse(6);
    l.gateway.pump(l.cluster.now());
    assert_eq!(
        replies(&command_sink),
        vec![json!({"outcome": OUTCOME_UNKNOWN})]
    );
    assert!(l.gateway.inspect(&bearer).unwrap().outstanding.is_none());

    // The command did commit, late. Its upcall is a no-op: the reply
    // was already given, and a re-drive is a new command, never a
    // resurrection.
    l.cluster.advance(1);
    let committed = l.cluster.committed_since();
    assert_eq!(committed.len(), 1);
    l.gateway.committed(
        l.cluster.now(),
        committed[0].slot,
        committed[0].uuid,
        &committed[0].payload,
    );
    assert_eq!(command_sink.bytes().len(), 1);
}

/// Obligation 6: the sweeper drops an inactive session — releasing the
/// nexus entry it still holds and closing the handle — and shutdown
/// releases every nexus entry and closes every handle.
#[test]
fn obligation_6_sweeper_and_shutdown_release_every_entry_and_handle() {
    let mut l = Loop::boot("obligation-6", config(30, 10));

    let join_sink = MemSink::new();
    assert_eq!(l.join(&audit(), &join_sink), AcceptOutcome::Accepted);
    l.run();
    let bearer = bearer_of(&join_sink);

    // One command in flight, its handle held by the nexus.
    let command_sink = MemSink::new();
    assert_eq!(
        l.command(&bearer, 1, &body(), &command_sink),
        AcceptOutcome::Accepted
    );

    // The session goes idle past its lifetime while the command is
    // still in flight. The sweeper drops the session and releases the
    // entry with it: the handle closes, nothing is written.
    l.cluster.elapse(11);
    l.gateway.pump(l.cluster.now());
    assert!(l.gateway.inspect(&bearer).is_none());
    assert!(command_sink.dropped());
    assert!(command_sink.bytes().is_empty());

    // The bearer resolves no more: a fresh command is refused by name.
    let late = MemSink::new();
    assert_eq!(
        l.command(&bearer, 2, &body(), &late),
        AcceptOutcome::Refused(REFUSAL_BEARER_UNKNOWN)
    );
    assert_eq!(
        replies(&late),
        vec![json!({"refused": REFUSAL_BEARER_UNKNOWN})]
    );

    // Shutdown: a live session with a command in flight; every nexus
    // entry released, every handle closed, every session gone.
    let second_join = MemSink::new();
    assert_eq!(l.join(&audit(), &second_join), AcceptOutcome::Accepted);
    l.run();
    let bearer2 = bearer_of(&second_join);
    let flight = MemSink::new();
    assert_eq!(
        l.command(&bearer2, 1, &body(), &flight),
        AcceptOutcome::Accepted
    );
    let uuid = l.gateway.inspect(&bearer2).unwrap().outstanding.unwrap().0;
    l.gateway.shutdown();
    assert!(flight.dropped());
    assert!(!l.gateway.nexus_holds(uuid));
    assert!(l.gateway.inspect(&bearer2).is_none());
}

/// Obligation 7: the join's audit metadata is committed under the same
/// session_id — read back from the committed entry — and the session it
/// establishes is the one the bearer names.
#[test]
fn obligation_7_join_audit_committed_under_the_session_id() {
    let mut l = Loop::boot("obligation-7", config(30, 1_000));

    let join_sink = MemSink::new();
    assert_eq!(l.join(&audit(), &join_sink), AcceptOutcome::Accepted);
    let committed = l.run();
    assert_eq!(committed.len(), 1);
    let slot = committed[0].slot;

    // The committed entry at the session_id's slot carries the audit
    // the application supplied.
    let payload: Value =
        serde_json::from_slice(&committed[0].payload).expect("the committed join is JSON");
    assert_eq!(payload["op"], "join");
    assert_eq!(payload["audit"], audit());

    // The session that slot established is the one the bearer names:
    // the reply's bearer is the bearer of the slot that holds the
    // audit, and a command accepted under it rides that session_id.
    let bearer = bearer_of(&join_sink);
    assert_eq!(bearer, l.gateway.bearer(slot));
    let command_sink = MemSink::new();
    assert_eq!(
        l.command(&bearer, 1, &body(), &command_sink),
        AcceptOutcome::Accepted
    );
    let uuid = l.gateway.inspect(&bearer).unwrap().outstanding.unwrap().0;
    assert_eq!(&uuid[..8], &slot.to_be_bytes());
}
