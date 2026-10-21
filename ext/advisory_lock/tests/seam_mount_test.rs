use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use lunet_advisory_lock::locks::{Response, StateSnapshot};
use lunet_advisory_lock::spi::{
    CommitHook, Disk, DiskDirEntry, DiskFile, FileStateStore, LifecycleStore, StateStore, StdDisk,
};
use lunet_advisory_lock::{Node, OK, OUTPUT_REPLY, OUTPUT_SEND, PRIMARY_TIMEOUT_MS, census_paths};
use uuid::Uuid;
use vrr::ids::Slot;
use vrr::lifecycle::{CopyState, Marker, SuperblockCopies};

// ---------------------------------------------------------------------------
// The out-of-crate implementations
// ---------------------------------------------------------------------------

struct SeamDisk {
    inner: StdDisk,
}

impl SeamDisk {
    fn new() -> Self {
        Self { inner: StdDisk }
    }
}

impl Disk for SeamDisk {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.inner.read(path)
    }

    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        self.inner.read_to_string(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.inner.exists(path)
    }

    fn file_len(&self, path: &Path) -> io::Result<u64> {
        self.inner.file_len(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<DiskDirEntry>> {
        self.inner.read_dir(path)
    }

    fn sync_path(&self, path: &Path) -> io::Result<()> {
        self.inner.sync_path(path)
    }

    fn open_read(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.open_read(path)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.open_read_write(path)
    }

    fn open_read_append(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.open_read_append(path)
    }

    fn open_or_create_write(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.open_or_create_write(path)
    }

    fn open_or_create_append(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.open_or_create_append(path)
    }

    fn create_new_write(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.create_new_write(path)
    }

    fn create_new_append(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.create_new_append(path)
    }

    fn create_new_read_write(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.create_new_read_write(path)
    }
}

struct SeamStore {
    inner: FileStateStore,
    disk: Arc<dyn Disk>,
    state_path: PathBuf,
    tally: Arc<Mutex<Vec<&'static str>>>,
}
impl SeamStore {
    fn new(disk: Arc<dyn Disk>, state: &Path) -> Self {
        Self {
            inner: FileStateStore::new_on(Arc::clone(&disk), state),
            disk,
            state_path: state.to_path_buf(),
            tally: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn count(&self, what: &str) -> usize {
        self.tally
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|line| **line == what)
            .count()
    }
}

impl Clone for SeamStore {
    fn clone(&self) -> Self {
        Self {
            inner: FileStateStore::new_on(Arc::clone(&self.disk), &self.state_path),
            disk: Arc::clone(&self.disk),
            state_path: self.state_path.clone(),
            tally: Arc::clone(&self.tally),
        }
    }
}

impl StateStore for SeamStore {
    fn flush(&mut self, snap: &StateSnapshot) -> io::Result<()> {
        self.tally
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("flush");
        self.inner.flush(snap)
    }

    fn load(&self) -> io::Result<Option<StateSnapshot>> {
        self.tally
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("load");
        self.inner.load()
    }
}

#[derive(Clone, Default)]
struct SeamHook {
    slots: Arc<Mutex<Vec<u64>>>,
}

impl SeamHook {
    fn commits(&self) -> usize {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

impl CommitHook for SeamHook {
    fn on_commit(&mut self, slot: Slot) {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(slot.0);
    }
}

#[derive(Clone)]
struct SeamFence {
    disk: Arc<dyn Disk>,
    path: PathBuf,
    tape: Arc<Mutex<Vec<String>>>,
}

fn marker_word(marker: Marker) -> &'static str {
    match marker {
        Marker::Stopping => "stopping",
        Marker::Stopped => "stopped",
        Marker::Restarting => "restarting",
        Marker::Joining => "joining",
    }
}

fn marker_from_word(word: &str) -> Option<Marker> {
    match word {
        "stopping" => Some(Marker::Stopping),
        "stopped" => Some(Marker::Stopped),
        "restarting" => Some(Marker::Restarting),
        "joining" => Some(Marker::Joining),
        _ => None,
    }
}

impl SeamFence {
    fn new(disk: Arc<dyn Disk>, path: PathBuf) -> Self {
        Self {
            disk,
            path,
            tape: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn tape(&self) -> Vec<String> {
        self.tape
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl LifecycleStore for SeamFence {
    type Error = io::Error;

    fn read_copies(&mut self) -> Result<Option<SuperblockCopies>, Self::Error> {
        if !self.disk.exists(&self.path) {
            self.tape
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("read none".to_string());
            return Ok(None);
        }
        let text = self.disk.read_to_string(&self.path)?;
        let lines: Vec<&str> = text.lines().collect();
        if lines.len() != 4 {
            return Err(io::Error::other(format!(
                "the fence's marker file holds {} copies, not four",
                lines.len()
            )));
        }
        let mut copies = [CopyState {
            identity: vrr::ids::NodeId(0),
            marker: Marker::Joining,
        }; 4];
        for (slot, line) in lines.iter().enumerate() {
            let (identity, word) = line
                .split_once(' ')
                .ok_or_else(|| io::Error::other("the fence's marker line has no marker"))?;
            let identity = identity.parse::<u32>().map_err(|error| {
                io::Error::other(format!("the fence's identity refused: {error}"))
            })?;
            let marker = marker_from_word(word)
                .ok_or_else(|| io::Error::other(format!("the fence's marker word '{word}'")))?;
            copies[slot] = CopyState {
                identity: vrr::ids::NodeId(identity),
                marker,
            };
        }
        self.tape
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(format!("read {}", marker_word(copies[0].marker)));
        Ok(Some(SuperblockCopies { copies }))
    }

    fn commit(&mut self, copies: &SuperblockCopies) -> Result<(), Self::Error> {
        let copy = copies.copies[0];
        assert!(
            copies.copies.iter().all(|one| *one == copy),
            "the boot gate's rewrites are uniform 4x"
        );
        let mut text = String::new();
        for one in &copies.copies {
            text.push_str(&format!("{} {}\n", one.identity.0, marker_word(one.marker)));
        }
        let mut staging = self.path.clone();
        staging.as_mut_os_string().push(".staging");
        let mut file = self.disk.create_new_write(&staging)?;
        file.write_all(text.as_bytes())?;
        file.flush()?;
        file.sync_all()?;
        self.disk.rename(&staging, &self.path)?;
        self.disk.sync_path(
            self.path
                .parent()
                .ok_or_else(|| io::Error::other("the fence's marker file has no directory"))?,
        )?;
        self.tape
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(format!(
                "commit {}@{}",
                marker_word(copy.marker),
                copy.identity.0
            ));
        Ok(())
    }

    fn drain(&mut self) -> Result<(), Self::Error> {
        self.disk.sync_path(
            self.path
                .parent()
                .ok_or_else(|| io::Error::other("the fence's marker file has no directory"))?,
        )?;
        self.tape
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("drain".to_string());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The switchboard: three real nodes on one thread
// ---------------------------------------------------------------------------

struct Cluster {
    nodes: Vec<Node>,
    ids: [u32; 3],
    replies: Vec<(u32, Vec<u8>)>,
}

impl Cluster {
    fn pump(&mut self) {
        let mut inboxes: [Vec<(u32, Vec<u8>)>; 3] = [Vec::new(), Vec::new(), Vec::new()];
        for (index, node) in self.nodes.iter_mut().enumerate() {
            while let Some(out) = node.next_output() {
                if out.kind == OUTPUT_SEND {
                    if let Some(target) = self.ids.iter().position(|id| *id == out.to) {
                        inboxes[target].push((self.ids[index], out.bytes));
                    }
                } else if out.kind == OUTPUT_REPLY {
                    self.replies.push((self.ids[index], out.bytes));
                }
            }
        }
        for (index, node) in self.nodes.iter_mut().enumerate() {
            for (from, bytes) in inboxes[index].drain(..) {
                node.receive(from, &bytes);
            }
            let _ = node.idle();
        }
    }

    fn drive_until(&mut self, until: impl Fn(&Self) -> bool, rounds: usize) {
        for _ in 0..rounds {
            self.pump();
            if until(self) {
                return;
            }
        }
        panic!("the cluster never reached the driven shape");
    }

    fn reply(&self, message_id: &Uuid) -> Vec<u8> {
        self.replies
            .iter()
            .find(|(_, bytes)| {
                serde_json::from_slice::<Response>(bytes)
                    .is_ok_and(|reply| reply_message_id(&reply) == *message_id)
            })
            .map(|(_, bytes)| bytes.clone())
            .expect("the reply for the proposed message id arrived")
    }
}

fn reply_message_id(reply: &Response) -> Uuid {
    match reply {
        Response::Get { message_id, .. }
        | Response::Set { message_id, .. }
        | Response::Release { message_id, .. }
        | Response::Break { message_id, .. } => *message_id,
    }
}

fn members() -> String {
    [1u32, 2, 3]
        .iter()
        .map(|system| {
            format!(
                "{}:{}\0",
                (*system << 16) | 1,
                (b'a' + (*system - 1) as u8) as char
            )
        })
        .collect::<String>()
        .trim_end_matches('\0')
        .to_string()
}

fn member_id(system: u32) -> u32 {
    (system << 16) | 1
}

fn set_request(message_id: Uuid, client_id: u64, request_num: u64, lock_id: u64) -> Vec<u8> {
    serde_json::json!({
        "op": "set",
        "message_id": message_id.to_string(),
        "client_id": client_id,
        "request_num": request_num,
        "lock_id": lock_id,
        "lease": {
            "lease_id": 900,
            "holder": holder().to_string(),
            "lease_ms": 30_000,
        },
        "name": format!("/seam-{lock_id}"),
        "labels": serde_json::Value::Null,
    })
    .to_string()
    .into_bytes()
}

fn get_request(message_id: Uuid, client_id: u64, request_num: u64, lock_id: u64) -> Vec<u8> {
    serde_json::json!({
        "op": "get",
        "message_id": message_id.to_string(),
        "client_id": client_id,
        "request_num": request_num,
        "lock_id": lock_id,
    })
    .to_string()
    .into_bytes()
}

fn holder() -> Uuid {
    Uuid::from_u128(0x0f1e_2d3c_0000_0000_0000_0000_0000_0001u128)
}

fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/seam-mount");
    fs::create_dir_all(&root).expect("the scratch root creates");
    let dir = root.join(format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    fs::create_dir_all(&dir).expect("the case directory creates");
    dir
}

struct Seams {
    disk: Arc<dyn Disk>,
    store: SeamStore,
    hook: SeamHook,
    fence: SeamFence,
    state_path: PathBuf,
}

fn seams(dir: &Path) -> Seams {
    let disk: Arc<dyn Disk> = Arc::new(SeamDisk::new());
    let state_path = dir.join("node-a.marker");
    let store = SeamStore::new(Arc::clone(&disk), &state_path);
    let fence = SeamFence::new(Arc::clone(&disk), dir.join("fence-a.lines"));
    Seams {
        disk,
        store,
        hook: SeamHook::default(),
        fence,
        state_path,
    }
}

fn first_life(dir: &Path, seams: &Seams) -> Cluster {
    let _ = census_paths();
    let a = Node::open_with_seams(
        &members(),
        "a",
        seams.state_path.to_str().expect("the path is UTF-8"),
        None,
        0,
        PRIMARY_TIMEOUT_MS,
        Arc::clone(&seams.disk),
        Box::new(seams.store.clone()),
        Arc::new(Mutex::new(seams.hook.clone())),
        Some(Box::new(seams.fence.clone())),
    )
    .expect("the seam-mounted node boots on a fresh marker");
    let b = Node::open(
        &members(),
        "b",
        dir.join("node-b.marker")
            .to_str()
            .expect("the path is UTF-8"),
        None,
        0,
        PRIMARY_TIMEOUT_MS,
    )
    .expect("the first peer boots");
    let c = Node::open(
        &members(),
        "c",
        dir.join("node-c.marker")
            .to_str()
            .expect("the path is UTF-8"),
        None,
        0,
        PRIMARY_TIMEOUT_MS,
    )
    .expect("the second peer boots");
    let tape = census_paths();
    assert_eq!(
        (
            tape.contains(&"boot.first"),
            tape.contains(&"boot.first-latch"),
            tape.contains(&"boot.provision"),
        ),
        (true, true, true),
        "a fresh marker is the first life: {tape:?}"
    );
    Cluster {
        nodes: vec![a, b, c],
        ids: [member_id(1), member_id(2), member_id(3)],
        replies: Vec::new(),
    }
}

fn drive_request(cluster: &mut Cluster, json: &[u8], message_id: &Uuid) -> Vec<u8> {
    cluster.drive_until(
        |cluster| {
            let status = cluster.nodes[0].status();
            status.state == 0 && status.leader == member_id(1)
        },
        100,
    );
    assert_eq!(
        cluster.nodes[0].request(json),
        OK,
        "the propose is admitted"
    );
    cluster.drive_until(
        |cluster| {
            cluster.replies.iter().any(|(_, bytes)| {
                serde_json::from_slice::<Response>(bytes)
                    .is_ok_and(|reply| reply_message_id(&reply) == *message_id)
            })
        },
        400,
    );
    cluster.reply(message_id)
}

fn assert_stop_schedule(tape: &[&'static str]) {
    let at = |name: &str| {
        tape.iter()
            .position(|line| *line == name)
            .unwrap_or_else(|| panic!("the tape carries {name}: {tape:?}"))
    };
    assert_eq!(
        (
            at("stop.drain-window.open") < at("state.flush"),
            at("state.flush") < at("stop.round.finish"),
            at("state.flush") < at("stop.complete"),
        ),
        (true, true, true),
        "the flush sits in the drain window: after it opens, before the \
         drain-proven Stopped round and before the stop completes — {tape:?}"
    );
}

#[test]
fn a_node_mounts_all_four_seams_through_the_public_door() {
    let dir = scratch("full-mount");
    let seams = seams(&dir);
    let mut cluster = first_life(&dir, &seams);

    // The boot went through the mounted fence: the first read found no
    // markers, and the first-life latch wrote the joining round through
    // the trait's commit — before the test drove anything.
    assert_eq!(
        seams.fence.tape(),
        vec!["read none".to_string(), "commit joining@65537".to_string(),],
        "the first life's boot read and latched through the mounted fence"
    );

    // A client commit: the mounted hook observes exactly the applied
    // commits — one request, one commit — and the first life's verdict
    // refused the lazy load before the mounted store was ever touched.
    let message_id = Uuid::from_u128(0x0f1e_2d3c_0000_0000_0000_0000_0000_0042u128);
    let reply = drive_request(&mut cluster, &set_request(message_id, 7, 1, 1), &message_id);
    let Response::Set { granted, .. } = serde_json::from_slice(&reply).expect("the reply decodes")
    else {
        panic!("the reply is the set's: {reply:?}");
    };
    assert!(granted, "the lock is taken: {reply:?}");
    assert_eq!(
        seams.hook.commits(),
        1,
        "the mounted hook observed exactly the applied commit"
    );
    assert_eq!(
        seams.store.count("load"),
        0,
        "the first life's verdict distrusts the state file: the mounted \
         store is never asked to load"
    );
    assert_eq!(
        seams.store.count("flush"),
        0,
        "the regular path never flushes: load is lazy, flush is the \
         shutdown path's"
    );

    // The stop: the census schedule is the one the law fixed, and the
    // marker rounds go through the mounted fence — first round, the
    // drain between the rounds, then the drain-proven round.
    let _ = census_paths();
    assert_eq!(cluster.nodes[0].stop(), OK, "a seated node stops clean");
    assert_stop_schedule(&census_paths());

    // The eager flush went through the mounted store — the one flush of
    // the life — and the file it left is loadable, the taken lock in it.
    assert_eq!(
        seams.store.count("flush"),
        1,
        "the stop's flush rode the mounted store"
    );
    let reopened = FileStateStore::new_on(seams.disk.clone(), &seams.state_path);
    let loaded = reopened
        .load()
        .expect("the load reads")
        .expect("a clean stop's flush is loadable");
    assert_eq!(loaded.locks.len(), 1, "the taken lock is in the table");

    // The fence's rounds, in write order: the boot's latch, the halt's
    // first round, the drain between the rounds, the drain-proven round.
    assert_eq!(
        seams.fence.tape(),
        vec![
            "read none".to_string(),
            "commit joining@65537".to_string(),
            "commit stopping@65537".to_string(),
            "drain".to_string(),
            "commit stopped@65537".to_string(),
        ],
        "the marker machine's whole schedule rode the mounted fence"
    );
}

#[test]
fn the_fence_door_carries_the_boot_classification_and_the_law_rides_it() {
    let dir = scratch("fence-classification");
    let seams = seams(&dir);
    let mut cluster = first_life(&dir, &seams);

    // Life one: a lock taken, a clean stop, the stopped quorum in the
    // mounted fence's file.
    let first = Uuid::from_u128(0x0f1e_2d3c_0000_0000_0000_0000_0000_0043u128);
    drive_request(&mut cluster, &set_request(first, 7, 1, 1), &first);
    assert_eq!(cluster.nodes[0].stop(), OK, "life one stops clean");
    let life_one_tape = seams.fence.tape();
    assert!(
        life_one_tape.contains(&"commit stopped@65537".to_string()),
        "life one's halt wrote its drain-proven round through the mounted \
         fence: {life_one_tape:?}"
    );
    let hook_before = seams.hook.commits();

    // Life two: fresh peers (their own fresh state paths), the same seam
    // paths — the mounted fence reads the stopped quorum life one left.
    let _ = census_paths();
    let store = SeamStore::new(Arc::clone(&seams.disk), &seams.state_path);
    let fence = SeamFence::new(Arc::clone(&seams.disk), dir.join("fence-a.lines"));
    let a = Node::open_with_seams(
        &members(),
        "a",
        seams.state_path.to_str().expect("the path is UTF-8"),
        None,
        0,
        PRIMARY_TIMEOUT_MS,
        Arc::clone(&seams.disk),
        Box::new(store.clone()),
        Arc::new(Mutex::new(seams.hook.clone())),
        Some(Box::new(fence.clone())),
    )
    .expect("life two boots through the mounted fence");
    let b = Node::open(
        &members(),
        "b",
        dir.join("node-b2.marker")
            .to_str()
            .expect("the path is UTF-8"),
        None,
        0,
        PRIMARY_TIMEOUT_MS,
    )
    .expect("life two's first peer boots");
    let c = Node::open(
        &members(),
        "c",
        dir.join("node-c2.marker")
            .to_str()
            .expect("the path is UTF-8"),
        None,
        0,
        PRIMARY_TIMEOUT_MS,
    )
    .expect("life two's second peer boots");
    let mut life_two = Cluster {
        nodes: vec![a, b, c],
        ids: [member_id(1), member_id(2), member_id(3)],
        replies: Vec::new(),
    };

    // The classification went through the mounted fence, and its verdict
    // is the CLEAN start: the tape's first boot line is the stopped
    // quorum's verdict, the latch rode the fence, and nothing here is a
    // crash or a first life for the seam node.
    let tape = census_paths();
    assert_eq!(
        tape.iter().find(|line| line.starts_with("boot.")).copied(),
        Some("boot.clean"),
        "life two classifies CLEAN off the mounted fence's stopped quorum: \
         {tape:?}"
    );
    assert!(
        tape.contains(&"boot.clean-latch"),
        "the clean start latched through the mounted fence: {tape:?}"
    );
    assert!(
        !tape.contains(&"boot.crashed"),
        "a clean stop is never a crash: {tape:?}"
    );
    assert_eq!(
        fence.tape()[..2],
        [
            "read stopped".to_string(),
            "commit restarting@65537".to_string()
        ],
        "the classification's read and the latch went through the mounted \
         fence: {:?}",
        fence.tape()
    );

    // The law rides the mounted store: life two's first demand — the
    // first committed entry's apply — loads the flushed table, once. The
    // lock life one took is in the restored table, and the node serves
    // it, holder and all.
    let second = Uuid::from_u128(0x0f1e_2d3c_0000_0000_0000_0000_0000_0044u128);
    drive_request(&mut life_two, &set_request(second, 7, 2, 2), &second);
    assert_eq!(
        store.count("load"),
        1,
        "the lazy load fired exactly once, at the first demand"
    );
    assert_eq!(
        seams.hook.commits(),
        hook_before + 1,
        "the mounted hook observed life two's applied commit"
    );
    let probe = Uuid::from_u128(0x0f1e_2d3c_0000_0000_0000_0000_0000_0045u128);
    let get_reply = drive_request(&mut life_two, &get_request(probe, 7, 3, 1), &probe);
    let Response::Get { lease, .. } =
        serde_json::from_slice(&get_reply).expect("the reply decodes")
    else {
        panic!("the reply is the get's: {get_reply:?}");
    };
    assert_eq!(
        lease.map(|lease| lease.holder),
        Some(holder()),
        "the restored table serves life one's lock, holder and all"
    );
}
