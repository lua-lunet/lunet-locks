use crate::locks::Service;
use std::collections::VecDeque;
use std::ffi::{OsString, c_void};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::{SystemTime, UNIX_EPOCH};
use vrr::vrr::{Body, Header, Input, LogEntry, MAX_DATAGRAM, Message, Output, Replica, Status};

const OK: i32 = 0;
const INVALID: i32 = -1;
const CONFIG: i32 = -2;
const CLIENT_JSON: i32 = -4;
const VRR_MESSAGE: i32 = -5;
const TOO_LARGE: i32 = -6;
const SERVICE: i32 = -7;
const PANIC: i32 = -127;

const OUTPUT_BROADCAST: u32 = 1;
const OUTPUT_TO: u32 = 2;
const OUTPUT_REPLY: u32 = 3;

struct Queued {
    kind: u32,
    to: u32,
    header: Option<Header>,
    message_id: [u8; 16],
    bytes: Vec<u8>,
}

pub struct Node {
    replica: Replica,
    outputs: VecDeque<Queued>,
    service: Service,
    nonce_path: PathBuf,
    poisoned: bool,
}

impl Node {
    fn run(
        replica: &mut Replica,
        service: &mut Service,
        outputs: Vec<Output>,
    ) -> Result<VecDeque<Queued>, i32> {
        let mut pending = VecDeque::from(outputs);
        let mut staged = VecDeque::new();
        while let Some(output) = pending.pop_front() {
            match output {
                Output::Broadcast(message) => {
                    staged.push_back(Self::peer(OUTPUT_BROADCAST, 0, message)?)
                }
                Output::To(to, message) => staged.push_back(Self::peer(OUTPUT_TO, to, message)?),
                Output::Execute {
                    slot,
                    client_id,
                    request_num,
                    message_id,
                    execution_time,
                    payload,
                } => {
                    let bytes = service
                        .execute(message_id, client_id, request_num, execution_time, &payload)
                        .map_err(|_| SERVICE)?;
                    let next = replica.step(Input::Complete {
                        slot,
                        result: bytes,
                    });
                    if replica.executed() != slot {
                        return Err(SERVICE);
                    }
                    for output in next.into_iter().rev() {
                        pending.push_front(output);
                    }
                }
                Output::Reply(bytes) => {
                    if bytes.len() > MAX_DATAGRAM {
                        return Err(TOO_LARGE);
                    }
                    let message_id = Service::response_message_id(&bytes).ok_or(SERVICE)?;
                    staged.push_back(Queued {
                        kind: OUTPUT_REPLY,
                        to: 0,
                        header: None,
                        message_id: *message_id.as_bytes(),
                        bytes,
                    });
                }
            }
        }
        Ok(staged)
    }

    fn peer(kind: u32, to: u32, message: Message) -> Result<Queued, i32> {
        let header = message.header();
        let bytes = message.encode().map_err(|_| VRR_MESSAGE)?;
        if bytes.len() > MAX_DATAGRAM {
            return Err(TOO_LARGE);
        }
        Ok(Queued {
            kind,
            to,
            header: Some(header),
            message_id: [0; 16],
            bytes,
        })
    }

    fn step(&mut self, input: Input) -> i32 {
        if self.poisoned {
            return SERVICE;
        }
        if let Input::Message { message, .. } = &input {
            if !valid_message_payloads(message) {
                return VRR_MESSAGE;
            }
        }
        match catch_unwind(AssertUnwindSafe(|| {
            let mut replica = self.replica.clone();
            let mut service = self.service.clone();
            let outputs = replica.step(input);
            let staged = Self::run(&mut replica, &mut service, outputs)?;
            self.replica = replica;
            self.service = service;
            self.outputs.extend(staged);
            Ok(())
        })) {
            Ok(Ok(())) => OK,
            Ok(Err(error)) => error,
            Err(_) => {
                self.poisoned = true;
                self.outputs.clear();
                PANIC
            }
        }
    }

    fn recover(&mut self) -> i32 {
        let Ok(nonce) = next_nonce(&self.nonce_path) else {
            return CONFIG;
        };
        self.step(Input::Recover { nonce })
    }
}

fn valid_entry(entry: &LogEntry) -> bool {
    Service::validate(
        entry.message_id,
        entry.client_id,
        entry.request_num,
        &entry.payload,
    )
}

fn valid_message_payloads(message: &Message) -> bool {
    match &message.body {
        Body::Prepare { entry, .. } => valid_entry(entry),
        Body::DoEpochChange { state, .. } | Body::StartEpoch { state } => {
            state.log.iter().all(valid_entry)
        }
        Body::RecoveryResponse {
            state: Some(state), ..
        } => state.log.iter().all(valid_entry),
        _ => true,
    }
}

unsafe fn bytes<'a>(len: usize, data: *const u8) -> Result<&'a [u8], i32> {
    if data.is_null() {
        return if len == 0 { Ok(&[]) } else { Err(INVALID) };
    }
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}

fn unix_millis() -> Result<u64, i32> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SERVICE)
        .and_then(|duration| u64::try_from(duration.as_millis()).map_err(|_| SERVICE))
}

fn initialize_nonce(path: &Path) -> std::io::Result<bool> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(b"0\n")?;
            file.sync_all()?;
            sync_parent(path)?;
            Ok(false)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            read_nonce(path)?;
            Ok(true)
        }
        Err(error) => Err(error),
    }
}

fn read_nonce(path: &Path) -> std::io::Result<u64> {
    fs::read_to_string(path)?
        .trim()
        .parse()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid nonce"))
}

fn next_nonce(path: &Path) -> std::io::Result<u64> {
    let nonce = read_nonce(path)?
        .checked_add(1)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "nonce overflow"))?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let base = path.file_name().unwrap_or_default();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let mut temporary = OsString::from(".");
    temporary.push(base);
    temporary.push(format!(".tmp-{}-{unique}", std::process::id()));
    let temporary = parent.join(temporary);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let result = (|| {
        writeln!(file, "{nonce}")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map(|_| nonce)
}

fn sync_parent(path: &Path) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    sync_dir(parent)
}

// Fsyncing the containing directory after create/rename is a POSIX crash-
// consistency idiom (persist the new directory entry, not just the file's
// data). Windows has no equivalent: `File::open` on a directory fails with
// ERROR_ACCESS_DENIED (std does not set FILE_FLAG_BACKUP_SEMANTICS), and NTFS
// does not require or support an explicit directory fsync for this guarantee
// the way POSIX filesystems do. Other Rust crates with the same durability
// pattern (e.g. `atomicwrites`) no-op this step on Windows for the same
// reason; do the same here rather than fail every nonce write on Windows.
#[cfg(unix)]
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(windows)]
fn sync_dir(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

fn guarded(run: impl FnOnce() -> i32) -> i32 {
    catch_unwind(AssertUnwindSafe(run)).unwrap_or(PANIC)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_new(
    members_len: usize,
    members_data: *const u8,
    own_len: usize,
    own_data: *const u8,
    state_len: usize,
    state_data: *const u8,
    out: *mut *mut c_void,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return INVALID;
        }
        let Ok(members_data) = (unsafe { bytes(members_len, members_data) }) else {
            return INVALID;
        };
        let Ok(own_data) = (unsafe { bytes(own_len, own_data) }) else {
            return INVALID;
        };
        let Ok(state_data) = (unsafe { bytes(state_len, state_data) }) else {
            return INVALID;
        };
        let Some(members) = members_data
            .split(|byte| *byte == 0)
            .map(|member| std::str::from_utf8(member).ok().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
        else {
            return CONFIG;
        };
        let Ok(own) = std::str::from_utf8(own_data) else {
            return CONFIG;
        };
        let Ok(state) = std::str::from_utf8(state_data) else {
            return CONFIG;
        };
        if state.is_empty() {
            return CONFIG;
        }
        let Ok(replica) = Replica::new(members, own) else {
            return CONFIG;
        };
        let nonce_path = PathBuf::from(state);
        let Ok(previously_started) = initialize_nonce(&nonce_path) else {
            return CONFIG;
        };
        let mut node = Node {
            replica,
            outputs: VecDeque::new(),
            service: Service::default(),
            nonce_path,
            poisoned: false,
        };
        if previously_started && node.recover() != OK {
            return CONFIG;
        }
        unsafe { *out = Box::into_raw(Box::new(node)).cast() };
        OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_free(node: *mut c_void) {
    if !node.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
            drop(Box::from_raw(node.cast::<Node>()));
        }));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_request(
    node: *mut c_void,
    json_len: usize,
    json: *const u8,
) -> i32 {
    guarded(|| {
        let Some(node) = (unsafe { node.cast::<Node>().as_mut() }) else {
            return INVALID;
        };
        let Ok(json) = (unsafe { bytes(json_len, json) }) else {
            return INVALID;
        };
        if json.len() > MAX_DATAGRAM {
            return TOO_LARGE;
        }
        let Ok(request) = Service::decode(json) else {
            return CLIENT_JSON;
        };
        let (message_id, client_id, request_num) = request.ids();
        let Ok(execution_time) = unix_millis() else {
            return SERVICE;
        };
        if node.replica.request_datagram_size(
            client_id,
            request_num,
            message_id,
            execution_time,
            json,
        ) > MAX_DATAGRAM
        {
            return TOO_LARGE;
        }
        node.step(Input::Request {
            client_id,
            request_num,
            message_id,
            execution_time,
            payload: json.to_vec(),
        })
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_receive(
    node: *mut c_void,
    from: u32,
    len: usize,
    data: *const u8,
) -> i32 {
    guarded(|| {
        let Some(node) = (unsafe { node.cast::<Node>().as_mut() }) else {
            return INVALID;
        };
        let Ok(data) = (unsafe { bytes(len, data) }) else {
            return INVALID;
        };
        if data.len() > MAX_DATAGRAM {
            return TOO_LARGE;
        }
        let Some(message) = Message::decode(data) else {
            return VRR_MESSAGE;
        };
        node.step(Input::Message { from, message })
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_idle(node: *mut c_void) -> i32 {
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.step(Input::Idle))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_leader_timeout(node: *mut c_void) -> i32 {
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.step(Input::LeaderTimeout))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_recover(node: *mut c_void) -> i32 {
    guarded(|| unsafe { node.cast::<Node>().as_mut().map_or(INVALID, Node::recover) })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_status(
    node: *mut c_void,
    out_status: *mut u32,
    out_leader: *mut u32,
    out_epoch: *mut u32,
) -> i32 {
    guarded(|| {
        if node.is_null() || out_status.is_null() || out_leader.is_null() || out_epoch.is_null() {
            return INVALID;
        }
        let node = unsafe { &mut *node.cast::<Node>() };
        let status = match node.replica.status() {
            Status::Normal => 0,
            Status::EpochChange => 1,
            Status::Recovering => 2,
            Status::Replaying => 3,
        };
        unsafe {
            *out_status = status;
            let epoch = node.replica.epoch();
            *out_leader = node.replica.leader_of(epoch);
            *out_epoch = epoch;
        }
        OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_leader_for_epoch(
    node: *mut c_void,
    epoch: u32,
    out_leader: *mut u32,
) -> i32 {
    guarded(|| {
        if node.is_null() || out_leader.is_null() {
            return INVALID;
        }
        let node = unsafe { &mut *node.cast::<Node>() };
        unsafe { *out_leader = node.replica.leader_of(epoch) };
        OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_next(
    node: *mut c_void,
    out_kind: *mut u32,
    out_to: *mut u32,
    out_tag: *mut u32,
    out_epoch: *mut u32,
    out_slot_hi: *mut u32,
    out_slot_lo: *mut u32,
    out_message_id: *mut u8,
    capacity: usize,
    out_len: *mut usize,
    out_data: *mut u8,
) -> i32 {
    guarded(|| {
        if node.is_null()
            || out_kind.is_null()
            || out_to.is_null()
            || out_tag.is_null()
            || out_epoch.is_null()
            || out_slot_hi.is_null()
            || out_slot_lo.is_null()
            || out_message_id.is_null()
            || out_len.is_null()
        {
            return INVALID;
        }
        let node = unsafe { &mut *node.cast::<Node>() };
        let Some(output) = node.outputs.front() else {
            return 0;
        };
        unsafe { *out_len = output.bytes.len() };
        if output.bytes.len() > capacity || (out_data.is_null() && !output.bytes.is_empty()) {
            return TOO_LARGE;
        }
        unsafe {
            *out_kind = output.kind;
            *out_to = output.to;
            if let Some(header) = output.header {
                *out_tag = header.tag as u32;
                *out_epoch = header.epoch;
                *out_slot_hi = (header.slot >> 32) as u32;
                *out_slot_lo = header.slot as u32;
            } else {
                *out_tag = 0;
                *out_epoch = 0;
                *out_slot_hi = 0;
                *out_slot_lo = 0;
            }
            ptr::copy_nonoverlapping(output.message_id.as_ptr(), out_message_id, 16);
            if !output.bytes.is_empty() {
                ptr::copy_nonoverlapping(output.bytes.as_ptr(), out_data, output.bytes.len());
            }
        }
        node.outputs.pop_front();
        1
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn state_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "lunet-advisory-lock-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock is after Unix epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn recovery_nonces_are_created_then_durably_incremented() {
        let path = state_path("nonce");
        assert!(!initialize_nonce(&path).expect("first boot creates nonce"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "0\n");
        assert_eq!(next_nonce(&path).unwrap(), 1);
        assert_eq!(fs::read_to_string(&path).unwrap(), "1\n");
        assert_eq!(next_nonce(&path).unwrap(), 2);
        assert_eq!(fs::read_to_string(&path).unwrap(), "2\n");
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn committed_request_produces_a_correlated_reply_and_duplicate_replay() {
        let members = vec!["n1".to_owned(), "n2".to_owned(), "n3".to_owned()];
        let mut leader = Node {
            replica: Replica::new(members.clone(), "n1").unwrap(),
            outputs: VecDeque::new(),
            service: Service::default(),
            nonce_path: state_path("leader"),
            poisoned: false,
        };
        let mut first_backup = Node {
            replica: Replica::new(members.clone(), "n2").unwrap(),
            outputs: VecDeque::new(),
            service: Service::default(),
            nonce_path: state_path("backup-one"),
            poisoned: false,
        };
        let mut second_backup = Node {
            replica: Replica::new(members, "n3").unwrap(),
            outputs: VecDeque::new(),
            service: Service::default(),
            nonce_path: state_path("backup-two"),
            poisoned: false,
        };
        let message_id = Uuid::from_bytes([7; 16]);
        let request = crate::locks::Request::Get {
            message_id,
            client_id: 11,
            request_num: 13,
            lock_id: 17,
        };
        let payload = serde_json::to_vec(&request).unwrap();

        assert_eq!(
            leader.step(Input::Request {
                client_id: 11,
                request_num: 13,
                message_id,
                execution_time: 100,
                payload: payload.clone(),
            }),
            OK
        );
        let prepare = leader
            .outputs
            .pop_front()
            .expect("leader broadcasts prepare");
        assert_eq!(prepare.kind, OUTPUT_BROADCAST);
        let prepare = Message::decode(&prepare.bytes).unwrap();
        assert_eq!(
            first_backup.step(Input::Message {
                from: 0,
                message: prepare.clone(),
            }),
            OK
        );
        assert_eq!(
            second_backup.step(Input::Message {
                from: 0,
                message: prepare,
            }),
            OK
        );
        let prepare_ok = first_backup
            .outputs
            .pop_front()
            .expect("backup acknowledges");
        assert_eq!(prepare_ok.kind, OUTPUT_TO);
        assert_eq!(
            leader.step(Input::Message {
                from: 1,
                message: Message::decode(&prepare_ok.bytes).unwrap(),
            }),
            OK
        );
        let reply = leader
            .outputs
            .iter()
            .find(|output| output.kind == OUTPUT_REPLY)
            .expect("quorum commits and replies");
        assert_eq!(reply.message_id, *message_id.as_bytes());
        let first_reply = reply.bytes.clone();
        leader.outputs.clear();

        assert_eq!(
            leader.step(Input::Request {
                client_id: 11,
                request_num: 13,
                message_id,
                execution_time: 999,
                payload,
            }),
            OK
        );
        let replay = leader
            .outputs
            .pop_front()
            .expect("duplicate reply replayed");
        assert_eq!(replay.kind, OUTPUT_REPLY);
        assert_eq!(replay.message_id, *message_id.as_bytes());
        assert_eq!(replay.bytes, first_reply);
    }
}
