use crate::locks::Service;
use crate::vrr::{
    Body, Header, Input, LogEntry, Message, NoJournal, Output, Replica, MAX_DATAGRAM,
};
use std::collections::VecDeque;
use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

const OK: i32 = 0;
const INVALID: i32 = -1;
const CONFIG: i32 = -2;
const NUMBER: i32 = -3;
const CLIENT_JSON: i32 = -4;
const VRR_MESSAGE: i32 = -5;
const TOO_LARGE: i32 = -6;
const SERVICE: i32 = -7;
const PANIC: i32 = -127;

struct Queued {
    kind: u32,
    to: u32,
    header: Option<Header>,
    bytes: Vec<u8>,
}

pub struct Node {
    replica: Replica,
    outputs: VecDeque<Queued>,
    service: Service,
    poisoned: bool,
}

impl Node {
    fn run(&mut self, outputs: Vec<Output>) -> Result<(), i32> {
        let mut pending = VecDeque::from(outputs);
        let mut staged = VecDeque::new();
        while let Some(output) = pending.pop_front() {
            match output {
                Output::Broadcast(message) => staged.push_back(Self::peer(1, 0, message)?),
                Output::To(to, message) => staged.push_back(Self::peer(2, to, message)?),
                Output::Execute {
                    slot,
                    client_id,
                    request_num,
                    message_id,
                    execution_time,
                    payload,
                } => {
                    let bytes = self
                        .service
                        .execute(message_id, client_id, request_num, execution_time, &payload)
                        .map_err(|_| SERVICE)?;
                    let next = self.replica.step(
                        &mut NoJournal,
                        Input::Complete {
                            slot,
                            result: bytes,
                        },
                    );
                    if self.replica.executed() != slot {
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
                    staged.push_back(Queued {
                        kind: 3,
                        to: 0,
                        header: None,
                        bytes,
                    });
                }
            }
        }
        self.outputs.extend(staged);
        Ok(())
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
            let outputs = self.replica.step(&mut NoJournal, input);
            self.run(outputs)
        })) {
            Ok(Ok(())) => OK,
            Ok(Err(error)) => {
                self.poisoned = true;
                self.outputs.clear();
                error
            }
            Err(_) => {
                self.poisoned = true;
                self.outputs.clear();
                PANIC
            }
        }
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

fn decimal(bytes: &[u8]) -> Result<u64, i32> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or(NUMBER)
}

fn guarded(run: impl FnOnce() -> i32) -> i32 {
    catch_unwind(AssertUnwindSafe(run)).unwrap_or(PANIC)
}

#[no_mangle]
pub unsafe extern "C" fn vrr_node_new(
    members_len: usize,
    members_data: *const u8,
    own_len: usize,
    own_data: *const u8,
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
        let Ok(replica) = Replica::new(members, own) else {
            return CONFIG;
        };
        let node = Box::new(Node {
            replica,
            outputs: VecDeque::new(),
            service: Service::default(),
            poisoned: false,
        });
        unsafe { *out = Box::into_raw(node).cast() };
        OK
    })
}

#[no_mangle]
pub unsafe extern "C" fn vrr_node_free(node: *mut c_void) {
    if !node.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
            drop(Box::from_raw(node.cast::<Node>()));
        }));
    }
}

#[no_mangle]
pub unsafe extern "C" fn vrr_node_request(
    node: *mut c_void,
    execution_time_len: usize,
    execution_time: *const u8,
    json_len: usize,
    json: *const u8,
) -> i32 {
    guarded(|| {
        let Some(node) = (unsafe { node.cast::<Node>().as_mut() }) else {
            return INVALID;
        };
        let Ok(time) = (unsafe { bytes(execution_time_len, execution_time) }) else {
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
        let Ok(execution_time) = decimal(time) else {
            return NUMBER;
        };
        let input = Input::Request {
            client_id,
            request_num,
            message_id,
            execution_time,
            payload: json.to_vec(),
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
        node.step(input)
    })
}

#[no_mangle]
pub unsafe extern "C" fn vrr_node_receive(
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

#[no_mangle]
pub unsafe extern "C" fn vrr_node_idle(node: *mut c_void) -> i32 {
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.step(Input::Idle))
    })
}

#[no_mangle]
pub unsafe extern "C" fn vrr_node_leader_timeout(node: *mut c_void) -> i32 {
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.step(Input::LeaderTimeout))
    })
}

#[no_mangle]
pub unsafe extern "C" fn vrr_node_recover(
    node: *mut c_void,
    nonce_len: usize,
    nonce: *const u8,
) -> i32 {
    guarded(|| {
        let Some(node) = (unsafe { node.cast::<Node>().as_mut() }) else {
            return INVALID;
        };
        let Ok(nonce) = (unsafe { bytes(nonce_len, nonce) }) else {
            return INVALID;
        };
        match decimal(nonce) {
            Ok(nonce) => node.step(Input::Recover { nonce }),
            Err(error) => error,
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn vrr_node_next(
    node: *mut c_void,
    out_kind: *mut u32,
    out_to: *mut u32,
    out_tag: *mut u32,
    out_epoch: *mut u32,
    out_slot_hi: *mut u32,
    out_slot_lo: *mut u32,
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
            if !output.bytes.is_empty() {
                ptr::copy_nonoverlapping(output.bytes.as_ptr(), out_data, output.bytes.len());
            }
        }
        node.outputs.pop_front();
        1
    })
}
