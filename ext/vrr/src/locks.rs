use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    pub lease_id: u64,
    pub holder: Uuid,
    pub expiry: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Get {
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        lock_id: u64,
    },
    Set {
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        lock_id: u64,
        lease: Lease,
    },
}

impl Request {
    pub fn ids(&self) -> (Uuid, u64, u64) {
        match self {
            Self::Get {
                message_id,
                client_id,
                request_num,
                ..
            }
            | Self::Set {
                message_id,
                client_id,
                request_num,
                ..
            } => (*message_id, *client_id, *request_num),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Response {
    Get {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        lease: Option<Lease>,
    },
    Set {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        granted: bool,
        lease: Option<Lease>,
    },
}

#[derive(Default)]
pub struct Service {
    locks: BTreeMap<u64, Lease>,
}

impl Service {
    pub fn decode(bytes: &[u8]) -> Result<Request, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    pub fn validate(message_id: Uuid, client_id: u64, request_num: u64, payload: &[u8]) -> bool {
        Self::decode(payload)
            .is_ok_and(|request| request.ids() == (message_id, client_id, request_num))
    }

    pub fn execute(
        &mut self,
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        execution_time: u64,
        payload: &[u8],
    ) -> Result<Vec<u8>, serde_json::Error> {
        let request = Self::decode(payload)?;
        if request.ids() != (message_id, client_id, request_num) {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "replicated envelope does not match lock payload",
            ));
        }
        let response = match request {
            Request::Get {
                message_id,
                request_num,
                lock_id,
                ..
            } => Response::Get {
                message_id,
                request_num,
                lock_id,
                lease: self.live(execution_time, lock_id),
            },
            Request::Set {
                message_id,
                request_num,
                lock_id,
                lease,
                ..
            } => {
                let held = self.live(execution_time, lock_id);
                let granted = held.is_none_or(|current| current.holder == lease.holder);
                if granted {
                    self.locks.insert(lock_id, lease);
                }
                Response::Set {
                    message_id,
                    request_num,
                    lock_id,
                    granted,
                    lease: if granted { Some(lease) } else { held },
                }
            }
        };
        let bytes = serde_json::to_vec(&response)?;
        Ok(bytes)
    }

    fn live(&self, execution_time: u64, lock_id: u64) -> Option<Lease> {
        self.locks
            .get(&lock_id)
            .copied()
            .filter(|lease| lease.expiry > execution_time)
    }
}
