use anyhow::Result;
use p2panda_core::{Body, Header, Operation};
use serde::{Deserialize, Serialize};

use crate::mesh::operations::Extensions;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkMessage {
    pub payload: NetworkPayload,
}

impl NetworkMessage {
    pub fn new(op: Operation<Extensions>) -> Self {
        Self {
            payload: NetworkPayload::Operation(op.header, op.body),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let message: NetworkMessage = ciborium::from_reader(bytes)?;
        Ok(message)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("encoding network message");
        bytes
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum NetworkPayload {
    Operation(Header<Extensions>, Option<Body>),
}

impl NetworkPayload {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let message: Self = ciborium::from_reader(bytes)?;
        Ok(message)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("encoding network message");
        bytes
    }
}
