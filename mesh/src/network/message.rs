use anyhow::Result;
use bytes::Bytes;
use p2panda_core::{Hash, PrivateKey, PublicKey, Signature};
use serde::{Deserialize, Serialize};

use crate::kube::types::CacheProtocol;

/// Messages which are exchanged in the p2panda network, via sync or gossip.
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkMessage {
    pub payload: NetworkPayload,

    /// Public key of the source.
    pub public_key: PublicKey,

    /// Cryptographic proof from original author that they are indeed authentic.
    #[serde(skip_serializing_if = "Option::is_none", default = "Option::default")]
    pub signature: Option<Signature>,
}

impl NetworkMessage {
    pub fn new_resource_msg(
        source: String,
        message: CacheProtocol,
        public_key: &PublicKey,
    ) -> Self {
        let payload = match message {
            CacheProtocol::Update(_) | CacheProtocol::Delete(_) => {
                NetworkPayload::ResourceUpdate(source, message.to_bytes().into())
            }
            CacheProtocol::Snapshot { .. } => {
                NetworkPayload::ResourceSnapshot(source, message.to_bytes().into())
            }
        };
        Self {
            public_key: public_key.to_owned(),
            signature: None,
            payload,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let message: Self = ciborium::from_reader(bytes)?;
        Ok(message)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("encoding network message");
        bytes
    }

    pub fn hash(&self) -> Hash {
        Hash::new(self.to_bytes())
    }

    pub fn sign(&mut self, private_key: &PrivateKey) {
        self.signature = None;
        let bytes = self.to_bytes();
        let signature = private_key.sign(&bytes);
        self.signature = Some(signature);
    }

    pub fn verify(&self) -> bool {
        match &self.signature {
            Some(signature) => {
                let mut message = self.clone();

                // Remove existing signature.
                message.signature = None;

                let bytes = message.to_bytes();
                self.public_key.verify(&bytes, signature)
            }
            None => false,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum NetworkPayload {
    #[serde(rename = "update")]
    ResourceUpdate(String, Bytes),

    #[serde(rename = "snapshot")]
    ResourceSnapshot(String, Bytes),
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
#[cfg(test)]
mod tests {
    use p2panda_core::PrivateKey;

    use super::{NetworkMessage, NetworkPayload};

    #[test]
    fn signature() {
        let private_key = PrivateKey::new();
        let public_key = private_key.public_key();
        let mut message = NetworkMessage {
            payload: NetworkPayload::ResourceUpdate("bucket-out".into(), "payload".into()),
            public_key,
            signature: None,
        };
        assert!(!message.verify());
        message.sign(&private_key);
        assert!(message.verify());
        message.public_key = PrivateKey::new().public_key();
        assert!(!message.verify());
    }
}
