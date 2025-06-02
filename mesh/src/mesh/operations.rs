use super::topic::MeshLogId;
use p2panda_core::Body;
use p2panda_core::PruneFlag;
use p2panda_core::cbor::DecodeError;
use p2panda_core::cbor::decode_cbor;
use p2panda_core::{Hash, Header, Operation, PrivateKey};
use serde::{Deserialize, Serialize};

use super::event::MeshEvent;
use super::topic::InstanceId;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extensions {
    pub prune_flag: PruneFlag,
    pub log_id: MeshLogId,
}

impl TryFrom<&[u8]> for Extensions {
    type Error = DecodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        decode_cbor(value)
    }
}

pub struct LinkedOperations {
    key: PrivateKey,
    seq_num: u64,
    backlink: Option<Hash>,
    log_id: MeshLogId,
}

impl LinkedOperations {
    pub fn new(key: PrivateKey, instance_id: InstanceId) -> LinkedOperations {
        LinkedOperations {
            key,
            seq_num: 0,
            backlink: None,
            log_id: MeshLogId(instance_id),
        }
    }

    pub fn next(&mut self, event: MeshEvent) -> Operation<Extensions> {
        let operation = self.make_operation(&event);
        self.seq_num += 1;
        self.backlink = Some(operation.hash);
        operation
    }

    fn make_operation(&self, event: &MeshEvent) -> Operation<Extensions> {
        let prune_flag = matches!(event, MeshEvent::Snapshot { .. });
        let body = Body::new(&event.to_bytes());
        let mut header = Header {
            version: 1,
            public_key: self.key.public_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp: 0,
            seq_num: self.seq_num,
            backlink: self.backlink,
            previous: vec![],
            extensions: Some(Extensions {
                prune_flag: PruneFlag::new(prune_flag),
                log_id: self.log_id.clone(),
            }),
        };
        header.sign(&self.key);
        let hash = header.hash();
        Operation {
            hash,
            header,
            body: Some(body),
        }
    }
}
