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
    log_id: MeshLogId,
    seq_num: u64,
    last_snapshot_seq_num: u64,
    backlink: Option<Hash>,
}

impl LinkedOperations {
    pub fn new(key: PrivateKey, instance_id: InstanceId) -> LinkedOperations {
        LinkedOperations {
            key,
            log_id: MeshLogId(instance_id),
            backlink: None,
            seq_num: 0,
            last_snapshot_seq_num: 0,
        }
    }

    pub fn next(&mut self, event: MeshEvent) -> Operation<Extensions> {
        let is_snapshot = matches!(event, MeshEvent::Snapshot { .. });
        let operation = self.make_operation(&event, is_snapshot);
        if is_snapshot {
            self.last_snapshot_seq_num = self.seq_num;
        }
        self.seq_num += 1;
        self.backlink = Some(operation.hash);
        operation
    }

    fn make_operation(&self, event: &MeshEvent, prune_flag: bool) -> Operation<Extensions> {
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

    pub fn count_since_snapshot(&self) -> u64 {
        self.seq_num.wrapping_sub(self.last_snapshot_seq_num)
    }
}
