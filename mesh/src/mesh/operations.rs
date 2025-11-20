use crate::mesh::types::MembershipEvent;

use super::topic::MeshLogId;
use p2panda_core::Body;
use p2panda_core::PruneFlag;
use p2panda_core::cbor::DecodeError;
use p2panda_core::cbor::decode_cbor;
use p2panda_core::{Hash, Header, Operation, PrivateKey};
use serde::{Deserialize, Serialize};

use super::event::MeshEvent;
use super::topic::InstanceId;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    MeshEvent,
    MembershipUpdate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extensions {
    pub prune_flag: PruneFlag,
    pub log_id: MeshLogId,
    pub event_type: EventType,
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

    pub fn next(&mut self, event: MeshEvent, timestamp: u64) -> Operation<Extensions> {
        let is_snapshot = matches!(event, MeshEvent::Snapshot { .. });
        let event_bytes = event.to_bytes();
        let operation =
            self.make_operation(&event_bytes, EventType::MeshEvent, is_snapshot, timestamp);
        if is_snapshot {
            self.last_snapshot_seq_num = self.seq_num;
        }
        self.seq_num += 1;
        self.backlink = Some(operation.hash);
        operation
    }

    pub fn next_membership_update(
        &mut self,
        event: MembershipEvent,
        timestamp: u64,
    ) -> Operation<Extensions> {
        let event_bytes = event.to_bytes();
        let operation =
            self.make_operation(&event_bytes, EventType::MembershipUpdate, false, timestamp);
        self.seq_num += 1;
        self.backlink = Some(operation.hash);
        operation
    }

    fn make_operation(
        &self,
        event_bytes: &[u8],
        event_type: EventType,
        prune_flag: bool,
        timestamp: u64,
    ) -> Operation<Extensions> {
        let body = Body::new(event_bytes);
        let mut header = Header {
            version: 1,
            public_key: self.key.public_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp,
            seq_num: self.seq_num,
            backlink: self.backlink,
            previous: vec![],
            extensions: Some(Extensions {
                prune_flag: PruneFlag::new(prune_flag),
                log_id: self.log_id.clone(),
                event_type,
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

#[cfg(test)]
pub mod tests {
    use std::collections::BTreeMap;

    use kube::api::DynamicObject;
    use meshkube::kube::{dynamic_object_ext::DynamicObjectExt, subscriptions::Version};
    use p2panda_core::PrivateKey;

    use crate::{
        mesh::{
            event::MeshEvent, operations::LinkedOperations, topic::InstanceId,
            types::MembershipEvent,
        },
        network::discovery::{
            nodes::{MembershipState, PeerEvent},
            types::{Membership, MembershipUpdate, PeerStateUpdate},
        },
    };

    #[test]
    fn test_ordinary_operation_flow() {
        let key = PrivateKey::new();
        let instance_id = InstanceId::new("test-instance".into());
        let mut linked_operations = LinkedOperations::new(key.clone(), instance_id);
        assert_eq!(linked_operations.count_since_snapshot(), 0);

        // Snapshot1 event
        let event1 = MeshEvent::Snapshot {
            snapshot: BTreeMap::new(),
            version: 1,
        };
        let operation1 = linked_operations.next(event1.clone(), 1);
        assert_eq!(operation1.header.backlink, None);
        assert_eq!(operation1.header.seq_num, 0);
        assert!(operation1.header.extensions.unwrap().prune_flag.is_set());
        assert_eq!(operation1.header.public_key, key.public_key());
        assert_eq!(operation1.body.unwrap().to_bytes(), event1.to_bytes());
        assert_eq!(linked_operations.count_since_snapshot(), 1);

        // Update event
        let event2 = MeshEvent::Update {
            object: make_object("test", 1, "data"),
            version: 1,
        };
        let operation2 = linked_operations.next(event2.clone(), 2);
        assert_eq!(operation2.header.backlink, Some(operation1.hash));
        assert_eq!(operation2.header.seq_num, 1);
        assert!(!operation2.header.extensions.unwrap().prune_flag.is_set());
        assert_eq!(operation2.header.public_key, key.public_key());
        assert_eq!(operation2.body.unwrap().to_bytes(), event2.to_bytes());
        assert_eq!(linked_operations.count_since_snapshot(), 2);

        // Snapshot2 event
        let event3 = MeshEvent::Snapshot {
            snapshot: BTreeMap::new(),
            version: 2,
        };
        let operation3 = linked_operations.next(event3.clone(), 3);
        assert_eq!(operation3.header.backlink, Some(operation2.hash));
        assert_eq!(operation3.header.seq_num, 2);
        assert!(operation3.header.extensions.unwrap().prune_flag.is_set());
        assert_eq!(operation3.header.public_key, key.public_key());
        assert_eq!(operation3.body.unwrap().to_bytes(), event3.to_bytes());
        assert_eq!(linked_operations.count_since_snapshot(), 1);
    }

    #[test]
    pub fn test_membership_update() {
        let key = PrivateKey::new();
        let instance_id = InstanceId::new("test-instance".into());
        let mut linked_operations = LinkedOperations::new(key.clone(), instance_id);
        assert_eq!(linked_operations.count_since_snapshot(), 0);

        // membership update
        let event1 = MembershipEvent::Update {
            update: MembershipUpdate {
                membership: Membership::new(1),
                peers: vec![PeerStateUpdate {
                    peer: key.public_key(),
                    state: MembershipState::Ready { since: 1 },
                    instance: Some(InstanceId {
                        zone: "zone".into(),
                        start_time: 0,
                    }),
                    timestamp: 1,
                }],
            },
            peer_event: PeerEvent::PeerDiscovered {
                peer: key.public_key(),
                now: 1,
            },
        };
        let operation1 = linked_operations.next_membership_update(event1.clone(), 1);
        assert_eq!(operation1.header.backlink, None);
        assert_eq!(operation1.header.seq_num, 0);
        assert!(!operation1.header.extensions.unwrap().prune_flag.is_set());
        assert_eq!(operation1.header.public_key, key.public_key());
        assert_eq!(operation1.body.unwrap().to_bytes(), event1.to_bytes());
        assert_eq!(linked_operations.count_since_snapshot(), 1);
    }

    fn make_object(zone: &str, version: Version, data: &str) -> DynamicObject {
        let mut object: DynamicObject = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "example",
                "namespace": "default"
            },
            "spec": {
                "data": data,
            }
        }))
        .unwrap();
        object.set_owner_zone(zone.into());
        object.set_owner_version(version);
        object
    }
}
