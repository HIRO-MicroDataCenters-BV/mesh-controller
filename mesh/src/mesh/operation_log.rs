use std::collections::{BTreeMap, HashMap};

use crate::mesh::{operations::Extensions, topic::MeshLogId};
use anyhow::Result;
use p2panda_core::{Operation, PublicKey};
use p2panda_store::{LogStore, MemoryStore};
use p2panda_stream::operation::{IngestError, IngestResult, ingest_operation};
use tracing::{debug, error, trace};

const MAX_PENDING_BATCH_SIZE: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogKey(PublicKey, MeshLogId);

pub struct OperationLog {
    own_log_id: MeshLogId,
    own_public_key: PublicKey,
    incoming_pending: HashMap<LogKey, BTreeMap<u64, Operation<Extensions>>>,
    store: MemoryStore<MeshLogId, Extensions>,
    pointers: LogPointers,
}

impl OperationLog {
    pub fn new(
        own_log_id: MeshLogId,
        own_public_key: PublicKey,
        store: MemoryStore<MeshLogId, Extensions>,
    ) -> Self {
        let mut pointers = LogPointers::new();
        pointers.add(own_log_id.clone());
        Self {
            incoming_pending: HashMap::default(),
            pointers,
            store,
            own_log_id,
            own_public_key,
        }
    }

    pub async fn insert(&mut self, operation: Operation<Extensions>) -> Result<()> {
        let Operation { header, .. } = &operation;

        let Some(extensions) = header.extensions.as_ref() else {
            return Err(IngestError::MissingHeaderExtension("extension".into()).into());
        };

        let log_id = extensions.log_id.clone();

        let result = self.insert_internal(operation).await?;
        match result {
            IngestResult::Complete(op) => {
                debug!("Insert Operation({},{})", log_id.0.zone, op.header.seq_num);
                if log_id != self.own_log_id {
                    self.replay_pending_inserts(&LogKey(op.header.public_key, log_id.clone()))
                        .await?;
                }
            }
            IngestResult::Retry(header, body, _, ops_missing) => {
                debug!(
                    "Insert Operation({},{}) retrying, missing ops = {:?}",
                    log_id.0.zone, header.seq_num, ops_missing
                );
                if log_id != self.own_log_id {
                    self.incoming_pending
                        .entry(LogKey(header.public_key, log_id.clone()))
                        .or_default()
                        .insert(
                            header.seq_num,
                            Operation {
                                hash: header.hash(),
                                header,
                                body,
                            },
                        );
                }
            }
        }
        Ok(())
    }

    async fn insert_internal(
        &mut self,
        operation: Operation<Extensions>,
    ) -> Result<IngestResult<Extensions>, IngestError> {
        let Operation {
            hash: _,
            header,
            body,
        } = operation;

        let Some(extensions) = header.extensions.as_ref() else {
            return Err(IngestError::MissingHeaderExtension("extension".into()));
        };

        let log_id = extensions.log_id.clone();
        let prune_flag = extensions.prune_flag.is_set();

        let header_bytes = header.to_bytes();
        ingest_operation(
            &mut self.store,
            header,
            body.clone(),
            header_bytes,
            &log_id,
            prune_flag,
        )
        .await
        .inspect_err(|e| {
            error!("Error during ingest operation {}", e);
        })
    }

    pub fn update_active_log(&mut self, new_log: MeshLogId, old_log: Option<MeshLogId>) {
        self.pointers.add(new_log);
        if let Some(old) = &old_log {
            self.pointers.remove(old);
        }
    }

    async fn replay_pending_inserts(&mut self, key: &LogKey) -> Result<()> {
        let batch = self.get_pending_batch(key).await?;
        if batch.is_empty() {
            return Ok(());
        }
        for operation in batch {
            match self.insert_internal(operation).await? {
                IngestResult::Complete(operation) => {
                    let Some(pending) = self.incoming_pending.get_mut(key) else {
                        break;
                    };
                    pending.remove(&operation.header.seq_num);
                }
                IngestResult::Retry(_, _, _, _) => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn get_pending_batch(&self, key: &LogKey) -> Result<Vec<Operation<Extensions>>> {
        let Some(pending) = self.incoming_pending.get(key) else {
            return Ok(vec![]);
        };

        let Some((header, _)) = self.store.latest_operation(&key.0, &key.1).await? else {
            return Ok(vec![]);
        };

        let next_pointer = header.seq_num + 1;

        let pending_bulk = pending
            .range(next_pointer..)
            .take(MAX_PENDING_BATCH_SIZE)
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>();
        Ok(pending_bulk)
    }

    pub async fn truncate_obsolete_logs(
        &mut self,
        obsolete_logs: Vec<(PublicKey, Vec<MeshLogId>)>,
    ) -> Result<()> {
        for (source, log_ids) in obsolete_logs {
            for log_id in log_ids {
                self.pointers.remove(&log_id);
                if let Some((header, _)) = self.store.latest_operation(&source, &log_id).await? {
                    trace!("Truncating log {log_id:?}");
                    self.incoming_pending
                        .remove(&LogKey(header.public_key, log_id.clone()));
                    if let Err(err) = self
                        .store
                        .delete_operations(&source, &log_id, header.seq_num)
                        .await
                    {
                        error!("Error while truncating log {log_id:?}: {err}");
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn get_ready(
        &self,
        remote_active_logs: &HashMap<PublicKey, MeshLogId>,
    ) -> Option<Ready> {
        let incoming = self.get_ready_incoming(remote_active_logs).await;
        let outgoing = self.get_ready_outgoing().await;
        if incoming.is_empty() && outgoing.is_empty() {
            None
        } else {
            Some(Ready { incoming, outgoing })
        }
    }

    async fn get_ready_incoming(
        &self,
        remote_active_logs: &HashMap<PublicKey, MeshLogId>,
    ) -> HashMap<MeshLogId, Vec<Operation<Extensions>>> {
        let mut incoming = HashMap::<MeshLogId, Vec<Operation<Extensions>>>::new();
        for (peer_id, log_id) in remote_active_logs {
            let current = self.pointers.get_current(log_id);
            if let Some(current) = current {
                self.get_operations(peer_id, log_id, current)
                    .await
                    .into_iter()
                    .for_each(|ops| {
                        if !ops.is_empty() {
                            incoming.insert(log_id.to_owned(), ops);
                        }
                    });
            } else {
                error!("No current pointer for log {log_id:?}");
            }
        }
        incoming
    }

    async fn get_ready_outgoing(&self) -> Vec<Operation<Extensions>> {
        let own_pointer = self
            .pointers
            .get_current(&self.own_log_id)
            .unwrap_or_else(|| panic!("No current pointer for own log {}", self.own_log_id));
        self.get_operations(&self.own_public_key, &self.own_log_id, own_pointer)
            .await
            .unwrap_or_default()
    }

    async fn get_operations(
        &self,
        peer_id: &PublicKey,
        log_id: &MeshLogId,
        from: SeqNum,
    ) -> Option<Vec<Operation<Extensions>>> {
        match self.store.get_log(peer_id, log_id, Some(from)).await {
            Ok(log) => log
                .unwrap_or_default()
                .into_iter()
                .map(|(header, body)| Operation {
                    hash: header.hash(),
                    header,
                    body,
                })
                .collect::<Vec<_>>()
                .into(),
            Err(err) => {
                error!(
                    "Error while fetching log {log_id:?} for peer {peer_id:?}: {:?}",
                    err
                );
                None
            }
        }
    }

    pub fn advance_log_pointer(&mut self, log_id: &MeshLogId, seq_num: SeqNum) {
        self.pointers.advance(log_id, seq_num);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Ready {
    incoming: HashMap<MeshLogId, Vec<Operation<Extensions>>>,
    outgoing: Vec<Operation<Extensions>>,
}

impl Default for Ready {
    fn default() -> Self {
        Self::new()
    }
}

impl Ready {
    pub fn new() -> Self {
        Self {
            incoming: HashMap::new(),
            outgoing: Vec::new(),
        }
    }

    pub fn take_incoming(&mut self) -> HashMap<MeshLogId, Vec<Operation<Extensions>>> {
        std::mem::take(&mut self.incoming)
    }

    pub fn take_outgoing(&mut self) -> Vec<Operation<Extensions>> {
        std::mem::take(&mut self.outgoing)
    }
}

pub type SeqNum = u64;

#[derive(Debug, Clone, PartialEq)]
pub struct LogPointers {
    pointers: HashMap<MeshLogId, SeqNum>,
}

impl Default for LogPointers {
    fn default() -> Self {
        Self::new()
    }
}

impl LogPointers {
    pub fn new() -> Self {
        Self {
            pointers: HashMap::new(),
        }
    }
    pub fn add(&mut self, log_id: MeshLogId) {
        self.pointers.insert(log_id, 0);
    }

    pub fn remove(&mut self, log_id: &MeshLogId) {
        self.pointers.remove(log_id);
    }

    pub fn get_current(&self, log_id: &MeshLogId) -> Option<SeqNum> {
        self.pointers.get(log_id).cloned()
    }

    pub fn advance(&mut self, log_id: &MeshLogId, seq_num: SeqNum) {
        if let Some(current) = self.get_current(log_id) {
            if seq_num > current {
                self.pointers.insert(log_id.clone(), seq_num);
            }
        } else {
            self.pointers.insert(log_id.clone(), seq_num);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::{BTreeMap, HashMap};

    use anyhow::Result;
    use kube::api::DynamicObject;
    use maplit::hashmap;
    use p2panda_core::{PrivateKey, PublicKey};
    use p2panda_store::MemoryStore;

    use crate::{
        kube::{dynamic_object_ext::DynamicObjectExt, subscriptions::Version},
        mesh::{
            event::MeshEvent,
            operation_log::OperationLog,
            operations::LinkedOperations,
            topic::{InstanceId, MeshLogId},
        },
    };

    #[tokio::test]
    async fn insert_local_operations() -> Result<()> {
        let LocalTestSetup {
            mut own_linked_operations,
            own_mesh_log_id,
            mut log,
            active_logs,
            ..
        } = setup_local_log();

        let event1 = snapshot();
        let event2 = update();
        let event3 = snapshot();

        let op1 = own_linked_operations.next(event1);
        let op2 = own_linked_operations.next(event2);
        let op3 = own_linked_operations.next(event3);

        log.insert(op1.clone()).await?;
        log.insert(op2.clone()).await?;

        assert_ready(&mut log, &active_logs, hashmap! {}, vec![0, 1]).await;

        // the same ready if pointer is not advanced
        assert_ready(&mut log, &active_logs, hashmap! {}, vec![0, 1]).await;

        // advance pointers to commit the operations
        log.advance_log_pointer(&own_mesh_log_id, 2);
        assert_not_ready(&log, &active_logs).await;

        // more operations can be inserted
        log.insert(op3.clone()).await?;

        assert_ready(&mut log, &active_logs, hashmap! {}, vec![2]).await;
        log.advance_log_pointer(&own_mesh_log_id, 3);

        assert_not_ready(&log, &active_logs).await;

        Ok(())
    }

    #[tokio::test]
    async fn insert_remote_operations() -> Result<()> {
        let RemoteTestSetup {
            mut remote_linked_operations,
            remote_mesh_log_id,
            mut log,
            mut remote_active_logs,
            remote_key,
            ..
        } = setup_remote_log();

        let event_remote1 = snapshot();
        let event_remote2 = update();
        let event_remote3 = snapshot();

        let remote_op1 = remote_linked_operations.next(event_remote1);
        let remote_op2 = remote_linked_operations.next(event_remote2);
        let remote_op3 = remote_linked_operations.next(event_remote3);

        remote_active_logs.insert(remote_key.public_key(), remote_mesh_log_id.clone());

        log.update_active_log(remote_mesh_log_id.clone(), None);

        log.insert(remote_op1.clone()).await?;
        log.insert(remote_op2.clone()).await?;

        assert_ready(
            &mut log,
            &remote_active_logs,
            hashmap! { remote_mesh_log_id.clone() => vec![0, 1]},
            vec![],
        )
        .await;
        log.advance_log_pointer(&remote_mesh_log_id, 2);

        assert_not_ready(&log, &remote_active_logs).await;

        log.insert(remote_op3.clone()).await?;
        assert_ready(
            &mut log,
            &remote_active_logs,
            hashmap! { remote_mesh_log_id.clone() => vec![2]},
            vec![],
        )
        .await;
        log.advance_log_pointer(&remote_mesh_log_id, 3);

        assert_not_ready(&log, &remote_active_logs).await;

        Ok(())
    }

    #[tokio::test]
    async fn wait_till_gaps_are_filled_with_incremental_ops() -> Result<()> {
        let RemoteTestSetup {
            mut remote_linked_operations,
            remote_mesh_log_id,
            mut log,
            mut remote_active_logs,
            remote_key,
            ..
        } = setup_remote_log();

        let event_remote1 = snapshot();
        let event_remote2 = update();
        let event_remote3 = update();
        let event_remote4 = update();

        let remote_op1 = remote_linked_operations.next(event_remote1);
        let remote_op2 = remote_linked_operations.next(event_remote2);
        let remote_op3 = remote_linked_operations.next(event_remote3);
        let remote_op4 = remote_linked_operations.next(event_remote4);

        remote_active_logs.insert(remote_key.public_key(), remote_mesh_log_id.clone());

        log.update_active_log(remote_mesh_log_id.clone(), None);

        log.insert(remote_op1.clone()).await?;
        log.insert(remote_op4.clone()).await?;

        assert_ready(
            &mut log,
            &remote_active_logs,
            hashmap! { remote_mesh_log_id.clone() => vec![0]},
            vec![],
        )
        .await;
        log.advance_log_pointer(&remote_mesh_log_id, 1);

        assert_not_ready(&log, &remote_active_logs).await;

        log.insert(remote_op2.clone()).await?;
        log.insert(remote_op3.clone()).await?;
        assert_ready(
            &mut log,
            &remote_active_logs,
            hashmap! { remote_mesh_log_id.clone() => vec![1, 2, 3]},
            vec![],
        )
        .await;
        log.advance_log_pointer(&remote_mesh_log_id, 4);

        assert_not_ready(&log, &remote_active_logs).await;

        Ok(())
    }

    #[tokio::test]
    async fn wait_till_gaps_are_filled_with_next_snapshot() -> Result<()> {
        let RemoteTestSetup {
            mut remote_linked_operations,
            remote_mesh_log_id,
            mut log,
            mut remote_active_logs,
            remote_key,
            ..
        } = setup_remote_log();

        let event_remote1 = snapshot();
        let event_remote2 = update();
        let event_remote3 = update();
        let event_remote4 = update();
        let event_remote5 = snapshot();

        let remote_op1 = remote_linked_operations.next(event_remote1);
        let _remote_op2 = remote_linked_operations.next(event_remote2);
        let _remote_op3 = remote_linked_operations.next(event_remote3);
        let remote_op4 = remote_linked_operations.next(event_remote4);
        let remote_op5 = remote_linked_operations.next(event_remote5);

        remote_active_logs.insert(remote_key.public_key(), remote_mesh_log_id.clone());
        log.update_active_log(remote_mesh_log_id.clone(), None);

        log.insert(remote_op1.clone()).await?;
        log.insert(remote_op4.clone()).await?;

        assert_ready(
            &mut log,
            &remote_active_logs,
            hashmap! { remote_mesh_log_id.clone() => vec![0]},
            vec![],
        )
        .await;
        log.advance_log_pointer(&remote_mesh_log_id, 1);

        assert_not_ready(&log, &remote_active_logs).await;

        log.insert(remote_op5.clone()).await?;
        assert_ready(
            &mut log,
            &remote_active_logs,
            hashmap! { remote_mesh_log_id.clone() => vec![4]},
            vec![],
        )
        .await;
        log.advance_log_pointer(&remote_mesh_log_id, 5);

        assert_not_ready(&log, &remote_active_logs).await;

        Ok(())
    }

    #[tokio::test]
    async fn continue_with_new_log_on_restart() -> Result<()> {
        let RemoteTestSetup {
            mut remote_linked_operations,
            remote_mesh_log_id,
            remote_key,
            mut log,
            mut remote_active_logs,
            ..
        } = setup_remote_log();

        let event_remote11 = snapshot();
        let event_remote12 = update();

        let remote_op11 = remote_linked_operations.next(event_remote11);
        let remote_op12 = remote_linked_operations.next(event_remote12);

        remote_active_logs.insert(remote_key.public_key(), remote_mesh_log_id.clone());
        log.update_active_log(remote_mesh_log_id.clone(), None);

        log.insert(remote_op11.clone()).await?;
        log.insert(remote_op12.clone()).await?;

        assert_ready(
            &mut log,
            &remote_active_logs,
            hashmap! { remote_mesh_log_id.clone() => vec![0, 1]},
            vec![],
        )
        .await;
        log.advance_log_pointer(&remote_mesh_log_id, 2);

        assert_not_ready(&log, &remote_active_logs).await;

        // Restarting the peer
        let new_remote_instance_id = InstanceId::new(remote_mesh_log_id.0.zone.clone());
        let mut new_remote_linked_operations =
            LinkedOperations::new(remote_key.clone(), new_remote_instance_id.clone());
        let new_remote_mesh_log_id = MeshLogId(new_remote_instance_id);

        // new events after restart
        let event_remote21 = snapshot();
        let event_remote22 = update();
        let event_remote23 = update();
        let event_remote24 = snapshot();

        let remote_op21 = new_remote_linked_operations.next(event_remote21);
        let remote_op22 = new_remote_linked_operations.next(event_remote22);
        let remote_op23 = new_remote_linked_operations.next(event_remote23);
        let remote_op24 = new_remote_linked_operations.next(event_remote24);

        remote_active_logs.insert(remote_key.public_key(), new_remote_mesh_log_id.clone());
        log.update_active_log(new_remote_mesh_log_id.clone(), Some(remote_mesh_log_id));

        log.insert(remote_op21.clone()).await?;
        log.insert(remote_op22.clone()).await?;
        log.insert(remote_op23.clone()).await?;
        log.insert(remote_op24.clone()).await?;

        // new log started from the last snapshot (version 2)
        assert_ready(
            &mut log,
            &remote_active_logs,
            hashmap! { new_remote_mesh_log_id.clone() => vec![3]},
            vec![],
        )
        .await;
        log.advance_log_pointer(&new_remote_mesh_log_id, 4);

        assert_not_ready(&log, &remote_active_logs).await;

        Ok(())
    }

    fn snapshot() -> MeshEvent {
        MeshEvent::Snapshot {
            snapshot: BTreeMap::new(),
        }
    }

    fn update() -> MeshEvent {
        MeshEvent::Update {
            object: make_object("test", 1, "data"),
        }
    }

    async fn assert_not_ready(log: &OperationLog, active_logs: &HashMap<PublicKey, MeshLogId>) {
        assert_eq!(log.get_ready(active_logs).await, None);
    }

    async fn assert_ready(
        log: &mut OperationLog,
        active_logs: &HashMap<PublicKey, MeshLogId>,
        incoming: HashMap<MeshLogId, Vec<usize>>,
        outgoing: Vec<usize>,
    ) {
        let Some(mut ready) = log.get_ready(active_logs).await else {
            panic!("Expected ready operations");
        };

        let actual_incoming = ready.take_incoming();
        assert_eq!(
            actual_incoming.len(),
            incoming.len(),
            "Actual incoming operations do not match expected"
        );
        for (log_id, actual_incoming) in actual_incoming {
            let actual_incoming: Vec<usize> = actual_incoming
                .into_iter()
                .map(|op| op.header.seq_num as usize)
                .collect();
            assert_eq!(
                &actual_incoming,
                incoming.get(&log_id).unwrap(),
                "Incoming operations for log {log_id:?} do not match expected"
            );
        }

        let actual_outgoing = ready.take_outgoing();

        let actual_outgoing: Vec<usize> = actual_outgoing
            .into_iter()
            .map(|op| op.header.seq_num as usize)
            .collect();
        assert_eq!(actual_outgoing.len(), outgoing.len());
    }

    fn setup_local_log() -> LocalTestSetup {
        let own_key = PrivateKey::new();
        let own_instance_id = InstanceId::new("1".to_string());
        let own_linked_operations = LinkedOperations::new(own_key.clone(), own_instance_id.clone());
        let own_mesh_log_id = MeshLogId(own_instance_id);
        // let own_topic_map = Nodes::new(
        //     own_key.public_key(),
        //     own_mesh_log_id.clone(),
        //     Duration::from_secs(120),
        // );
        let active_logs = hashmap! {};
        let log = OperationLog::new(
            own_mesh_log_id.clone(),
            own_key.public_key(),
            MemoryStore::new(),
        );

        LocalTestSetup {
            own_linked_operations,
            own_mesh_log_id,
            log,
            active_logs,
        }
    }

    struct LocalTestSetup {
        pub own_linked_operations: LinkedOperations,
        pub own_mesh_log_id: MeshLogId,

        pub log: OperationLog,
        pub active_logs: HashMap<PublicKey, MeshLogId>,
    }

    fn setup_remote_log() -> RemoteTestSetup {
        let own_key = PrivateKey::new();
        let own_instance_id = InstanceId::new("1".to_string());
        let own_mesh_log_id = MeshLogId(own_instance_id);
        // let own_topic_map = Nodes::new(
        //     own_key.public_key(),
        //     own_mesh_log_id.clone(),
        //     Duration::from_secs(120),
        // );
        let active_logs = hashmap! {own_key.public_key() => own_mesh_log_id.clone() };
        let log = OperationLog::new(
            own_mesh_log_id.clone(),
            own_key.public_key(),
            MemoryStore::new(),
        );

        let remote_key = PrivateKey::new();
        let remote_instance_id = InstanceId::new("2".to_string());
        let remote_linked_operations =
            LinkedOperations::new(remote_key.clone(), remote_instance_id.clone());
        let remote_mesh_log_id = MeshLogId(remote_instance_id);

        RemoteTestSetup {
            remote_key,
            remote_linked_operations,
            remote_mesh_log_id,
            remote_active_logs: active_logs,
            log,
        }
    }

    struct RemoteTestSetup {
        pub remote_key: PrivateKey,
        pub remote_linked_operations: LinkedOperations,
        pub remote_mesh_log_id: MeshLogId,

        pub log: OperationLog,
        pub remote_active_logs: HashMap<PublicKey, MeshLogId>,
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
