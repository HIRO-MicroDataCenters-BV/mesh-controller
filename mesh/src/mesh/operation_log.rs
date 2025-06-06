use std::collections::{BTreeMap, HashMap};

use crate::mesh::{
    operations::Extensions,
    topic::{MeshLogId, MeshTopicLogMap},
};
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
    topic_log_map: MeshTopicLogMap,
    store: MemoryStore<MeshLogId, Extensions>,
    pointers: LogPointers,
}

impl OperationLog {
    pub fn new(
        own_log_id: MeshLogId,
        own_public_key: PublicKey,
        topic_log_map: MeshTopicLogMap,
        store: MemoryStore<MeshLogId, Extensions>,
    ) -> Self {
        let mut pointers = LogPointers::new();
        pointers.add(own_log_id.clone());
        Self {
            incoming_pending: HashMap::default(),
            pointers,
            store,
            topic_log_map,
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

        let is_obsolete = self.update_log_id(&header.public_key, &log_id);
        if is_obsolete {
            debug!(
                "skipping Operation({}, {}), for log {:?}",
                log_id.0.zone, header.seq_num, log_id
            );
            self.pointers.remove(&log_id);
            return Ok(IngestResult::Complete(Operation {
                hash: header.hash(),
                header,
                body: None,
            }));
        }
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

    async fn replay_pending_inserts(&mut self, key: &LogKey) -> Result<()> {
        let batch = self.get_pending_batch(key).await?;
        if batch.is_empty() {
            return Ok(());
        }
        for operation in batch {
            match self.insert_internal(operation).await? {
                IngestResult::Complete(operation) => {
                    let Some(pending) = self.incoming_pending.get_mut(&key) else {
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
        let Some(pending) = self.incoming_pending.get(&key) else {
            return Ok(vec![]);
        };

        let Some((header, _)) = self.store.latest_operation(&key.0, &key.1).await? else {
            return Ok(vec![]);
        };

        let pointer = header.seq_num + 1;

        let pending_bulk = pending
            .range(pointer..)
            .take(MAX_PENDING_BATCH_SIZE)
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>();
        Ok(pending_bulk)
    }

    fn update_log_id(&mut self, incoming_source: &PublicKey, incoming_log_id: &MeshLogId) -> bool {
        match self.topic_log_map.get_latest_log(incoming_source) {
            Some(latest) => match latest.0.start_time.cmp(&incoming_log_id.0.start_time) {
                std::cmp::Ordering::Less => {
                    debug!("new log {} found from peer", incoming_log_id);
                    self.topic_log_map
                        .update_log(incoming_source.to_owned(), incoming_log_id.to_owned());
                    self.pointers.add(incoming_log_id.to_owned());
                    false
                }
                std::cmp::Ordering::Equal => false,
                std::cmp::Ordering::Greater => true,
            },
            None => {
                debug!("new log {} found from peer", incoming_log_id);
                self.topic_log_map
                    .update_log(incoming_source.to_owned(), incoming_log_id.to_owned());
                self.pointers.add(incoming_log_id.to_owned());
                false
            }
        }
    }

    pub async fn truncate_obsolete_logs(&mut self) -> Result<()> {
        for (source, log_ids) in self.topic_log_map.take_obsolete_log_ids() {
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

    pub async fn get_ready(&self) -> Option<Ready> {
        let incoming = self.get_ready_incoming().await;
        let outgoing = self.get_ready_outgoing().await;
        if incoming.is_empty() && outgoing.is_empty() {
            None
        } else {
            Some(Ready { incoming, outgoing })
        }
    }

    async fn get_ready_incoming(&self) -> HashMap<MeshLogId, Vec<Operation<Extensions>>> {
        let peer_log_ids = self.topic_log_map.get_peer_logs();
        let mut incoming = HashMap::<MeshLogId, Vec<Operation<Extensions>>>::new();
        for (peer_id, log_id) in peer_log_ids {
            let current = self.pointers.get_current(&log_id);
            if let Some(current) = current {
                self.get_operations(&peer_id, &log_id, current)
                    .await
                    .into_iter()
                    .for_each(|ops| {
                        incoming.insert(log_id.to_owned(), ops);
                    });
            } else {
                error!("No current pointer for log {log_id:?}");
            }
        }
        incoming
    }

    async fn get_ready_outgoing(&self) -> Vec<Operation<Extensions>> {
        let own_pointer = self.pointers.get_current(&self.own_log_id).expect(&format!(
            "No current pointer for own log {}",
            self.own_log_id
        ));
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
        match self.store.get_log(&peer_id, &log_id, Some(from)).await {
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

    pub fn advance_commit_pointer(&mut self, log_id: &MeshLogId, seq_num: SeqNum) {
        if let Some(current) = self.pointers.get_current(log_id) {
            if seq_num > current {
                self.pointers.pointers.insert(log_id.clone(), seq_num);
            }
        } else {
            self.pointers.pointers.insert(log_id.clone(), seq_num);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Ready {
    incoming: HashMap<MeshLogId, Vec<Operation<Extensions>>>,
    outgoing: Vec<Operation<Extensions>>,
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

    pub fn advance_commit_pointer(&mut self, log_id: &MeshLogId, seq_num: SeqNum) {
        if let Some(current) = self.get_current(log_id) {
            if seq_num > current {
                self.pointers.insert(log_id.clone(), seq_num);
            }
        } else {
            self.pointers.insert(log_id.clone(), seq_num);
        }
    }
}
