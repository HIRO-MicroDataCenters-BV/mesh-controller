use anyhow::Result;
use p2panda_core::Body;
use p2panda_core::{Header, PublicKey};
use p2panda_store::MemoryStore;
use p2panda_stream::operation::ingest_operation;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Serialize, Deserialize)]
pub struct MeshLogId();

pub type Logs<T> = HashMap<PublicKey, Vec<T>>;

#[derive(Clone, Default)]
pub struct KubeApi {
    store: MemoryStore<MeshLogId>,
}

impl KubeApi {
    pub fn new(store: MemoryStore<MeshLogId>) -> KubeApi {
        KubeApi { store }
    }

    pub async fn ingest(
        &mut self,
        header: Header<()>,
        body: Option<Body>,
        header_bytes: Vec<u8>,
        log_id: &MeshLogId,
    ) -> Result<()> {
        ingest_operation(&mut self.store, header, body, header_bytes, log_id, false).await?;
        Ok(())
    }
}
