use crate::JoinErrToStr;
use crate::kube::cache::KubeCache;
use crate::kube::types::CacheProtocol;
use anyhow::Result;
use futures::future::{MapErr, Shared};
use futures::{FutureExt, StreamExt, TryFutureExt};
use p2panda_core::Body;
use p2panda_core::Operation;
use p2panda_core::{Header, PublicKey};
use p2panda_store::MemoryStore;
use p2panda_stream::operation::{IngestResult, ingest_operation};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug};
use tokio::task::JoinError;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, info, trace};

use super::operations::{BoxedOperationStream, Extensions, KubeOperation};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy, Serialize, Deserialize)]
pub struct MeshLogId();

pub type Logs<T> = HashMap<PublicKey, Vec<T>>;

pub struct KubeApi {
    store: MemoryStore<MeshLogId, Extensions>,
    cache: KubeCache,
    #[allow(dead_code)]
    handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl KubeApi {
    pub fn new(
        store: MemoryStore<MeshLogId, Extensions>,
        cache: KubeCache,
        mut local_operations_stream: BoxedOperationStream,
    ) -> KubeApi {
        let mut the_store = store.clone();

        let handle = tokio::spawn(async move {
            let log_id = MeshLogId();
            loop {
                match local_operations_stream.next().await {
                    Some(operation) => {
                        let KubeOperation {
                            panda_op: Operation { header, body, .. },
                            ..
                        } = operation;

                        let header_bytes = header.to_bytes();
                        let prune_flag = header
                            .extensions
                            .as_ref()
                            .map(|e| e.prune_flag.is_set())
                            .unwrap_or(false);

                        match ingest_operation(
                            &mut the_store,
                            header,
                            body,
                            header_bytes,
                            &log_id,
                            prune_flag,
                        )
                        .await
                        {
                            Err(error) => {
                                error!("Error during ingest operation {}", error);
                                break;
                            }
                            Ok(result) => match result {
                                IngestResult::Complete(op) => {
                                    info!("Local ingested operation: {:?}", op);
                                }
                                IngestResult::Retry(_, _, _, ops_missing) => {
                                    info!(
                                        "Local retrying operation: missing ops = {:?}",
                                        ops_missing
                                    );
                                }
                            },
                        }
                    }
                    None => break,
                }
            }
        });

        let handle = AbortOnDropHandle::new(handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        KubeApi {
            store,
            cache,
            handle,
        }
    }

    pub async fn incoming(
        &mut self,
        header: Header<Extensions>,
        body: Option<Body>,
        header_bytes: Vec<u8>,
        log_id: &MeshLogId,
    ) -> Result<()> {
        trace!(
            "KubeApi ingest operation {}, seq_num {}",
            header.hash(),
            header.seq_num
        );
        let result = ingest_operation(&mut self.store, header, body, header_bytes, log_id, false)
            .await
            .inspect_err(|e| {
                error!("Error during ingest operation {}", e);
            })?;
        match result {
            IngestResult::Complete(op) => {
                info!("Incoming operation ingest completed: {:?}", op);
                if let Some(body) = op.body {
                    let incoming_event = CacheProtocol::try_from(body.to_bytes())?;
                    self.cache.merge(incoming_event).await?;
                }
            }
            IngestResult::Retry(_, _, _, ops_missing) => {
                info!("Incoming operation retry: missing ops = {:?}", ops_missing);
            }
        }
        Ok(())
    }
}
