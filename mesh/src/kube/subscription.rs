use super::dynamic_object_ext::DynamicObjectExt;
use super::event::KubeEvent;
use super::types::NamespacedName;
use crate::JoinErrToStr;
use crate::client::kube_client::KubeClient;
use crate::kube::subscriptions::Version;
use anyhow::Result;
use anyhow::bail;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::future::MapErr;
use futures::future::Shared;
use kube::api::GroupVersionKind;
use kube::{
    api::{DynamicObject, ResourceExt},
    runtime::watcher::Event,
};
use std::time::Duration;
use std::{collections::BTreeMap, sync::Arc};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, info};

const STREAM_RECONNECT_DELAY_MS: u64 = 1000;

pub struct Subscription {
    inner: Arc<SubscriptionInner>,
}

impl Subscription {
    pub fn new(
        client: KubeClient,
        gvk: GroupVersionKind,
        tx: loole::Sender<KubeEvent>,
        cancelation: CancellationToken,
        namespace: Option<String>,
    ) -> Subscription {
        Subscription {
            inner: Arc::new(SubscriptionInner {
                client,
                gvk,
                tx,
                cancelation,
                namespace,
            }),
        }
    }
    pub fn run(&self) -> Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>> {
        let inner = self.inner.clone();

        let handle = tokio::spawn(async move {
            while !inner.cancelation.is_cancelled() {
                if let Err(error) = inner.run_inner().await {
                    tokio::time::sleep(Duration::from_millis(STREAM_RECONNECT_DELAY_MS)).await;
                    error!("Retrying subscription: {error}");
                } else {
                    info!("Stream stopped.");
                    break;
                }
            }
        });

        AbortOnDropHandle::new(handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared()
    }
}
pub struct SubscriptionInner {
    client: KubeClient,
    gvk: GroupVersionKind,
    namespace: Option<String>,
    tx: loole::Sender<KubeEvent>,
    cancelation: CancellationToken,
}

#[derive(Debug, Clone)]
enum StreamState {
    New,
    SnapshotCollection {
        snapshot: BTreeMap<NamespacedName, DynamicObject>,
        max_version: Option<Version>,
    },
    Streaming,
}

impl StreamState {
    pub fn new() -> Self {
        StreamState::New
    }

    fn next_state(self, event: Event<DynamicObject>) -> Result<(StreamState, Option<KubeEvent>)> {
        match (self, event) {
            (StreamState::New | StreamState::Streaming, Event::Init) => Ok((
                StreamState::SnapshotCollection {
                    snapshot: BTreeMap::new(),
                    max_version: None,
                },
                None,
            )),
            (
                StreamState::SnapshotCollection {
                    mut snapshot,
                    max_version,
                },
                Event::InitApply(object),
            ) => {
                let name = object.get_namespaced_name();
                let version = object
                    .resource_version()
                    .expect("resourceVersion is not set for dynamic object")
                    .parse::<Version>()
                    .expect("resourceVersion in dynamic object should be u64");
                let max_version = max_version
                    .map(|current| if version > current { version } else { current })
                    .or(Some(version));

                snapshot.insert(name, object);

                Ok((
                    StreamState::SnapshotCollection {
                        snapshot,
                        max_version,
                    },
                    None,
                ))
            }
            (
                StreamState::SnapshotCollection {
                    snapshot,
                    max_version,
                },
                Event::InitDone,
            ) => {
                let version = max_version.unwrap_or(0);
                Ok((
                    StreamState::Streaming,
                    Some(KubeEvent::Snapshot { version, snapshot }),
                ))
            }

            (StreamState::Streaming, Event::Apply(object)) => {
                let version = object
                    .resource_version()
                    .expect("resourceVersion is not set for dynamic object")
                    .parse::<Version>()
                    .expect("resourceVersion in dynamic object should be u64");
                Ok((
                    StreamState::Streaming,
                    Some(KubeEvent::Update { version, object }),
                ))
            }
            (StreamState::Streaming, Event::Delete(object)) => {
                let version = object
                    .resource_version()
                    .expect("resourceVersion is not set for dynamic object")
                    .parse::<Version>()
                    .expect("resourceVersion in dynamic object should be u64");
                Ok((
                    StreamState::Streaming,
                    Some(KubeEvent::Delete { version, object }),
                ))
            }
            (st, event) => bail!("Unexpected event {:?} in state {:?}", event, st),
        }
    }
}

impl SubscriptionInner {
    async fn run_inner(&self) -> Result<()> {
        let event_stream = self
            .client
            .event_stream_for(&self.gvk, &self.namespace)
            .await?;
        let mut events = event_stream.boxed();
        let mut stream_state = StreamState::new();
        loop {
            tokio::select! {
                _ = self.cancelation.cancelled() => break,
                event = events.next() => {
                    match event {
                        Some(Ok(event)) => {
                            let (new_state, event) = stream_state.next_state(event)?;
                            if let Some(event) = event {
                                self.tx.send(event).inspect_err(|e| {
                                    error!("Failed to send event: {}", e);
                                }).ok();
                            }
                            stream_state = new_state;
                        },
                        Some(Err(e)) => bail!("Error in event stream {}", e),
                        None => break,
                    }
                }
            }
        }
        Ok(())
    }
}
