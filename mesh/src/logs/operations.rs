use std::pin::Pin;
use std::task::Context;

use crate::kube::types::CacheProtocol;
use futures::Stream;
use futures::StreamExt;
use futures::ready;
use loole::RecvStream;
use p2panda_core::Body;
use p2panda_core::cbor::DecodeError;
use p2panda_core::cbor::decode_cbor;
use p2panda_core::{Hash, Header, Operation, PrivateKey, PruneFlag};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::task::Poll;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::info;

pub trait OperationExt<S>: Stream<Item = CacheProtocol> {
    fn to_operation(self, key: PrivateKey) -> OperationStream<Self>
    where
        S: Stream<Item = CacheProtocol>,
        Self: Sized,
    {
        OperationStream::new(self, key)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extensions {
    pub prune_flag: PruneFlag,
}

#[derive(Debug, Clone)]
pub struct KubeOperation {
    pub panda_op: Operation<Extensions>,
    pub backlink: Option<Hash>,
}

impl std::fmt::Display for KubeOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KubeOperation(hash: {}, backlink: {:?})",
            self.panda_op.hash, self.backlink
        )
    }
}

#[pin_project]
pub struct OperationStream<S>
where
    S: Stream<Item = CacheProtocol>,
{
    #[pin]
    kube_events: S,
    key: PrivateKey,
    backlink: Option<Hash>,
    seq_num: u64,
}

impl<S> OperationStream<S>
where
    S: Stream<Item = CacheProtocol>,
{
    pub fn new(kube_events: S, key: PrivateKey) -> OperationStream<S> {
        OperationStream {
            key,
            kube_events,
            backlink: None,
            seq_num: 0,
        }
    }
}

impl<S> Stream for OperationStream<S>
where
    S: Stream<Item = CacheProtocol>,
{
    type Item = KubeOperation;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();
        match ready!(this.kube_events.poll_next(cx)) {
            Some(event) => {
                info!("OperationStream: received event {}", event);
                let operation = map_event(&event, &this.key, this.backlink, *this.seq_num);
                *this.seq_num += 1;
                *this.backlink = Some(operation.panda_op.hash);
                Poll::Ready(Some(operation))
            }
            None => Poll::Ready(None),
        }
    }
}

fn map_event(
    event: &CacheProtocol,
    key: &PrivateKey,
    backlink: &Option<Hash>,
    seq_num: u64,
) -> KubeOperation {
    let prune_flag = matches!(event, CacheProtocol::Snapshot { .. });
    let (hash, header, body) = create_operation(key, &event, seq_num, 0, *backlink, prune_flag);
    let panda_op = Operation {
        hash,
        header,
        body: Some(body),
    };
    let backlink = Some(hash);
    KubeOperation { panda_op, backlink }
}

fn create_operation(
    private_key: &PrivateKey,
    event: &CacheProtocol,
    seq_num: u64,
    timestamp: u64,
    backlink: Option<Hash>,
    prune_flag: bool,
) -> (Hash, Header<Extensions>, Body) {
    let body = Body::new(&event.to_bytes());
    let mut header = Header {
        version: 1,
        public_key: private_key.public_key(),
        signature: None,
        payload_size: body.size(),
        payload_hash: Some(body.hash()),
        timestamp,
        seq_num,
        backlink,
        previous: vec![],
        extensions: Some(Extensions {
            prune_flag: PruneFlag::new(prune_flag),
        }),
    };
    header.sign(private_key);
    (header.hash(), header, body)
}

impl OperationExt<RecvStream<CacheProtocol>> for RecvStream<CacheProtocol> {}

pub type OpStream = dyn Stream<Item = KubeOperation> + Send + Sync + 'static;
pub type BoxedOperationStream = Pin<Box<OpStream>>;

impl TryFrom<&[u8]> for Extensions {
    type Error = DecodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        decode_cbor(value)
    }
}

pub fn fanout2<T, S>(
    stream: S,
    capacity: usize,
) -> (
    impl Stream<Item = T> + Send + Sync + 'static,
    impl Stream<Item = T> + Send + Sync + 'static,
)
where
    T: Clone + Send + Sync + 'static,
    S: Stream<Item = T> + Send + Sync + 'static,
{
    let (tx, _rx) = broadcast::channel(capacity);
    let mut stream = stream.boxed();
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            let _ = tx_clone.send(item);
        }
    });

    let left = BroadcastStream::new(tx.subscribe()).filter_map(|value| async {
        match value {
            Ok(v) => Some(v),
            Err(_e) => None,
        }
    });

    let right = BroadcastStream::new(tx.subscribe()).filter_map(|value| async {
        match value {
            Ok(v) => Some(v),
            Err(_e) => None,
        }
    });

    (left, right)
}
