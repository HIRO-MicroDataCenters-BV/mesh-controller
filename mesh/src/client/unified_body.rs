use anyhow::Result;
use bytes::Bytes;
use futures::task::Context;
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::StreamBody;
use std::pin::Pin;
use std::pin::pin;
use std::task::Poll;
use tracing::info;

use super::types::EventBytesStream;

pub struct UnifiedBody {
    kind: Kind,
}

impl UnifiedBody {
    fn new(kind: Kind) -> Self {
        UnifiedBody { kind }
    }

    pub fn empty() -> Self {
        Self::new(Kind::Once(None))
    }

    pub fn from_str(body: &str) -> Self {
        Self::new(Kind::Once(Some(Bytes::from(body.as_bytes().to_vec()))))
    }

    pub fn wrap_body(body: StreamBody<EventBytesStream>) -> Self {
        UnifiedBody::new(Kind::Wrap(body))
    }
}

enum Kind {
    Once(Option<Bytes>),
    Wrap(StreamBody<EventBytesStream>),
}

impl HttpBody for UnifiedBody {
    type Data = Bytes;
    type Error = anyhow::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.kind {
            Kind::Once(val) => Poll::Ready(val.take().map(|bytes| Ok(Frame::data(bytes)))),
            Kind::Wrap(body) => {
                let frame = pin!(body).poll_frame(cx);
                info!("poll frame {:?}", frame);
                frame
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.kind {
            Kind::Once(Some(bytes)) => SizeHint::with_exact(bytes.len() as u64),
            Kind::Once(None) => SizeHint::with_exact(0),
            Kind::Wrap(body) => HttpBody::size_hint(body),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.kind {
            Kind::Once(Some(bytes)) => bytes.is_empty(),
            Kind::Once(None) => true,
            Kind::Wrap(body) => body.is_end_stream(),
        }
    }
}
