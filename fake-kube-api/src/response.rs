use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use http::Response;
use http::StatusCode;
use http::header::TRANSFER_ENCODING;
use http_body::Frame;
use http_body_util::StreamBody;
use serde::Serialize;

use super::types::EventBytesStream;
use super::types::EventStream;
use super::unified_body::UnifiedBody;

pub struct ApiResponse {
    pub code: StatusCode,
    pub body: UnifiedBody,
}

impl ApiResponse {
    pub fn try_from<T>(code: StatusCode, body: T) -> Result<ApiResponse>
    where
        T: Serialize,
    {
        let body = serde_json::to_string(&body)?;
        let body = UnifiedBody::from_str(&body);
        Ok(ApiResponse { code, body })
    }

    pub fn from_stream(
        code: StatusCode,
        stream: EventStream,
    ) -> Result<http::Response<UnifiedBody>> {
        let frames: EventBytesStream = Box::pin(stream.map(|e| {
            let event_str = serde_json::to_string(&e).context("cannot serialize event to json");
            event_str.map(|e| Frame::data(Bytes::from(e)))
        }));

        let response = Response::builder()
            .status(code)
            .header(TRANSFER_ENCODING, "chunked")
            .body(UnifiedBody::wrap_body(StreamBody::new(frames)))?;

        Ok(response)
    }
}

impl TryInto<Response<UnifiedBody>> for ApiResponse {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Response<UnifiedBody>, Self::Error> {
        let response = Response::builder().status(self.code).body(self.body)?;
        Ok(response)
    }
}

impl From<anyhow::Error> for ApiResponse {
    fn from(value: anyhow::Error) -> Self {
        let body = UnifiedBody::from_str(&value.to_string());
        ApiResponse {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            body,
        }
    }
}
