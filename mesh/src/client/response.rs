use anyhow::Result;
use http::Response;
use http::StatusCode;
use kube::client::Body;
use serde::Serialize;

pub struct ApiResponse {
    pub code: StatusCode,
    pub body: String,
}

impl ApiResponse {
    pub fn new(code: StatusCode, body: String) -> ApiResponse {
        ApiResponse { code, body }
    }

    pub fn try_from<T>(code: StatusCode, body: T) -> Result<ApiResponse>
    where
        T: Serialize,
    {
        let body = serde_json::to_string(&body)?;
        Ok(ApiResponse { code, body })
    }

    pub fn to_http_response(self) -> Result<Response<Body>> {
        let response = Response::builder()
            .status(self.code)
            .body(self.body.as_bytes().to_vec().into())?;
        Ok(response)
    }
}

impl From<anyhow::Error> for ApiResponse {
    fn from(value: anyhow::Error) -> Self {
        ApiResponse {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            body: value.to_string(),
        }
    }
}
