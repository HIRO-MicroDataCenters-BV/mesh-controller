use http::Method;
use std::{fmt::Debug, pin::Pin};
use strum_macros::{Display, EnumString};

use super::response::ApiResponse;

pub type ApiHandlerResponse =
    Pin<Box<dyn Future<Output = Result<ApiResponse, anyhow::Error>> + Send + Sync>>;

#[derive(Debug, Clone, EnumString, Display, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ApiServiceType {
    #[strum(serialize = "ApiResources")]
    ApiResources,
    #[strum(serialize = "CustomResource")]
    Resource,
}


pub trait ApiHandler {
    type Req;

    fn call(&self, method: &Method, request: Self::Req) -> ApiHandlerResponse {
        match method {
            &http::Method::GET => self.get(request),
            &http::Method::POST => self.post(request),
            &http::Method::PUT => self.put(request),
            &http::Method::DELETE => self.delete(request),
            &http::Method::PATCH => self.patch(request),
            _ => unimplemented!("{}", method),
        }
    }

    fn get(&self, _request: Self::Req) -> ApiHandlerResponse {
        unimplemented!()
    }
    fn post(&self, _request: Self::Req) -> ApiHandlerResponse {
        unimplemented!()
    }
    fn put(&self, _request: Self::Req) -> ApiHandlerResponse {
        unimplemented!()
    }
    fn delete(&self, _request: Self::Req) -> ApiHandlerResponse {
        unimplemented!()
    }
    fn patch(&self, _request: Self::Req) -> ApiHandlerResponse {
        unimplemented!()
    }
}
