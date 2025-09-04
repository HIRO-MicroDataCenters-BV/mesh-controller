use super::response::ApiResponse;
use bytes::Bytes;
use http::Method;
use http_body::Frame;
use kube::api::DynamicObject;
use kube::api::WatchEvent;
use std::{fmt::Debug, pin::Pin};
use strum_macros::{Display, EnumString};

pub type ApiHandlerResponse =
    Pin<Box<dyn Future<Output = Result<ApiResponse, anyhow::Error>> + Send + Sync>>;

pub type ApiHandlerWatchResponse =
    Pin<Box<dyn Future<Output = Result<EventStream, anyhow::Error>> + Send>>;

pub type EventStream =
    Pin<Box<dyn futures::Stream<Item = WatchEvent<DynamicObject>> + Send + 'static>>;

pub type EventBytesStream =
    Pin<Box<dyn futures::Stream<Item = Result<Frame<Bytes>, anyhow::Error>> + Send + 'static>>;

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
        match *method {
            http::Method::GET => self.get(request),
            http::Method::DELETE => self.delete(request),
            http::Method::PATCH => self.patch(request),
            http::Method::POST => self.post(request),
            _ => unimplemented!("{}", method),
        }
    }
    fn get(&self, _request: Self::Req) -> ApiHandlerResponse {
        unimplemented!()
    }
    fn delete(&self, _request: Self::Req) -> ApiHandlerResponse {
        unimplemented!()
    }
    fn patch(&self, _request: Self::Req) -> ApiHandlerResponse {
        unimplemented!()
    }
    fn post(&self, _request: Self::Req) -> ApiHandlerResponse {
        unimplemented!()
    }

    fn watch_method(&self, method: &Method, request: Self::Req) -> ApiHandlerWatchResponse {
        match method {
            &http::Method::GET => self.watch(request),
            _ => unimplemented!("{}", method),
        }
    }

    fn watch(&self, _request: Self::Req) -> ApiHandlerWatchResponse {
        unimplemented!()
    }
}
