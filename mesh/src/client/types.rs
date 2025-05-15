use std::fmt::Debug;
use strum_macros::{Display, EnumString};

use super::{request::ApiRequest, response::ApiResponse};

#[derive(Debug, Clone, EnumString, Display)]
pub enum ApiServiceType {
    #[strum(serialize = "ApiResources")]
    ApiResources,
    #[strum(serialize = "CustomResource")]
    CustomResource,
}

pub trait ApiHandler {
    type Fut: Future<Output = Result<ApiResponse, anyhow::Error>> + Send;

    fn call(&mut self, request: ApiRequest) -> Self::Fut {
        match request.method() {
            &http::Method::GET => self.get(request),
            &http::Method::POST => self.post(request),
            &http::Method::PUT => self.put(request),
            &http::Method::DELETE => self.delete(request),
            &http::Method::HEAD => self.head(request),
            &http::Method::OPTIONS => self.options(request),
            &http::Method::CONNECT => self.connect(request),
            &http::Method::PATCH => self.patch(request),
            &http::Method::TRACE => self.trace(request),
            _ => unimplemented!("{}", request.method()),
        }
    }

    fn get(&mut self, _request: ApiRequest) -> Self::Fut {
        unimplemented!()
    }
    fn post(&mut self, _request: ApiRequest) -> Self::Fut {
        unimplemented!()
    }
    fn put(&mut self, _request: ApiRequest) -> Self::Fut {
        unimplemented!()
    }
    fn delete(&mut self, _request: ApiRequest) -> Self::Fut {
        unimplemented!()
    }
    fn head(&mut self, _request: ApiRequest) -> Self::Fut {
        unimplemented!()
    }
    fn options(&mut self, _request: ApiRequest) -> Self::Fut {
        unimplemented!()
    }
    fn connect(&mut self, _request: ApiRequest) -> Self::Fut {
        unimplemented!()
    }
    fn patch(&mut self, _request: ApiRequest) -> Self::Fut {
        unimplemented!()
    }
    fn trace(&mut self, _request: ApiRequest) -> Self::Fut {
        unimplemented!()
    }
}
