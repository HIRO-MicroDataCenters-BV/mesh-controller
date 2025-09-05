use std::sync::Arc;

use super::{
    handlers::{api_resource::ApiResourceHandler, resource::ResourceHandler},
    request::{ApiRequest, Args},
    response::ApiResponse,
    storage::Storage,
    types::{ApiHandler, ApiServiceType, EventStream},
};
use anyhow::Result;

pub struct ApiRequestRouter {
    api_resources: ApiResourceHandler,
    resources: ResourceHandler,
}

impl ApiRequestRouter {
    pub fn new(storage: Arc<Storage>) -> ApiRequestRouter {
        let api_resources = ApiResourceHandler::new(storage.clone());
        let resources = ResourceHandler::new(storage.clone());
        ApiRequestRouter {
            api_resources,
            resources,
        }
    }

    pub async fn handle(&self, req: ApiRequest) -> Result<ApiResponse> {
        let method = req.method().clone();
        match (req.service, req.args) {
            (ApiServiceType::ApiResources, Args::ApiResource(arg)) => {
                self.api_resources.call(&method, arg).await
            }
            (ApiServiceType::Resource, Args::Resource(arg)) => {
                self.resources.call(&method, arg).await
            }
            (svc, arg) => unimplemented!("unknown request type {} and arg {}", svc, arg),
        }
    }

    pub async fn handle_watch(&self, req: ApiRequest) -> Result<EventStream> {
        let method = req.method().clone();
        match (req.service, req.args) {
            (ApiServiceType::Resource, Args::Resource(arg)) => {
                self.resources.watch_method(&method, arg).await
            }
            (svc, arg) => unimplemented!("unknown request type {} and arg {}", svc, arg),
        }
    }
}
