use crate::client::api_resources::ApiResourceHandler;
use crate::client::request::ApiRequest;
use crate::client::resource_handler::CustomResourceHandler;
use crate::client::storage::ResourceEntry;
use crate::client::types::{ApiHandler, ApiServiceType};
use crate::kube::types::NamespacedName;

use dashmap::DashMap;
use http::{Method, Request, Response};
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use kube::client::Body;

use anyhow::Result;
use anyhow::{anyhow, bail};
use std::sync::Arc;
use std::task::Poll;
use std::{error::Error, pin::Pin, task::Context};
use tower_service::Service;
use tracing::{info, trace};

use super::response::ApiResponse;
use super::storage::Storage;

pub struct FakeKubeApiService {
    storage: Arc<Storage>,
}

impl FakeKubeApiService {
    pub fn new() -> Self {
        // use tower_test::mock;
        // use http::{Request, Response};
        // use kube::client::Body;
        // let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();

        FakeKubeApiService {
            storage: Arc::new(Storage::new()),
        }
    }

    pub fn register(&self, ar: &ApiResource) {
        let gvk = GroupVersionKind::gvk(&ar.group, &ar.version, &ar.kind);
        let mut entry = self.storage.metadata.entry(gvk).or_insert_with(|| vec![]);
        entry.value_mut().push(ar.clone())
    }

    pub fn store(&self, gvk: &GroupVersionKind, resource: DynamicObject) -> Result<()> {
        if !self.storage.metadata.contains_key(gvk) {
            bail!("Resource {gvk:?} is not registred.");
        }
        let resources = self
            .storage
            .resources
            .entry(gvk.to_owned())
            .or_insert_with(|| DashMap::new());
        let name = resource
            .metadata
            .name
            .to_owned()
            .ok_or(anyhow!("name is expected"))?;
        let namespace = resource
            .metadata
            .namespace
            .to_owned()
            .ok_or(anyhow!("namespace is expected"))?;
        let ns_name = NamespacedName::new(name, namespace);
        resources.insert(
            ns_name,
            ResourceEntry {
                version: 1,
                resource,
            },
        );
        Ok(())
    }
}

impl Service<Request<Body>> for FakeKubeApiService {
    type Response = http::Response<Body>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let storage = self.storage.clone();
        Box::pin(async move {
            trace!("request {req:?}");
            let maybe_api_request = ApiRequest::build(req).await;
            match maybe_api_request {
                Ok(api_request) => match api_request.service {
                    ApiServiceType::ApiResources => {
                        let mut handler = ApiResourceHandler::new(storage);
                        let response = handler.call(api_request).await;
                        return response.and_then(|v| v.to_http_response());
                    }
                    ApiServiceType::CustomResource => {
                        let mut handler = CustomResourceHandler::new(storage);
                        let response = handler.call(api_request).await;
                        return response.and_then(|v| v.to_http_response());
                    }
                },
                Err(error) => ApiResponse::from(error).to_http_response(),
            }
        })
    }
}
