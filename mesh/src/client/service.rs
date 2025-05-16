use crate::client::request::ApiRequest;
use crate::client::types::ApiServiceType;

use http::Request;
use kube::api::{ApiResource, DynamicObject};
use kube::client::Body;

use anyhow::Result;
use std::sync::Arc;
use std::task::Poll;
use std::{pin::Pin, task::Context};
use tower_service::Service;

use super::handlers::api_resource::ApiResourceHandler;
use super::handlers::resource::ResourceHandler;
use super::response::ApiResponse;
use super::router::ApiRequestRouter;
use super::storage::Storage;

pub struct FakeKubeApiService {
    storage: Arc<Storage>,
    router: Arc<ApiRequestRouter>,
}

impl FakeKubeApiService {
    pub fn new() -> Self {
        let storage = Arc::new(Storage::new());

        let router = ApiRequestRouter::new()
            .with_handler(
                ApiServiceType::ApiResources,
                Arc::new(ApiResourceHandler::new(storage.clone())),
            )
            .with_handler(
                ApiServiceType::Resource,
                Arc::new(ResourceHandler::new(storage.clone())),
            );

        FakeKubeApiService {
            storage,
            router: Arc::new(router),
        }
    }

    pub fn register(&self, ar: &ApiResource) {
        self.storage.register(ar);
    }

    pub fn store(&self, resource: DynamicObject) -> Result<()> {
        self.storage.store(resource)
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
        let router = self.router.clone();
        Box::pin(async move {
            let maybe_api_request = ApiRequest::build(req).await;
            match maybe_api_request {
                Ok(req) => {
                    let response = router.handle(req).await;
                    return response.and_then(|v| v.to_http_response());
                }
                Err(error) => ApiResponse::from(error).to_http_response(),
            }
        })
    }
}

// use tower_test::mock;
// use http::{Request, Response};
// use kube::client::Body;
// let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();
