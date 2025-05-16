use crate::client::request::ApiRequest;

use http::Request;
use kube::api::{ApiResource, DynamicObject};
use kube::client::Body;

use anyhow::Result;
use std::sync::Arc;
use std::task::Poll;
use std::{pin::Pin, task::Context};
use tower_service::Service;

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
        let router = ApiRequestRouter::new(storage.clone());

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
            match ApiRequest::try_from(req).await {
                Ok(req) => router.handle(req).await.and_then(|v| v.to_http_response()),
                Err(error) => ApiResponse::from(error).to_http_response(),
            }
        })
    }
}
