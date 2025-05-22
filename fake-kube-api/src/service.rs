use http::Request;
use kube::api::{ApiResource, DynamicObject};
use kube::client::Body;

use crate::request::ApiRequest;

use super::response::ApiResponse;
use super::router::ApiRequestRouter;
use super::storage::Storage;
use super::unified_body::UnifiedBody;
use anyhow::Result;
use http::StatusCode;
use std::sync::Arc;
use std::task::Poll;
use std::{pin::Pin, task::Context};
use tower_service::Service;
use tracing::trace;

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

    pub async fn store(&self, resource: DynamicObject) -> Result<()> {
        self.storage.store(resource).await
    }
}

impl Default for FakeKubeApiService {
    fn default() -> Self {
        FakeKubeApiService::new()
    }
}

impl Service<Request<Body>> for FakeKubeApiService {
    type Response = http::Response<UnifiedBody>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let router = self.router.clone();
        Box::pin(async move {
            trace!("received request {req:?}");
            match ApiRequest::try_from(req).await {
                Ok(req) => {
                    if req.is_watch {
                        let events = router.handle_watch(req).await.unwrap();
                        ApiResponse::from_stream(StatusCode::OK, events)
                    } else {
                        router.handle(req).await.and_then(|r| r.try_into())
                    }
                }
                Err(error) => ApiResponse::from(error).try_into(),
            }
        })
    }
}
