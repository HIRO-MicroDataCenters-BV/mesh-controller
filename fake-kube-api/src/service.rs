use http::Request;
use kube::api::{ApiResource, DynamicObject};
use kube::client::Body;
use tokio::sync::Mutex;

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
use tracing::info;

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

    pub async fn store(&self, object: DynamicObject) -> Result<()> {
        self.storage.store(object).await
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
            info!("received request {req:?}");
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

#[derive(Clone)]
pub struct FakeEtcdServiceWrapper {
    service: Arc<Mutex<FakeKubeApiService>>,
}

impl FakeEtcdServiceWrapper {
    pub fn new() -> Self {
        FakeEtcdServiceWrapper {
            service: Arc::new(Mutex::new(FakeKubeApiService::new())),
        }
    }

    pub async fn register(&self, ar: &ApiResource) {
        let service = self.service.lock().await;
        service.register(ar);
    }

    pub async fn store(&self, object: DynamicObject) -> Result<()> {
        let service = self.service.lock().await;
        service.store(object).await
    }
}

impl Default for FakeEtcdServiceWrapper {
    fn default() -> Self {
        Self::new()
    }
}

impl Service<Request<Body>> for FakeEtcdServiceWrapper {
    type Response = http::Response<UnifiedBody>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let inner = self.service.clone();
        Box::pin(async move {
            let mut guard = inner.lock().await;
            guard.call(req).await
        })
    }
}
