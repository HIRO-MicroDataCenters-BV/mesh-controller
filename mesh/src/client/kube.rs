use crate::kube::types::CacheError;

use http::{Request, Response};
use kube::client::Body;
use std::task::Poll;
use std::{error::Error, pin::Pin, task::Context};
use tower_service::Service;

// use tower_test::mock;
// use http::{Request, Response};
// use kube::client::Body;
//  let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();
//  use crate::client::kube::FakeKubeApiService;

pub struct FakeKubeApiService {}

impl FakeKubeApiService {
    pub fn new() -> Self {
        FakeKubeApiService {}
    }
}

impl Service<Request<Body>> for FakeKubeApiService {
    type Response = http::Response<Body>;
    type Error = Box<dyn Error + Send + Sync>;
    type Future = ResponseFuture<Self::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        todo!()
    }
}

pub struct ResponseFuture<T> {
    response: Option<T>,
}

impl<T> Future for ResponseFuture<T> {
    type Output = Result<T, Box<dyn Error + Send + Sync>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unimplemented!()
    }
}
