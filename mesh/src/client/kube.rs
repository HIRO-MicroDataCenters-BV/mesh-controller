use crate::kube::types::{CacheError, NamespacedName};

use dashmap::{DashMap, DashSet};
use http::{Method, Request, Response};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResourceList;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use kube::client::Body;
use pin_project::pin_project;
use reqwest::get;
use std::task::Poll;
use tracing::info;
// use kube::core::discovery::{APIResource, APIResourceList};
use std::{error::Error, pin::Pin, task::Context};
use tower_service::Service;

struct ResourceEntry {
    version: u64,
    resource: DynamicObject,
}

struct InternalState {
    metadata: DashMap<GroupVersionKind, ApiResource>,
    resources: DashMap<GroupVersionKind, DashMap<NamespacedName, ResourceEntry>>,
}

impl InternalState {
    pub fn new() -> InternalState {
        InternalState {
            metadata: DashMap::new(),
            resources: DashMap::new(),
        }
    }
}

pub struct FakeKubeApiService {
    state: InternalState,
}

impl FakeKubeApiService {
    pub fn new() -> Self {
        // use tower_test::mock;
        // use http::{Request, Response};
        // use kube::client::Body;
        // let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();

        FakeKubeApiService {
            state: InternalState::new(),
        }
    }

    pub fn register(&self, ar: &ApiResource) {

    }

    pub fn add_resource(&self, gvk: &GroupVersionKind, resource: DynamicObject) {}

    fn get(&self, req: Request<Body>) -> http::Response<Body> {
        let path_and_query = req.uri().path_and_query();
        info!("path_and_query {:?}", path_and_query);
        let response = Response::builder()
            .status(200)
            // .header("X-Custom-Foo", "Bar")
            .body("{}".as_bytes().to_vec().into())
            .unwrap();
        response
    }

    fn api_resource_list_response(&self, ar: &ApiResource) -> APIResourceList {
        let object = APIResourceList {
            group_version: format!("{}/{}", ar.group, ar.version),
            resources: vec![
                k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResource {
                    name: ar.plural.clone(),
                    singular_name: ar.kind.to_lowercase(),
                    kind: ar.kind.clone(),
                    namespaced: true,
                    verbs: vec![
                        "delete".into(),
                        "deletecollection".into(),
                        "get".into(),
                        "list".into(),
                        "patch".into(),
                        "create".into(),
                        "update".into(),
                        "watch".into()
                    ],
                    .. Default::default()
                },
                k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResource {
                    name: format!("{}/status", ar.plural),
                    singular_name: String::new(),
                    kind: ar.kind.clone(),
                    namespaced: true,
                    verbs: vec![
                        "get".into(),
                        "patch".into(),
                        "update".into(),
                    ],
                    .. Default::default()
                },
            ],
        };
        // let value = format!(
        //     r#"{
        //         "kind": "APIResourceList",
        //         "apiVersion": "${version}",
        //         "groupVersion": "dcp".hiro.io/v1",
        //         "resources": [
        //             {
        //                 "name": "anyapplications",
        //                 "singularName": "anyapplication",
        //                 "namespaced": true,
        //                 "kind": "AnyApplication",
        //                 "verbs":
        //                 [
        //                     "delete",
        //                     "deletecollection",
        //                     "get",
        //                     "list",
        //                     "patch",
        //                     "create",
        //                     "update",
        //                     "watch"
        //                 ],
        //                 "storageVersionHash": "/flahFN8BJU="
        //             },
        //             {
        //                 "name": "anyapplications/status",
        //                 "singularName": "",
        //                 "namespaced": true,
        //                 "kind": "AnyApplication",
        //                 "verbs":
        //                 [
        //                     "get",
        //                     "patch",
        //                     "update"
        //                 ]
        //             }
        //         ]
        //     }"#,
        //     ar.group, ar.version, ar.kind
        // );
        object
    }
}

impl Service<Request<Body>> for FakeKubeApiService {
    type Response = http::Response<Body>;
    type Error = Box<dyn Error + Send + Sync>;
    type Future = ResponseFuture<Self::Response>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        info!("req {:?}", req);
        if req.method() == Method::GET {
            let response = self.get(req);
            return ResponseFuture::new(Ok(response));
        } else {
            // ResponseFuture::<http::Response<Body>>::new()
            unimplemented!()
        }
    }
}

pub struct ResponseFuture<T> {
    response: Option<Result<T, Box<dyn Error + Send + Sync>>>,
}

impl<T> ResponseFuture<T> {
    pub fn new(response: Result<T, Box<dyn Error + Send + Sync>>) -> ResponseFuture<T> {
        ResponseFuture {
            response: Some(response),
        }
    }
}
impl<T> Unpin for ResponseFuture<T> {}

impl<T> Future for ResponseFuture<T> {
    type Output = Result<T, Box<dyn Error + Send + Sync>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(
            self.response
                .take()
                .expect("`ResponseFuture` polled after completion"),
        )
    }
}
