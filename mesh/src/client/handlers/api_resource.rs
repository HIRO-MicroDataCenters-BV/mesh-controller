use std::{pin::Pin, sync::Arc};

use http::StatusCode;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{APIResource, APIResourceList};
use kube::api::ApiResource;

use crate::client::{
    request::ApiRequest, response::ApiResponse, storage::Storage, types::ApiHandler,
};

pub struct ApiResourceHandler {
    storage: Arc<Storage>,
}

impl ApiResourceHandler {
    pub fn new(storage: Arc<Storage>) -> ApiResourceHandler {
        ApiResourceHandler { storage }
    }
}

impl ApiHandler for ApiResourceHandler {
    type Fut = Pin<Box<dyn Future<Output = Result<ApiResponse, anyhow::Error>> + Send + Sync>>;

    fn get(&mut self, request: ApiRequest) -> Self::Fut {
        let resources = self
            .storage
            .get_api_resources(&request.group, &request.version);

        Box::pin(async move {
            ApiResponse::try_from(
                StatusCode::OK,
                api_resource_list_to_response(&request.group, &request.version, &resources),
            )
        })
    }
}

fn api_resource_list_to_response(
    group: &str,
    version: &str,
    ar: &Vec<ApiResource>,
) -> APIResourceList {
    let resources = ar
        .iter()
        .flat_map(api_resource_to_response)
        .collect::<Vec<APIResource>>();
    let object = APIResourceList {
        group_version: format!("{}/{}", group, version),
        resources,
    };
    object
}

fn api_resource_to_response(ar: &ApiResource) -> Vec<APIResource> {
    vec![
        APIResource {
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
                "watch".into(),
            ],
            ..Default::default()
        },
        APIResource {
            name: format!("{}/status", ar.plural),
            singular_name: String::new(),
            kind: ar.kind.clone(),
            namespaced: true,
            verbs: vec!["get".into(), "patch".into(), "update".into()],
            ..Default::default()
        },
    ]
}
