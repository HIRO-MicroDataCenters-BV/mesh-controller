use std::sync::Arc;

use http::StatusCode;
use kube::api::{ApiResource, DynamicObject, ListMeta, ObjectList, TypeMeta};

use crate::client::{
    request::{ApiRequest, Args},
    response::ApiResponse,
    storage::Storage,
    types::{ApiHandler, ApiHandlerResponse},
};

pub struct ResourceHandler {
    storage: Arc<Storage>,
}

impl ResourceHandler {
    pub fn new(storage: Arc<Storage>) -> ResourceHandler {
        ResourceHandler { storage }
    }
}

impl ApiHandler for ResourceHandler {
    fn get(&self, request: ApiRequest) -> ApiHandlerResponse {
        if let Args::Resource {
            group,
            version,
            kind_plural,
        } = request.args
        {
            let maybe_response = self
                .storage
                .find_objects(&group, &version, &kind_plural)
                .map(|(ar, objects)| to_object_list(&ar, objects));

            Box::pin(async {
                maybe_response
                    .map(|response| ApiResponse::try_from(StatusCode::OK, response))
                    .unwrap_or_else(|| Ok(ApiResponse::new(StatusCode::NOT_FOUND, String::new())))
            })
        } else {
            unimplemented!()
        }
    }
}

fn to_object_list(ar: &ApiResource, objects: Vec<DynamicObject>) -> ObjectList<DynamicObject> {
    let resource_version = objects
        .iter()
        .flat_map(|obj| &obj.metadata.resource_version)
        .map(|v| v.parse::<u64>().unwrap_or(0))
        .max()
        .map(|v| v.to_string())
        .unwrap_or("0".into());

    ObjectList {
        types: TypeMeta {
            api_version: ar.api_version.to_owned(),
            kind: format!("{}List", ar.kind),
        },
        metadata: ListMeta {
            resource_version: Some(resource_version),
            ..Default::default()
        },
        items: objects,
    }
}
