use std::{collections::HashMap, sync::Arc};

use super::{
    request::ApiRequest,
    response::ApiResponse,
    types::{ApiHandler, ApiServiceType},
};
use anyhow::Result;

pub struct ApiRequestRouter {
    handlers: HashMap<ApiServiceType, Arc<dyn ApiHandler + Send + Sync + 'static>>,
}

impl ApiRequestRouter {
    pub fn new() -> ApiRequestRouter {
        ApiRequestRouter {
            handlers: HashMap::new(),
        }
    }

    pub fn with_handler(
        mut self,
        handler_type: ApiServiceType,
        handler: Arc<dyn ApiHandler + Send + Sync + 'static>,
    ) -> Self {
        self.handlers.insert(handler_type, handler);
        self
    }

    pub async fn handle(&self, req: ApiRequest) -> Result<ApiResponse> {
        if let Some(handler) = self.handlers.get(&req.service) {
            handler.call(req).await
        } else {
            unimplemented!("unknown request type {}", req.service)
        }
    }
}
