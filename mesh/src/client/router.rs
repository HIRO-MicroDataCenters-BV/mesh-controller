use std::sync::Arc;

use dashmap::DashMap;

use super::{
    response::ApiResponse,
    types::{ApiHandler, ApiServiceType},
};

pub struct ApiRequestRouter<F>
where
    F: Future<Output = Result<ApiResponse, anyhow::Error>> + Send,
{
    handlers: DashMap<ApiServiceType, Arc<dyn ApiHandler<Fut = Self::F>>>,
}
