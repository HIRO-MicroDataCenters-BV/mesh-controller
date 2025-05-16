use super::handlers::api_resource::ApiResourceArgs;
use super::handlers::resource::ResourceArgs;
use super::types::ApiServiceType;
use anyhow::Result;
use anyhow::bail;
use http::Method;
use http::Request;
use http::request::Parts;
use http::uri::PathAndQuery;
use kube::client::Body;
use regex::Regex;
use strum::Display;

#[derive(Debug, Clone, Display)]
pub enum Args {
    ApiResource(ApiResourceArgs),
    Resource(ResourceArgs),
}

#[derive(Debug, Clone)]
pub struct ApiRequest {
    pub parts: Parts,

    pub service: ApiServiceType,

    pub args: Args,
}

impl ApiRequest {
    pub async fn try_from(req: Request<Body>) -> Result<ApiRequest> {
        let (parts, body) = req.into_parts();

        let path_and_query = parts
            .uri
            .path_and_query()
            .ok_or(anyhow::anyhow!("path and query are not in request"))?;

        let (args, service) = ApiRequest::parse_uri(path_and_query, body).await?;

        Ok(ApiRequest {
            parts,
            args,
            service,
        })
    }

    async fn parse_uri(
        path_and_query: &PathAndQuery,
        body: Body,
    ) -> Result<(Args, ApiServiceType)> {
        let input = body.collect_bytes().await?;

        let path = path_and_query.path();
        let re = Regex::new(r"^/apis/(?P<group>[^/]+)/(?P<version>[^/]+)/(?P<pluralkind>[^/]+)")
            .unwrap(); // TDOO
        if let Some(captures) = re.captures(path) {
            let group = captures
                .name("group")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let version = captures
                .name("version")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let kind_plural = captures
                .name("pluralkind")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let service = ApiServiceType::Resource;
            return Ok((
                Args::Resource(ResourceArgs {
                    group,
                    version,
                    kind_plural,
                    input,
                }),
                service,
            ));
        }

        let re = Regex::new(r"^/apis/(?P<group>[^/]+)/(?P<version>[^/]+)").unwrap();
        if let Some(captures) = re.captures(path) {
            let group = captures
                .name("group")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let version = captures
                .name("version")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let service = ApiServiceType::ApiResources;
            return Ok((
                Args::ApiResource(ApiResourceArgs {
                    group,
                    version,
                    input,
                }),
                service,
            ));
        }

        bail!("unknown path {path}")
    }

    pub fn method(&self) -> &Method {
        &self.parts.method
    }
}
