use super::types::ApiServiceType;
use anyhow::Result;
use anyhow::bail;
use bytes::Bytes;
use http::Method;
use http::Request;
use http::request::Parts;
use http::uri::PathAndQuery;
use kube::client::Body;
use regex::Regex;

#[derive(Debug, Clone)]
pub enum Args {
    ApiResource {
        group: String,
        version: String,
    },
    Resource {
        group: String,
        version: String,
        kind_plural: String,
    },
}

#[derive(Debug, Clone)]
pub struct ApiRequest {
    pub parts: Parts,

    pub input: Bytes,

    pub service: ApiServiceType,

    pub args: Args,
}

impl ApiRequest {
    pub async fn build(req: Request<Body>) -> Result<ApiRequest> {
        let (parts, body) = req.into_parts();

        let path_and_query = parts
            .uri
            .path_and_query()
            .ok_or(anyhow::anyhow!("path and query are not in request"))?;

        let (kube, service) = ApiRequest::parse_uri(path_and_query)?;

        let input = body.collect_bytes().await?;

        Ok(ApiRequest {
            parts,
            input,
            args: kube,
            service,
        })
    }

    fn parse_uri(path_and_query: &PathAndQuery) -> Result<(Args, ApiServiceType)> {
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
                Args::Resource {
                    group,
                    version,
                    kind_plural,
                },
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
            return Ok((Args::ApiResource { group, version }, service));
        }

        bail!("unknown path {path}")
    }

    pub fn method(&self) -> &Method {
        &self.parts.method
    }
}
