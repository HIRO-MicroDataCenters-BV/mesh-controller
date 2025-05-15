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
pub struct ApiRequest {
    pub parts: Parts,

    pub input: Bytes,

    pub group: String,

    pub version: String,

    pub kind: Option<String>,

    pub service: ApiServiceType,
}

impl ApiRequest {
    pub async fn build(req: Request<Body>) -> Result<ApiRequest> {
        let (parts, body) = req.into_parts();

        let path_and_query = parts
            .uri
            .path_and_query()
            .ok_or(anyhow::anyhow!("path and query are not in request"))?;

        let (group, version, kind, service) = ApiRequest::parse_uri(path_and_query)?;
        println!("{group}, {version}, {kind:?}, {service}");
        let input = body.collect_bytes().await?;

        Ok(ApiRequest {
            parts,
            input,
            group,
            version,
            kind,
            service,
        })
    }

    fn parse_uri(
        path_and_query: &PathAndQuery,
    ) -> Result<(String, String, Option<String>, ApiServiceType)> {
        let path = path_and_query.path();
        let re = Regex::new(r"^/apis/(?P<group>[^/]+)/(?P<version>[^/]+)/(?P<pluralkind>[^/]+)")
            .unwrap();
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
            let kind = captures
                .name("pluralkind")
                .map(|m| m.as_str())
                .unwrap_or("")
                .into();
            let kind = Some(kind);
            let service = ApiServiceType::CustomResource;
            return Ok((group, version, kind, service));
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
            let kind = None;
            let service = ApiServiceType::ApiResources;
            return Ok((group, version, kind, service));
        }

        bail!("unknown path {path}")
    }

    pub fn method(&self) -> &Method {
        &self.parts.method
    }
}
