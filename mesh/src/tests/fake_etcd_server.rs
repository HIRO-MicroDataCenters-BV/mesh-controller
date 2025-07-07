use dashmap::DashMap;
use fake_kube_api::service::FakeEtcdServiceWrapper;
use once_cell::sync::Lazy;

use crate::{client::kube_client::KubeClient, config::configuration::KubeConfiguration};

pub static TEST_FAKE_SERVER: Lazy<DashMap<KubeConfiguration, FakeEtcdServer>> =
    Lazy::new(DashMap::new);

#[derive(Clone)]
pub struct FakeEtcdServer {
    config: KubeConfiguration,
    pub service: FakeEtcdServiceWrapper,
}

impl FakeEtcdServer {
    pub fn new(config: KubeConfiguration) -> Self {
        let service = FakeEtcdServiceWrapper::new();
        FakeEtcdServer { service, config }
    }

    pub fn get_server(config: &KubeConfiguration) -> FakeEtcdServer {
        TEST_FAKE_SERVER
            .entry(config.to_owned())
            .or_insert_with(|| FakeEtcdServer::new(config.to_owned()))
            .value()
            .clone()
    }

    pub fn get_client(config: &KubeConfiguration) -> KubeClient {
        let server = FakeEtcdServer::get_server(config);
        KubeClient::build_fake(server.service)
    }

    pub fn get_fake_client(&self) -> KubeClient {
        let server = FakeEtcdServer::get_server(&self.config);
        KubeClient::build_fake(server.service)
    }

    pub fn shutdown(self) {
        TEST_FAKE_SERVER.remove(&self.config);
    }
}
