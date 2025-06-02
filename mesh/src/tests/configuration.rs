use once_cell::sync::Lazy;
use p2panda_core::PrivateKey;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU16, Ordering},
};

use crate::config::configuration::{
    Config, KnownNode, KubeConfiguration, MergeStrategyType, MeshConfig, PeriodicSnapshotConfig,
    ResourceConfig,
};

static TEST_INSTANCE_HTTP_PORT: Lazy<AtomicU16> = Lazy::new(|| AtomicU16::new(8080));
static TEST_INSTANCE_MESH_PORT: Lazy<AtomicU16> = Lazy::new(|| AtomicU16::new(31000));

pub fn generate_config(zone: &str) -> Config {
    let http_port = TEST_INSTANCE_HTTP_PORT.fetch_add(1, Ordering::SeqCst);
    let mesh_port = TEST_INSTANCE_MESH_PORT.fetch_add(1, Ordering::SeqCst);

    let mut config = Config::default();
    config.node.bind_port = mesh_port;
    config.node.http_bind_port = http_port;
    config.node.known_nodes = vec![];
    config.node.private_key_path = PathBuf::from("/tmp/private_key");
    config.node.network_id = "test".to_string();
    config.kubernetes = Some(KubeConfiguration::InCluster);
    config.mesh = MeshConfig {
        zone: zone.into(),
        bootstrap: true,
        snapshot: PeriodicSnapshotConfig {
            snapshot_interval_seconds: 10,
            snapshot_max_log: 10,
        },
        resource: ResourceConfig {
            group: "dcp.hiro.io".into(),
            version: "v1".into(),
            kind: "AnyApplication".into(),
            namespace: Some("test".into()),
            merge_strategy: MergeStrategyType::Default,
        },
        // resource: ResourceConfig {
        //     group: "".into(),
        //     version: "v1".into(),
        //     kind: "Secret".into(),
        //     namespace: Some("test".into()),
        //     merge_strategy: MergeStrategyType::Default,
        // },
    };
    config.log_level = Some("=INFO".to_string());
    config
}

pub fn configure_network(nodes: Vec<(&mut Config, &PrivateKey)>) {
    let mut nodes = nodes;
    for i in 0..nodes.len() {
        let mut known_nodes = vec![];
        for j in 0..nodes.len() {
            if i != j {
                let (node_config, private_key) = &nodes[j];
                known_nodes.push(KnownNode {
                    public_key: private_key.public_key(),
                    direct_addresses: vec![format!("127.0.0.1:{}", node_config.node.bind_port)],
                });
            }
        }
        nodes[i].0.node.known_nodes = known_nodes;
    }
}
