use crate::{
    tests::configuration::{configure_network, generate_config},
    tracing::setup_tracing,
};
use anyhow::{Context, Result};
use p2panda_core::PrivateKey;
use tracing::info;

use super::fake_mesh::FakeMeshServer;

#[test]
#[ignore] //TODO configurable fake etcd
pub fn test_state_replication() -> Result<()> {
    setup_tracing(Some("=INFO".into()));

    let TwoNodeMesh { server1, server2 } = create_two_node_mesh()?;
    info!("environment started");

    server1.discard()?;
    server2.discard()?;
    Ok(())
}

pub struct TwoNodeMesh {
    pub(crate) server1: FakeMeshServer,
    pub(crate) server2: FakeMeshServer,
}

pub fn create_two_node_mesh() -> Result<TwoNodeMesh> {
    let mut config1 = generate_config("zone1");
    let server1_private_key = PrivateKey::new();

    let mut config2 = generate_config("zone2");
    let server2_private_key = PrivateKey::new();

    configure_network(vec![
        (&mut config1, &server1_private_key),
        (&mut config2, &server2_private_key),
    ]);

    info!("server1 config {:?} ", config1.node);
    info!("server2 config {:?} ", config2.node);

    let server1 = FakeMeshServer::try_start(config1.clone(), server1_private_key.clone())
        .context("MeshServer1 start failed")?;
    let server2 = FakeMeshServer::try_start(config2.clone(), server2_private_key.clone())
        .context("MeshServer2 start failed")?;

    let mesh = TwoNodeMesh { server1, server2 };

    Ok(mesh)
}
