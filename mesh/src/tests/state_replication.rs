use std::time::Duration;

use crate::config::configuration::ConsoleConfig;
use crate::tests::utils::wait_for_condition;
use crate::{
    tests::{
        configuration::{configure_network, generate_config, generate_kube_config},
        fake_etcd_server::FakeEtcdServer,
    },
    tracing::setup_tracing,
};
use anyapplication::anyapplication::{
    AnyApplication, AnyApplicationSource, AnyApplicationSourceHelm, AnyApplicationSpec,
    AnyApplicationStatus, AnyApplicationStatusOwnership, AnyApplicationStatusOwnershipPlacements,
};
use anyhow::{Context, Result};
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ObjectMeta};
use meshkube::kube::dynamic_object_ext::DynamicObjectExt;
use p2panda_core::PrivateKey;
use tokio::runtime::Runtime;
use tracing::info;

use super::fake_mesh::FakeMeshServer;

#[test]
pub fn test_state_replication() -> Result<()> {
    setup_tracing(Some("=INFO".into()), ConsoleConfig::default());

    let TwoNodeMesh {
        gvk,
        server1,
        server2,
        etcd_service1,
        etcd_service2,
        test_runtime,
    } = create_two_node_mesh()?;
    info!("environment started");

    let (client1, client2) = test_runtime.block_on(async {
        let client1 = etcd_service1.get_fake_client();
        let client2 = etcd_service2.get_fake_client();
        (client1, client2)
    });

    std::thread::sleep(Duration::from_secs(2));

    let app1 = anyapplication("app1", "zone1");
    let app2 = anyapplication("app2", "zone2");
    let name1 = app1.get_namespaced_name();
    let name2 = app2.get_namespaced_name();

    test_runtime.block_on(async {
        client1.patch_apply(app1).await?;
        client2.patch_apply(app2).await
    })?;

    wait_for_condition(Duration::from_secs(10), || {
        let result = test_runtime.block_on(async { client2.get(&gvk, &name1).await })?;
        Ok(result.is_some())
    })?;

    wait_for_condition(Duration::from_secs(10), || {
        let result = test_runtime.block_on(async { client1.get(&gvk, &name2).await })?;
        Ok(result.is_some())
    })?;

    server1.discard()?;
    server2.discard()?;
    info!("exiting");

    etcd_service1.shutdown();
    etcd_service2.shutdown();
    Ok(())
}

pub struct TwoNodeMesh {
    pub(crate) gvk: GroupVersionKind,
    pub(crate) server1: FakeMeshServer,
    pub(crate) server2: FakeMeshServer,
    pub(crate) etcd_service1: FakeEtcdServer,
    pub(crate) etcd_service2: FakeEtcdServer,
    pub(crate) test_runtime: Runtime,
}

pub fn create_two_node_mesh() -> Result<TwoNodeMesh> {
    let gvk = GroupVersionKind::gvk("dcp.hiro.io", "v1", "AnyApplication");
    let ar = ApiResource::from_gvk(&gvk);

    let gvk_mesh = GroupVersionKind::gvk("dcp.hiro.io", "v1", "MeshPeer");
    let ar_mesh = ApiResource::from_gvk(&gvk_mesh);

    let gvk_event = GroupVersionKind::gvk("", "v1", "Event");
    let ar_event = ApiResource::from_gvk(&gvk_event);

    let kube_config1 = generate_kube_config("context1");
    let kube_config2 = generate_kube_config("context2");

    let etcd_service1 = FakeEtcdServer::get_server(&kube_config1);
    let etcd_service2 = FakeEtcdServer::get_server(&kube_config2);

    let test_runtime = tokio::runtime::Builder::new_current_thread().build()?;

    test_runtime.block_on(async {
        etcd_service2.service.register(&ar).await;
        etcd_service2.service.register(&ar_mesh).await;
        etcd_service2.service.register(&ar_event).await;
        etcd_service1.service.register(&ar).await;
        etcd_service1.service.register(&ar_mesh).await;
        etcd_service1.service.register(&ar_event).await;
    });

    let mut config1 = generate_config("zone1", &kube_config1, &gvk);
    let server1_private_key = PrivateKey::new();

    let mut config2 = generate_config("zone2", &kube_config2, &gvk);
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

    Ok(TwoNodeMesh {
        gvk,
        server1,
        server2,
        etcd_service1,
        etcd_service2,
        test_runtime,
    })
}

fn anyapplication(name: &str, owner_zone: &str) -> DynamicObject {
    let anyapp = AnyApplication {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("test".into()),
            labels: None,
            ..Default::default()
        },
        spec: AnyApplicationSpec {
            source: AnyApplicationSource {
                helm: Some(AnyApplicationSourceHelm {
                    chart: "chart".into(),
                    version: "1.0.0".into(),
                    namespace: "namespace".into(),
                    repository: "repo".into(),
                    values: None,
                    parameters: None,
                    release_name: None,
                    skip_crds: None,
                }),
            },
            placement_strategy: None,
            recover_strategy: None,
            sync_policy: None,
            zones: 2,
        },
        status: Some(AnyApplicationStatus {
            ownership: AnyApplicationStatusOwnership {
                epoch: 1,
                owner: owner_zone.into(),
                placements: Some(vec![
                    AnyApplicationStatusOwnershipPlacements {
                        node_affinity: None,
                        zone: "zone1".into(),
                    },
                    AnyApplicationStatusOwnershipPlacements {
                        node_affinity: None,
                        zone: "zone2".into(),
                    },
                ]),
                state: "New".into(),
            },
            zones: None,
        }),
    };
    let json_str = serde_json::to_value(&anyapp).expect("Resource is not serializable");
    let object: DynamicObject =
        serde_json::from_value(json_str).expect("Cannot parse dynamic object");
    object
}
