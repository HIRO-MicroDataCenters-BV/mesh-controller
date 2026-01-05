use anyhow::Result;
use kube::core::CustomResourceExt;
use meshresource::meshpeer::MeshPeer;

pub fn main() -> Result<()> {
    let crd = MeshPeer::crd();
    let yaml = serde_yaml_bw::to_string(&crd).unwrap();
    println!("{}", yaml);
    Ok(())
}
