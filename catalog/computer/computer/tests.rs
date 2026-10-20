//! Computer self-tests: the declared machine, and the handle every
//! action node resolves.

use serde_json::json;

use weft::infra::{EnvEntry, Expose, InfraHandle, InfraSpec, VolumeKind};
use weft::node_test::NODE_UNDER_TEST_ID;
use weft::{FakeRig, NodeTest, WeftResult};

use super::ComputerNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("provisions_one_container_with_a_kept_home_disk", provisions),
        NodeTest::fake("packages_reach_the_container_in_one_spelling", packages),
        NodeTest::fake("no_packages_sets_no_package_variable", no_packages),
        NodeTest::fake("hands_out_its_api_endpoint_as_an_infra_handle", hands_out_its_handle),
        NodeTest::fake("ssh_keys_open_ssh_to_the_installs_network_only", ssh_keys),
        NodeTest::fake("no_ssh_keys_means_no_ssh", no_ssh),
    ]
}

fn env(spec: &InfraSpec, name: &str) -> Option<String> {
    spec.units[0].containers[0]
        .env
        .iter()
        .find_map(|EnvEntry { name: n, value }| (n == name).then(|| value.clone()))
}

fn defaults() -> serde_json::Value {
    json!({ "screen": "1280x800", "disk": "10Gi", "cpu": "1", "memory": "2Gi" })
}

async fn provisions(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run_provision_infra(&ComputerNode, defaults()).await.ok()?;
    let spec = outcome.infra_spec()?;
    assert_eq!(spec.units.len(), 1);
    let unit = &spec.units[0];
    assert_eq!(unit.machine.cpu.as_deref(), Some("1"));
    assert_eq!(unit.machine.memory.as_deref(), Some("2Gi"));
    assert_eq!(unit.machine.kind, None);
    let container = &unit.containers[0];
    assert_eq!(container.ports[0].port, 8000);
    assert_eq!(env(&spec, "WEFT_COMPUTER_PORT").as_deref(), Some("8000"), "the env port matches the container port");
    assert_eq!(env(&spec, "WEFT_COMPUTER_SCREEN").as_deref(), Some("1280x800"));
    assert_eq!(container.mounts[0].path, "/home/user");
    assert!(container.liveness.is_none(), "installing packages at start must not be cut short by a restart");
    assert_eq!(spec.volumes.len(), 1);
    assert!(
        matches!(&spec.volumes[0].kind, VolumeKind::Disk { size, .. } if size == "10Gi"),
        "the home folder is a disk, kept across stop: {:?}",
        spec.volumes[0].kind
    );
    assert!(spec.keep_on_terminate.is_empty(), "terminate deletes the home disk");
    assert_eq!(spec.endpoints[0].name, "api");
    Ok(())
}

async fn packages(rig: FakeRig) -> WeftResult<()> {
    let mut input = defaults();
    input["packages"] = json!("  libreoffice \n gimp ");
    input["machineType"] = json!("n2-standard-4");
    let outcome = rig.run_provision_infra(&ComputerNode, input).await.ok()?;
    let spec = outcome.infra_spec()?;
    assert_eq!(env(&spec, "WEFT_COMPUTER_PACKAGES").as_deref(), Some("libreoffice gimp"));
    assert_eq!(spec.units[0].machine.kind.as_deref(), Some("n2-standard-4"));
    Ok(())
}

async fn no_packages(rig: FakeRig) -> WeftResult<()> {
    let mut input = defaults();
    input["packages"] = json!("   ");
    input["machineType"] = json!(" ");
    let outcome = rig.run_provision_infra(&ComputerNode, input).await.ok()?;
    let spec = outcome.infra_spec()?;
    assert_eq!(env(&spec, "WEFT_COMPUTER_PACKAGES"), None);
    assert_eq!(spec.units[0].machine.kind, None, "a blank machine type is no machine type");
    Ok(())
}

async fn hands_out_its_handle(rig: FakeRig) -> WeftResult<()> {
    rig.declare_endpoint("api", "http://computer.example:8000");
    let outcome = rig.run(&ComputerNode, defaults()).await.ok()?;
    let handle = InfraHandle::from_value(&outcome.outputs["computer"])?;
    assert_eq!(handle, InfraHandle::new(NODE_UNDER_TEST_ID, "api", None));
    Ok(())
}

async fn ssh_keys(rig: FakeRig) -> WeftResult<()> {
    let mut input = defaults();
    input["sshKeys"] = json!("  ssh-ed25519 AAAAone me@laptop \n\n ssh-rsa AAAAtwo ci\n");
    let outcome = rig.run_provision_infra(&ComputerNode, input).await.ok()?;
    let spec = outcome.infra_spec()?;
    assert_eq!(
        env(&spec, "WEFT_COMPUTER_SSH_KEYS").as_deref(),
        Some("ssh-ed25519 AAAAone me@laptop\nssh-rsa AAAAtwo ci"),
        "one key per line, in one spelling"
    );
    assert!(spec.units[0].containers[0].ports.iter().any(|p| p.name == "ssh" && p.port == 2222));
    let ssh = spec.endpoints.iter().find(|e| e.name == "ssh").expect("an ssh endpoint");
    assert!(matches!(ssh.expose, Expose::SameNetwork), "the install's own network, never the internet");
    Ok(())
}

async fn no_ssh(rig: FakeRig) -> WeftResult<()> {
    let mut input = defaults();
    input["sshKeys"] = json!(" \n ");
    let outcome = rig.run_provision_infra(&ComputerNode, input).await.ok()?;
    let spec = outcome.infra_spec()?;
    assert_eq!(env(&spec, "WEFT_COMPUTER_SSH_KEYS"), None);
    assert!(spec.endpoints.iter().all(|e| e.name != "ssh"));
    assert_eq!(spec.units[0].containers[0].ports.len(), 1);
    Ok(())
}
