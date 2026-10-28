#![cfg(unix)]
use conductor_runtime::{
    AdapterKind, ResumePacket, VendorError, VendorPin, binary_sha256, native_vendor_command,
};
use std::{collections::BTreeMap, fs};

fn fixture(kind: AdapterKind) -> (tempfile::TempDir, VendorPin, ResumePacket) {
    let directory = tempfile::tempdir().expect("directory");
    let root = directory.path().canonicalize().expect("root");
    let executable = root.join("fixture-binary");
    fs::write(&executable, b"synthetic binary identity; never executed").expect("fixture");
    let digest = binary_sha256(&executable).expect("hash");
    let pin = VendorPin {
        kind,
        version: "fixture-version".into(),
        executable,
        executable_sha256: digest.clone(),
        conformance_sha256: "b".repeat(64),
    };
    let packet = serde_json::from_value(serde_json::json!({
        "contract": {"version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"worker_id":"worker",
            "adapter":{"kind":kind,"version":"fixture-version","executable_sha256":digest,"conformance_sha256":"b".repeat(64)},
            "workspace":root,"starting_head":"c".repeat(40),"authority":{"read_roots":[root],"write_roots":[root],"network":"inference_only","allow_workspace_commands":kind == AdapterKind::Codex},
            "instructions":[],"checks":[],"max_runtime_ms":10000},
        "objective":"synthetic task","checkpoint":null,"untrusted_context":[]
    })).expect("packet");
    (directory, pin, packet)
}

#[test]
fn changed_executable_or_conformance_receipt_cannot_launch() {
    let (_directory, pin, packet) = fixture(AdapterKind::Claude);
    native_vendor_command(&pin, &packet, BTreeMap::new()).expect("accepted identity");
    let mut changed = packet.clone();
    changed.contract.adapter.conformance_sha256 = "d".repeat(64);
    assert!(matches!(
        native_vendor_command(&pin, &changed, BTreeMap::new()),
        Err(VendorError::Identity)
    ));
    fs::write(&pin.executable, b"replaced binary").expect("replace fixture");
    assert!(matches!(
        native_vendor_command(&pin, &packet, BTreeMap::new()),
        Err(VendorError::Identity)
    ));
}

#[test]
fn wider_roots_and_unsupported_command_authority_fail_closed() {
    for kind in [AdapterKind::Claude, AdapterKind::Codex] {
        let (_directory, pin, packet) = fixture(kind);
        let mut changed = packet.clone();
        changed.contract.authority.read_roots.push("/".into());
        assert!(matches!(
            native_vendor_command(&pin, &changed, BTreeMap::new()),
            Err(VendorError::Capability)
        ));
        let mut changed = packet.clone();
        changed.contract.authority.allow_workspace_commands =
            !packet.contract.authority.allow_workspace_commands;
        assert!(matches!(
            native_vendor_command(&pin, &changed, BTreeMap::new()),
            Err(VendorError::Capability)
        ));
    }
}

#[test]
fn environment_injection_is_refused_and_debug_never_contains_auth_values_or_prompt() {
    let (_directory, pin, mut packet) = fixture(AdapterKind::Claude);
    assert!(matches!(
        native_vendor_command(
            &pin,
            &packet,
            BTreeMap::from([("BASH_ENV".into(), "/outside/script".into())])
        ),
        Err(VendorError::Environment)
    ));
    packet.objective = "private-synthetic-prompt-7d72".into();
    let spec = native_vendor_command(
        &pin,
        &packet,
        BTreeMap::from([(
            "ANTHROPIC_API_KEY".into(),
            "synthetic-auth-sentinel-83c2".into(),
        )]),
    )
    .expect("spec");
    let debug = format!("{spec:?}");
    assert!(!debug.contains("synthetic-auth-sentinel-83c2"));
    assert!(!debug.contains("private-synthetic-prompt-7d72"));
    assert_eq!(
        spec.environment
            .get("ANTHROPIC_API_KEY")
            .expect("memory-only auth"),
        "synthetic-auth-sentinel-83c2"
    );
}
