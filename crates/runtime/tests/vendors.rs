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

#[test]
fn writer_admission_is_enforced_by_the_adapter_boundary() {
    for kind in [AdapterKind::Claude, AdapterKind::Codex] {
        let (_directory, pin, packet) = fixture(kind);
        let result = conductor_runtime::native_writer_launch(&pin, &packet, BTreeMap::new());
        if kind == AdapterKind::Claude {
            let launch = result.expect("restricted file writer");
            assert_eq!(launch.packet, packet);
            assert!(launch.arguments.iter().any(|arg| arg == "--restricted"));
        } else {
            assert!(matches!(result, Err(VendorError::Capability)));
        }
    }
}

#[test]
fn command_capable_native_writers_are_refused_before_launch_construction() {
    for kind in [AdapterKind::Claude, AdapterKind::Codex] {
        let (_directory, pin, mut packet) = fixture(kind);
        packet.contract.authority.allow_workspace_commands = true;
        assert!(matches!(
            conductor_runtime::native_writer_launch(&pin, &packet, BTreeMap::new()),
            Err(VendorError::Capability)
        ));
    }
}

#[test]
fn instruction_prompt_uses_only_verified_text_in_frozen_order() {
    use conductor_runtime::InstructionPin;
    use sha2::{Digest, Sha256};
    let (_directory, pin, mut packet) = fixture(AdapterKind::Claude);
    for name in ["outer-policy", "project-policy"] {
        let path = packet.contract.workspace.join(name);
        fs::write(&path, name).unwrap();
        packet.contract.instructions.push(InstructionPin {
            path,
            sha256: format!("{:x}", Sha256::digest(name.as_bytes())),
        });
    }
    let spec = native_vendor_command(&pin, &packet, BTreeMap::new()).unwrap();
    let prompt = spec.arguments.last().unwrap();
    assert!(prompt.contains("[\"outer-policy\",\"project-policy\"]"));
    assert!(!format!("{spec:?}").contains("outer-policy"));
    fs::write(&packet.contract.instructions[0].path, "replacement").unwrap();
    assert!(native_vendor_command(&pin, &packet, BTreeMap::new()).is_err());
}

#[test]
fn exact_external_instruction_file_does_not_grant_its_parent_directory() {
    use conductor_runtime::InstructionPin;
    use sha2::{Digest, Sha256};
    let (_root, pin, mut packet) = fixture(AdapterKind::Claude);
    let external = tempfile::tempdir().unwrap();
    let parent = external.path().canonicalize().unwrap();
    let path = parent.join("policy.md");
    fs::write(&path, "approved outer policy").unwrap();
    packet.contract.instructions.push(InstructionPin {
        path: path.clone(),
        sha256: format!("{:x}", Sha256::digest(b"approved outer policy")),
    });
    packet.contract.authority.read_roots.push(path);
    let spec = native_vendor_command(&pin, &packet, BTreeMap::new()).unwrap();
    assert!(!spec.arguments.iter().any(|arg| arg == "--add-dir"));
    assert!(
        spec.arguments
            .last()
            .unwrap()
            .contains("approved outer policy")
    );
    packet.contract.authority.read_roots.push(parent);
    assert!(matches!(
        native_vendor_command(&pin, &packet, BTreeMap::new()),
        Err(VendorError::Capability)
    ));
}

#[test]
fn encoded_instruction_expansion_is_bounded_before_launch() {
    use conductor_runtime::InstructionPin;
    use sha2::{Digest, Sha256};
    let (_root, pin, mut packet) = fixture(AdapterKind::Claude);
    let path = packet.contract.workspace.join("policy.md");
    let content = "\"".repeat(40 * 1024);
    fs::write(&path, &content).unwrap();
    packet.contract.instructions.push(InstructionPin {
        path,
        sha256: format!("{:x}", Sha256::digest(content.as_bytes())),
    });
    assert!(conductor_runtime::read_pinned_instructions(&packet).is_ok());
    assert!(matches!(
        native_vendor_command(&pin, &packet, BTreeMap::new()),
        Err(VendorError::Encoding)
    ));
}
