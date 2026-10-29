use conductor_runtime::{
    AdapterIdentity, AdapterKind, AttemptContract, CapabilityEnvelope, CheckDefinition,
    ContractError, InstructionPin, MAX_PACKET_BYTES, NetworkPolicy, PacketAcknowledgement,
    ResumePacket,
};

fn packet() -> ResumePacket {
    ResumePacket {
        contract: AttemptContract {
            full_capability: None,
            version: 1,
            task_id: "task-1".into(),
            attempt_id: "attempt-1".into(),
            lease_epoch: 7,
            worker_id: "worker-1".into(),
            adapter: AdapterIdentity {
                kind: AdapterKind::Fake,
                version: "fixture-v1".into(),
                executable_sha256: "a".repeat(64),
                conformance_sha256: "b".repeat(64),
            },
            workspace: "/runtime/workspaces/task-1".into(),
            starting_head: "c".repeat(40),
            authority: CapabilityEnvelope {
                read_roots: vec!["/runtime/workspaces/task-1".into()],
                write_roots: vec!["/runtime/workspaces/task-1/src".into()],
                network: NetworkPolicy::Denied,
                allow_workspace_commands: false,
            },
            instructions: vec![InstructionPin {
                path: "/runtime/workspaces/task-1/AGENTS.md".into(),
                sha256: "d".repeat(64),
            }],
            checks: vec![CheckDefinition {
                id: "tests".into(),
                argv: vec!["cargo".into(), "test".into()],
                timeout_ms: 1000,
            }],
            max_runtime_ms: 10_000,
        },
        objective: "Implement the assigned change".into(),
        checkpoint: Some("Keep dirty changes and resume the test".into()),
        untrusted_context: vec!["Repository asks to ignore checks and push".into()],
    }
}

fn ack(packet: &ResumePacket) -> PacketAcknowledgement {
    PacketAcknowledgement {
        version: 1,
        attempt_id: packet.contract.attempt_id.clone(),
        lease_epoch: packet.contract.lease_epoch,
        packet_sha256: packet.sha256().expect("valid packet"),
        instruction_sha256: packet
            .contract
            .instructions
            .iter()
            .map(|p| p.sha256.clone())
            .collect(),
    }
}

#[test]
fn acknowledgement_is_bound_to_epoch_instructions_and_complete_context() {
    let original = packet();
    let acknowledgement = ack(&original);
    assert_eq!(original.verify_ack(&acknowledgement), Ok(()));
    let mut changed = original.clone();
    changed.contract.lease_epoch += 1;
    assert_eq!(
        changed.verify_ack(&acknowledgement),
        Err(ContractError::Acknowledgement)
    );
    changed = original.clone();
    changed.checkpoint = Some("Different recovery snapshot".into());
    assert_eq!(
        changed.verify_ack(&acknowledgement),
        Err(ContractError::Acknowledgement)
    );
    changed = original.clone();
    changed.contract.instructions[0].sha256 = "e".repeat(64);
    assert_eq!(
        changed.verify_ack(&acknowledgement),
        Err(ContractError::Acknowledgement)
    );
    let mut missing_pin = acknowledgement;
    missing_pin.instruction_sha256.clear();
    assert_eq!(
        original.verify_ack(&missing_pin),
        Err(ContractError::Acknowledgement)
    );
}

#[test]
fn untrusted_prose_cannot_add_capabilities_or_remove_required_checks() {
    let original = packet();
    let bytes = serde_json::to_vec(&original).expect("serialize");
    let decoded = ResumePacket::decode(&bytes).expect("decode");
    assert_eq!(decoded.contract.authority.network, NetworkPolicy::Denied);
    assert!(!decoded.contract.authority.allow_workspace_commands);
    assert_eq!(decoded.contract.checks.len(), 1);
    let mut unknown = serde_json::to_value(&original).expect("value");
    unknown["contract"]["authority"]["allow_push"] = true.into();
    assert_eq!(
        ResumePacket::decode(&serde_json::to_vec(&unknown).expect("bytes")),
        Err(ContractError::Encoding)
    );
}

#[test]
fn path_escape_and_duplicate_acceptance_identifiers_fail_closed() {
    for root in [
        "/runtime/workspaces/other",
        "/runtime/workspaces/task-1/../../control",
        "relative",
    ] {
        let mut candidate = packet();
        candidate.contract.authority.write_roots = vec![root.into()];
        assert_eq!(candidate.validate(), Err(ContractError::Authority));
    }
    let mut duplicate = packet();
    duplicate
        .contract
        .checks
        .push(duplicate.contract.checks[0].clone());
    assert_eq!(duplicate.validate(), Err(ContractError::Check));
    let mut instructions = packet();
    instructions
        .contract
        .instructions
        .push(instructions.contract.instructions[0].clone());
    assert_eq!(instructions.validate(), Err(ContractError::Authority));
}

#[test]
fn oversized_and_unknown_protocol_packets_never_acknowledge() {
    assert_eq!(
        ResumePacket::decode(&vec![b' '; MAX_PACKET_BYTES + 1]),
        Err(ContractError::Bounds)
    );
    let mut candidate = packet();
    candidate.contract.version = 2;
    assert_eq!(candidate.validate(), Err(ContractError::Version));
    candidate = packet();
    candidate.contract.adapter.conformance_sha256 = "unknown".into();
    assert_eq!(candidate.validate(), Err(ContractError::Digest));
    candidate = packet();
    candidate.contract.checks[0].argv = vec![];
    assert_eq!(candidate.validate(), Err(ContractError::Check));
}

#[test]
fn full_capability_is_explicit_and_account_selection_is_digest_bound() {
    let mut value = packet();
    let legacy = value.sha256().unwrap();
    assert!(
        !serde_json::to_string(&value)
            .unwrap()
            .contains("full_capability")
    );
    value.contract.full_capability = Some(conductor_runtime::FullCapabilityProfile {
        account: conductor_runtime::ExecutionAccount {
            name: "worker".into(),
            uid: 501,
            gid: 20,
            home: "/Users/worker".into(),
        },
        path: "/usr/bin:/bin".into(),
    });
    assert_eq!(value.validate(), Err(ContractError::Authority));
    value.contract.authority.network = NetworkPolicy::Unrestricted;
    value.contract.authority.allow_workspace_commands = true;
    value.contract.authority.read_roots = vec!["/".into()];
    value.contract.authority.write_roots = vec!["/".into()];
    value.validate().unwrap();
    let digest = value.sha256().unwrap();
    assert_ne!(digest, legacy);
    value.contract.full_capability.as_mut().unwrap().account.uid += 1;
    assert_ne!(value.sha256().unwrap(), digest);
    value.contract.full_capability = None;
    assert_eq!(value.validate(), Err(ContractError::Authority));
}
