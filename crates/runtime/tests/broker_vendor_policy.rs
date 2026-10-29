#![cfg(unix)]
use conductor_runtime::{binary_sha256, codex_broker_tool_options};
use std::{fs, os::unix::fs::symlink};

#[test]
fn broker_policy_requires_exact_canonical_proxy_identity() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let proxy = root.join("proxy");
    fs::write(&proxy, b"fixture executable; never launched").unwrap();
    let hash = binary_sha256(&proxy).unwrap();
    assert!(codex_broker_tool_options(&proxy, &hash).is_ok());
    let alias = root.join("alias");
    symlink(&proxy, &alias).unwrap();
    assert!(codex_broker_tool_options(&alias, &hash).is_err());
    assert!(codex_broker_tool_options(&root, &hash).is_err());
    assert!(codex_broker_tool_options(&proxy, &hash.to_uppercase()).is_err());
    fs::write(&proxy, b"replacement").unwrap();
    assert!(codex_broker_tool_options(&proxy, &hash).is_err());
}

#[test]
fn broker_policy_quotes_path_without_expanding_tools_or_embedding_capability() {
    let directory = tempfile::tempdir().unwrap();
    let proxy = directory
        .path()
        .canonicalize()
        .unwrap()
        .join("proxy \\\"name");
    fs::write(&proxy, b"fixture").unwrap();
    let options = codex_broker_tool_options(&proxy, &binary_sha256(&proxy).unwrap()).unwrap();
    let overrides: Vec<_> = options
        .chunks_exact(2)
        .map(|pair| {
            assert_eq!(pair[0], "-c");
            pair[1].as_str()
        })
        .collect();
    assert_eq!(overrides.len(), 9);
    assert!(overrides.contains(&"features.shell_tool=false"));
    assert!(overrides.contains(&"features.multi_agent=false"));
    assert!(overrides.contains(&"features.plugins=false"));
    assert!(overrides.contains(&"features.remote_plugin=false"));
    let server = overrides.last().unwrap();
    assert!(server.contains(&format!(
        "command={}",
        serde_json::to_string(proxy.to_str().unwrap()).unwrap()
    )));
    assert!(server.contains("env_vars=[\"CONDUCTOR_BROKER_HANDOFF\"]"));
    assert!(server.contains("enabled_tools=[\"conductor_read_text\",\"conductor_write_text\"]"));
    assert!(server.contains("default_tools_approval_mode=\"prompt\""));
    assert!(!server.contains("env={"));
    let invalid = proxy.with_file_name("proxy\nconfig");
    fs::write(&invalid, b"fixture").unwrap();
    assert!(codex_broker_tool_options(&invalid, &binary_sha256(&invalid).unwrap()).is_err());
}
