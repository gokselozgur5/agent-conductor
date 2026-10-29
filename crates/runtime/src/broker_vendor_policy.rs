//! Fixed Codex MCP policy; constructing options is not native writer admission.
use crate::{VendorError, binary_sha256};
use std::path::Path;

/// Build the two-tool broker profile for an independently approved proxy binary.
/// The caller must also pin the vendor, ignore ambient configuration and enforce
/// read-only direct filesystem authority plus OS confinement before spawning.
/// The capability value is deliberately absent: forward it only in child env.
/// See <https://learn.chatgpt.com/docs/extend/mcp> for MCP policy semantics.
/// # Errors
/// Rejects noncanonical, changed or invalid proxy executable identities.
pub fn codex_broker_tool_options(
    proxy: &Path,
    approved_sha256: &str,
) -> Result<Vec<String>, VendorError> {
    let path = proxy.to_str().ok_or(VendorError::Identity)?;
    if !proxy.is_absolute()
        || path.len() > 4096
        || path.chars().any(char::is_control)
        || proxy.canonicalize().map_err(|_| VendorError::Identity)? != proxy
        || approved_sha256.len() != 64
        || !approved_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || binary_sha256(proxy)? != approved_sha256
    {
        return Err(VendorError::Identity);
    }
    let quoted = serde_json::to_string(path).map_err(|_| VendorError::Encoding)?;
    // These overrides are fixed; callers cannot supply additional tool names or
    // turn the server default into blanket approval. No secret enters an option.
    let server = format!(
        "mcp_servers.conductor_files={{command={quoted},required=true,env_vars=[\"CONDUCTOR_BROKER_HANDOFF\"],enabled_tools=[\"conductor_read_text\",\"conductor_write_text\"],default_tools_approval_mode=\"prompt\",tools={{conductor_read_text={{approval_mode=\"approve\"}},conductor_write_text={{approval_mode=\"approve\"}}}}}}"
    );
    Ok([
        "features.shell_tool=false",
        "features.shell_snapshot=false",
        "features.apps=false",
        "features.multi_agent=false",
        "web_search=\"disabled\"",
        "project_doc_max_bytes=0",
        &server,
    ]
    .into_iter()
    .flat_map(|value| ["-c".to_owned(), value.to_owned()])
    .collect())
}

/// Construct a command-free Codex writer command with read-only direct files.
/// Broker writes require the separately owned live session; the caller must add
/// its handoff only to child environment after release and enforce OS confinement.
/// Does not launch, authorize a fresh broker or establish native writer acceptance.
/// # Errors
/// Refuses non-Codex/command-capable contracts, missing scoped write authority,
/// changed vendor/proxy pins and unsupported authentication environment entries.
pub fn broker_codex_command(
    pin: &crate::VendorPin,
    packet: &crate::ResumePacket,
    authentication: std::collections::BTreeMap<String, String>,
    proxy: &Path,
    approved_proxy_sha256: &str,
) -> Result<crate::ProcessSpec, VendorError> {
    if pin.kind != crate::AdapterKind::Codex
        || packet.contract.authority.allow_workspace_commands
        || packet.contract.authority.write_roots != [packet.contract.workspace.clone()]
    {
        return Err(VendorError::Capability);
    }
    let options = codex_broker_tool_options(proxy, approved_proxy_sha256)?;
    let mut spec = crate::vendor::vendor_command(pin, packet, authentication, true)?;
    // Prefix trusted global options before the existing exec subcommand. Never
    // append options after the task prompt or mutate the frozen contract.
    spec.arguments.splice(0..0, options);
    Ok(spec)
}
