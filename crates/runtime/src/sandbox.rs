//! macOS command sandbox construction. This is a filesystem/network boundary,
//! not a process-group fence or proof of vendor adapter conformance.
use crate::{NetworkPolicy, ProcessSpec};
use std::fmt::Write;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, thiserror::Error)]
pub enum SandboxError {
    #[error("sandbox is unsupported on this platform or for this network policy")]
    Unsupported,
    #[error("sandbox roots must be existing canonical directories")]
    InvalidRoot,
}

const PROFILE: &str = r#"(version 1)
(deny default)
(allow process-exec)
(allow process-fork)
(allow signal (target same-sandbox))
(allow process-info* (target same-sandbox))
(allow sysctl-read)
; The loader/getcwd needs the root directory itself, not recursive root access.
(allow file-read* (literal "/") (subpath "/usr/bin") (subpath "/usr/lib")
                  (subpath "/bin") (subpath "/System/Library")
                  (subpath "/System/Volumes/Preboot/Cryptexes/OS")
                  (literal "/dev/null") (literal "/dev/urandom")
                  (subpath (param "WORKSPACE")))
(allow file-write* (subpath (param "WORKSPACE")))
(allow file-write-data (literal "/dev/null"))
(deny file-write-unlink (literal (param "WORKSPACE")))
"#;

/// Builds a deny-by-default macOS command with explicit environment and an
/// owner-selected work root. No network or control-socket access is granted.
/// Paths enter Seatbelt as parameters, never interpolated profile source.
/// # Errors
/// Refuses unsupported hosts/network modes and noncanonical/missing directories.
pub fn sandboxed_command(
    workspace: &Path,
    executable: &Path,
    arguments: &[String],
    read_roots: &[PathBuf],
    network: NetworkPolicy,
    timeout: Duration,
) -> Result<ProcessSpec, SandboxError> {
    #[cfg(target_os = "linux")]
    if network == NetworkPolicy::Denied {
        return crate::linux_sandbox::command(
            workspace, executable, arguments, read_roots, timeout,
        );
    }
    if !cfg!(target_os = "macos") || network != NetworkPolicy::Denied {
        return Err(SandboxError::Unsupported);
    }
    let workspace = canonical_directory(workspace)?;
    let mut profile = PROFILE.to_string();
    let mut parameters = vec![
        "-D".into(),
        format!(
            "WORKSPACE={}",
            workspace.to_str().ok_or(SandboxError::InvalidRoot)?
        ),
    ];
    for (index, root) in read_roots.iter().enumerate() {
        let root = canonical_directory(root)?;
        writeln!(
            profile,
            "(allow file-read* (subpath (param \"READ_{index}\")))"
        )
        .map_err(|_| SandboxError::InvalidRoot)?;
        parameters.extend([
            "-D".into(),
            format!(
                "READ_{index}={}",
                root.to_str().ok_or(SandboxError::InvalidRoot)?
            ),
        ]);
    }
    if !executable.is_absolute() {
        return Err(SandboxError::InvalidRoot);
    }
    parameters.extend([
        "-p".into(),
        profile,
        executable.to_str().ok_or(SandboxError::InvalidRoot)?.into(),
    ]);
    parameters.extend(arguments.iter().cloned());
    Ok(ProcessSpec {
        executable: "/usr/bin/sandbox-exec".into(),
        arguments: parameters,
        directory: workspace.clone(),
        environment: BTreeMap::from([
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("HOME".into(), workspace.to_string_lossy().into_owned()),
            ("TMPDIR".into(), workspace.to_string_lossy().into_owned()),
        ]),
        timeout,
        output_limit: 1024 * 1024,
    })
}
fn canonical_directory(path: &Path) -> Result<PathBuf, SandboxError> {
    let canonical = path.canonicalize().map_err(|_| SandboxError::InvalidRoot)?;
    if !path.is_absolute()
        || canonical != path
        || !canonical.is_dir()
        || canonical.parent().is_none()
    {
        return Err(SandboxError::InvalidRoot);
    }
    Ok(canonical)
}
