//! Immutable non-secret broker launch intent before native process creation.
use super::{digest, invalid};
use crate::StoreError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileBrokerLaunchBinding {
    pub launch_sha256: String,
    pub wrapper_path: String,
    pub wrapper_sha256: String,
    pub receipt_root: String,
    pub receipt_root_identity_sha256: String,
    pub platform: String,
    pub host_id: String,
    pub boot_id: String,
}
impl FileBrokerLaunchBinding {
    /// Validate bounded, non-secret metadata before persisting a launch intent.
    /// # Errors
    /// Rejects unsupported hosts, malformed digests and unsafe or oversized paths.
    pub fn validate(&self) -> Result<(), StoreError> {
        if [
            &self.launch_sha256,
            &self.wrapper_sha256,
            &self.receipt_root_identity_sha256,
            &self.host_id,
            &self.boot_id,
        ]
        .iter()
        .any(|v| !digest(v))
            || !matches!(self.platform.as_str(), "macos" | "linux")
            || [&self.wrapper_path, &self.receipt_root].iter().any(|v| {
                v.len() > 4096
                    || v.chars().any(char::is_control)
                    || !std::path::Path::new(v).is_absolute()
                    || std::path::Path::new(v)
                        .components()
                        .any(|c| matches!(c, std::path::Component::ParentDir))
            })
        {
            return Err(invalid());
        }
        let encoded = serde_json::to_string(self).map_err(|_| invalid())?;
        if encoded.len() > 16384 {
            return Err(invalid());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn launch_metadata_rejects_unsafe_paths_and_substituted_identity() {
        let valid = FileBrokerLaunchBinding {
            launch_sha256: "a".repeat(64),
            wrapper_path: "/approved/runner".into(),
            wrapper_sha256: "b".repeat(64),
            receipt_root: "/private/receipts".into(),
            receipt_root_identity_sha256: "c".repeat(64),
            platform: "linux".into(),
            host_id: "d".repeat(64),
            boot_id: "e".repeat(64),
        };
        valid.validate().unwrap();
        for path in [
            "relative",
            "/root/../other",
            "/root/\nname",
            &format!("/{}", "x".repeat(4096)),
        ] {
            for wrapper in [true, false] {
                let mut changed = valid.clone();
                if wrapper {
                    changed.wrapper_path = path.into();
                } else {
                    changed.receipt_root = path.into();
                }
                assert!(changed.validate().is_err());
            }
        }
        let mut changed = valid.clone();
        changed.platform = "unsupported".into();
        assert!(changed.validate().is_err());
        for field in [
            "launch_sha256",
            "wrapper_sha256",
            "receipt_root_identity_sha256",
            "host_id",
            "boot_id",
        ] {
            let mut json = serde_json::to_value(&valid).unwrap();
            json[field] = "A".repeat(64).into();
            let changed: FileBrokerLaunchBinding = serde_json::from_value(json).unwrap();
            assert!(changed.validate().is_err());
        }
        let mut json = serde_json::to_value(&valid).unwrap();
        json["bearer"] = "unexpected".into();
        assert!(serde_json::from_value::<FileBrokerLaunchBinding>(json).is_err());
    }
}
