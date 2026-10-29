//! Observable correlation only: tags are forgeable and never process authority.
use std::ffi::OsString;

const PREFIX: &str = "--conductor-owner=v1:";

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerProcessIdentity {
    pub authority_id: String,
    pub packet_sha256: String,
    pub wrapper_identity: String,
}
impl RunnerProcessIdentity {
    /// # Errors
    /// Rejects noncanonical namespace, packet digest or wrapper UUID.
    pub fn argument(&self) -> Result<String, &'static str> {
        if !hex64(&self.authority_id)
            || !hex64(&self.packet_sha256)
            || !uuid(&self.wrapper_identity)
        {
            return Err("invalid runner process identity");
        }
        Ok(format!(
            "{PREFIX}{}:{}:{}",
            self.authority_id, self.packet_sha256, self.wrapper_identity
        ))
    }

    /// Parses only the versioned single-argument protocol. No arguments means
    /// legacy/unscoped operation; this cannot support scoped process inventory.
    /// # Errors
    /// Rejects malformed, duplicate or unknown arguments without echoing them.
    pub fn from_arguments(
        arguments: impl IntoIterator<Item = OsString>,
    ) -> Result<Option<Self>, &'static str> {
        let mut arguments = arguments.into_iter();
        let Some(argument) = arguments.next() else {
            return Ok(None);
        };
        if arguments.next().is_some() {
            return Err("invalid runner arguments");
        }
        let value = argument
            .to_str()
            .and_then(|s| s.strip_prefix(PREFIX))
            .ok_or("invalid runner arguments")?;
        if value.len() != 166 {
            return Err("invalid runner arguments");
        }
        let fields: Vec<_> = value.split(':').collect();
        if fields.len() != 3 {
            return Err("invalid runner arguments");
        }
        let identity = Self {
            authority_id: fields[0].into(),
            packet_sha256: fields[1].into(),
            wrapper_identity: fields[2].into(),
        };
        identity.argument()?;
        Ok(Some(identity))
    }
}
fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn process_identity_is_canonical_bounded_and_argument_only() {
        let value = RunnerProcessIdentity {
            authority_id: "a".repeat(64),
            packet_sha256: "b".repeat(64),
            wrapper_identity: "12345678-1234-1234-1234-123456789abc".into(),
        };
        let arg = value.argument().unwrap();
        assert_eq!(
            RunnerProcessIdentity::from_arguments([arg.clone().into()]).unwrap(),
            Some(value)
        );
        assert_eq!(RunnerProcessIdentity::from_arguments([]).unwrap(), None);
        for args in [
            vec!["--unknown".into()],
            vec![arg.clone().into(), arg.clone().into()],
            vec![arg.replace("v1:", "v2:").into()],
            vec![arg.to_uppercase().into()],
            vec![format!("{arg}:extra").into()],
        ] {
            assert!(RunnerProcessIdentity::from_arguments(args).is_err());
        }
    }
}
