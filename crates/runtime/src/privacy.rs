//! A literal credential-shape policy, not a detector for arbitrary secrets.
const MARKER: &str = "[redacted]";
const PREFIXES: &[&str] = &[
    concat!("sk", "-ant-"),
    concat!("sk", "-proj-"),
    concat!("sk", "-svcacct-"),
    "ghp_",
    "github_pat_",
    "glpat-",
];

/// Removes recognized credential-shaped spans from untrusted display/context text.
/// Callers must bound input first. No file, environment or network reads occur.
#[must_use]
pub fn redact_credentials(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut offset = 0;
    while offset < text.len() {
        let remaining = &text[offset..];
        if let Some(length) = private_key_length(remaining).or_else(|| token_length(remaining)) {
            output.push_str(MARKER);
            offset += length;
        } else if let Some(character) = remaining.chars().next() {
            output.push(character);
            offset += character.len_utf8();
        }
    }
    output
}
fn token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"_-./+=".contains(&byte)
}
fn token_length(text: &str) -> Option<usize> {
    for prefix in PREFIXES {
        if let Some(body) = text.strip_prefix(prefix) {
            let length = body.bytes().take_while(|b| token_byte(*b)).count();
            if length >= 8 {
                return Some(prefix.len() + length);
            }
        }
    }
    if text
        .as_bytes()
        .get(..7)
        .is_some_and(|v| v.eq_ignore_ascii_case(b"Bearer "))
    {
        let length = text[7..].bytes().take_while(|b| token_byte(*b)).count();
        if length >= 8 {
            return Some(7 + length);
        }
    }
    if text.starts_with("AKIA")
        && text.as_bytes().get(4..20).is_some_and(|v| {
            v.iter()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        })
    {
        return Some(20);
    }
    None
}
fn private_key_length(text: &str) -> Option<usize> {
    let body = text.strip_prefix("-----BEGIN ")?;
    let labels = [
        "PRIVATE KEY",
        "RSA PRIVATE KEY",
        "EC PRIVATE KEY",
        "OPENSSH PRIVATE KEY",
        "ENCRYPTED PRIVATE KEY",
    ];
    for label in labels {
        if body.starts_with(&format!("{label}-----")) {
            let end = format!("-----END {label}-----");
            // An incomplete block must not expose a partial key.
            return Some(text.find(&end).map_or(text.len(), |at| at + end.len()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn literal_policy_covers_tokens_and_preserves_unicode_context() {
        for prefix in PREFIXES.iter().copied().chain(["Bearer ", "bEaReR "]) {
            let secret = format!("{prefix}SyntheticCanary12345");
            let input = format!("önce 🔒 {secret}; sonra");
            assert_eq!(redact_credentials(&input), "önce 🔒 [redacted]; sonra");
        }
        let access = ["AKIA", "0123456789ABCDEF"].concat();
        assert_eq!(
            redact_credentials(&format!("id={access}!")),
            "id=[redacted]!"
        );
        for plain in [
            "hello",
            "ghp_short",
            "Bearer ${TOKEN}",
            "東京",
            "0123456789abcdef",
        ] {
            assert_eq!(redact_credentials(plain), plain);
        }
    }
    #[test]
    fn private_key_blocks_are_removed_even_when_truncated() {
        for label in [
            "PRIVATE KEY",
            "RSA PRIVATE KEY",
            "EC PRIVATE KEY",
            "OPENSSH PRIVATE KEY",
            "ENCRYPTED PRIVATE KEY",
        ] {
            let beginning = format!("-----BEGIN {label}-----");
            let end = format!("-----END {label}-----");
            assert_eq!(
                redact_credentials(&format!("before {beginning}\nPRIVATE_BYTES\n{end} after")),
                "before [redacted] after"
            );
            assert_eq!(
                redact_credentials(&format!("before {beginning}\nPARTIAL")),
                "before [redacted]"
            );
        }
        let once = redact_credentials("Bearer SyntheticCanary12345");
        assert_eq!(redact_credentials(&once), once);
    }
}
