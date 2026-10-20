//! Last-mile redaction for model-visible values and persistent diagnostics.
//! This is a data-flow boundary, not protection against same-user process access.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::fmt;
use std::io::Write;
use std::sync::{Arc, LazyLock, Mutex, Weak};
use zeroize::{Zeroize, Zeroizing};

pub const REDACTED: &str = "[REDACTED]";
struct Secret {
    value: Zeroizing<String>,
    representations: Zeroizing<Vec<String>>,
}
static SECRETS: LazyLock<Mutex<Vec<Weak<Secret>>>> = LazyLock::new(|| Mutex::new(Vec::new()));
// Retain only authenticated password patterns after logout: in-flight output
// can arrive after its credential capability has already been revoked.
static RETAINED: LazyLock<Mutex<Vec<SensitiveString>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Clone shares one zeroized allocation. Debug/Serialize never reveal its value.
#[derive(Clone)]
pub struct SensitiveString(Arc<Secret>);
impl SensitiveString {
    pub fn new(value: String) -> Self {
        let escaped = Zeroizing::new(serde_json::to_string(&value).unwrap_or_default());
        let mut representations = vec![value.clone(), escaped.trim_matches('"').to_string()];
        let encoded = Zeroizing::new(
            value
                .bytes()
                .map(|byte| format!("%{byte:02X}"))
                .collect::<String>(),
        );
        representations.extend([
            encoded.to_string(),
            encoded.to_ascii_lowercase(),
            base64(value.as_bytes()),
        ]);
        representations.retain(|item| !item.is_empty());
        let secret = Arc::new(Secret {
            value: Zeroizing::new(value),
            representations: Zeroizing::new(representations),
        });
        let mut known = SECRETS.lock().unwrap_or_else(|e| e.into_inner());
        known.retain(|item| item.strong_count() != 0);
        known.push(Arc::downgrade(&secret));
        Self(secret)
    }
    /// Only authentication transports and native UI plumbing may call this.
    pub fn expose(&self) -> &str {
        self.0.value.as_str()
    }
    pub fn is_empty(&self) -> bool {
        self.0.value.is_empty()
    }
    /// Bounded process-lifetime output filter, separate from login authority.
    pub fn retain_for_redaction(&self) -> bool {
        let mut retained = RETAINED.lock().unwrap_or_else(|e| e.into_inner());
        if retained.iter().any(|known| known.expose() == self.expose()) {
            return true;
        }
        if retained.len() >= 128 {
            return false;
        }
        retained.push(self.clone());
        true
    }
}
impl fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}
impl Serialize for SensitiveString {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(REDACTED)
    }
}
impl<'de> Deserialize<'de> for SensitiveString {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d).map(Self::new)
    }
}

fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let word = ((chunk[0] as u32) << 16)
            | ((chunk.get(1).copied().unwrap_or(0) as u32) << 8)
            | chunk.get(2).copied().unwrap_or(0) as u32;
        result.push(TABLE[(word >> 18) as usize] as char);
        result.push(TABLE[((word >> 12) & 63) as usize] as char);
        result.push(if chunk.len() > 1 {
            TABLE[((word >> 6) & 63) as usize] as char
        } else {
            '='
        });
        result.push(if chunk.len() > 2 {
            TABLE[(word & 63) as usize] as char
        } else {
            '='
        });
    }
    result
}

/// Suppress the whole affected string, retaining structured status/exit fields.
/// Unlike repeated replacements this remains bounded even for short passwords.
pub fn redact_text(text: &str) -> String {
    let secrets: Vec<_> = SECRETS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter_map(Weak::upgrade)
        .collect();
    if secrets.iter().any(|secret| {
        secret
            .representations
            .iter()
            .any(|value| text.contains(value))
    }) {
        REDACTED.into()
    } else {
        text.into()
    }
}
pub fn redact_value(value: &mut Value) {
    match value {
        Value::String(text) => {
            let safe = redact_text(text);
            text.zeroize();
            *text = safe;
        }
        Value::Array(items) => {
            for item in items {
                redact_value(item);
            }
        }
        Value::Object(items) => {
            for (name, item) in items {
                if matches!(
                    name.to_ascii_lowercase().as_str(),
                    "password" | "passphrase" | "askpass_token"
                ) {
                    if let Value::String(text) = item {
                        text.zeroize();
                    }
                    *item = Value::String(REDACTED.into());
                } else {
                    redact_value(item);
                }
            }
        }
        _ => {}
    }
}
pub fn redacted_value(mut value: Value) -> Value {
    redact_value(&mut value);
    value
}

/// tracing's writer factory creates one guard per event, so fragments cannot
/// split a secret across writes. Oversized events are suppressed in full.
#[derive(Default)]
pub struct RedactingStderr {
    buffer: Zeroizing<Vec<u8>>,
    overflow: bool,
}
impl RedactingStderr {
    fn safe_text(&self) -> String {
        if self.overflow {
            format!("{REDACTED}: oversized diagnostic event\n")
        } else {
            redact_text(&String::from_utf8_lossy(&self.buffer))
        }
    }
}
impl Write for RedactingStderr {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.buffer.len().saturating_add(bytes.len()) > 65536 {
            self.buffer.zeroize();
            self.overflow = true;
        } else if !self.overflow {
            self.buffer.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Drop for RedactingStderr {
    fn drop(&mut self) {
        let _ = std::io::stderr()
            .lock()
            .write_all(self.safe_text().as_bytes());
    }
}
pub fn install_panic_redaction() {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("{}", redact_text(&info.to_string()));
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn revoked_credentials_still_filter_late_output() {
        let secret = SensitiveString::new("revoked-filter-canary-9712".into());
        assert!(secret.retain_for_redaction());
        drop(secret);
        assert_eq!(
            redact_text("late output: revoked-filter-canary-9712"),
            REDACTED
        );
    }
    #[test]
    fn secrets_never_serialize_and_known_encodings_are_suppressed() {
        let value = "test-private-\"credential-5678".to_string();
        let secret = SensitiveString::new(value.clone());
        assert_eq!(format!("{secret:?}"), REDACTED);
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"[REDACTED]\"");
        assert_eq!(redact_text(&format!("error: {value}")), REDACTED);
        assert_eq!(redact_text(&base64(value.as_bytes())), REDACTED);
        let mut result =
            serde_json::json!({"stdout":value,"exit_code":0,"nested":{"password":"unregistered"}});
        redact_value(&mut result);
        assert_eq!(result["stdout"], REDACTED);
        assert_eq!(result["exit_code"], 0);
        assert_eq!(result["nested"]["password"], REDACTED);
    }
    #[test]
    fn large_sensitive_output_is_suppressed_without_replacement_expansion() {
        let secret = SensitiveString::new("bounded-redaction-canary-5678".into());
        let text = format!("{}{}", "x".repeat(1024 * 1024), secret.expose());
        assert_eq!(redact_text(&text), REDACTED);
    }
    #[test]
    fn diagnostic_fragments_cannot_split_a_secret_and_overflow_is_not_dumped() {
        let password = SensitiveString::new("fragmented-private-canary-8134".into());
        let mut writer = RedactingStderr::default();
        writer.write_all(b"fragmented-private-").unwrap();
        writer.write_all(b"canary-8134").unwrap();
        assert_eq!(writer.safe_text(), REDACTED);
        writer.write_all(&vec![b'x'; 65537]).unwrap();
        assert!(!writer.safe_text().contains(password.expose()));
        assert!(writer.safe_text().contains("oversized"));
    }
}
