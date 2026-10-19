//! Shared safety helpers for paths and shell command construction.

use crate::error::{CoreError, CoreResult};
use std::path::{Path, PathBuf};

/// Quote one shell argument for POSIX shells.
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Normalize a remote POSIX absolute path independently of the local OS.
///
/// Remote paths must keep `/` semantics even when LRW runs natively on
/// Windows. Backslashes are accepted at configuration boundaries and converted
/// so an existing JSON file cannot accidentally produce remote `\` escapes.
pub fn normalize_remote_absolute(path: &Path) -> CoreResult<String> {
    let raw = path.to_string_lossy().replace('\\', "/");
    if raw.contains('\0') || !raw.starts_with('/') {
        return Err(CoreError::Validation(format!(
            "Remote path '{}' must be an absolute POSIX path",
            path.display()
        )));
    }
    let mut parts = Vec::new();
    for part in raw.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return Err(CoreError::PermissionDenied(format!(
                        "Remote path '{}' escapes filesystem root",
                        path.display()
                    )));
                }
            }
            value => parts.push(value),
        }
    }
    Ok(if parts.is_empty() {
        "/".into()
    } else {
        format!("/{}", parts.join("/"))
    })
}

/// Join a user-supplied path under a remote POSIX root without allowing escape.
pub fn safe_join(base: &Path, relative_path: &Path) -> CoreResult<PathBuf> {
    let base = normalize_remote_absolute(base)?;
    let raw = relative_path.to_string_lossy().replace('\\', "/");
    let bytes = raw.as_bytes();
    if raw.contains('\0')
        || raw.starts_with('/')
        || (bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic())
    {
        return Err(CoreError::PermissionDenied(format!(
            "Path '{}' must be relative to its configured remote root",
            relative_path.display()
        )));
    }
    let mut parts: Vec<&str> = base
        .trim_start_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    let base_depth = parts.len();
    for part in raw.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.len() <= base_depth {
                    return Err(CoreError::PermissionDenied(format!(
                        "Path '{}' escapes its configured remote root",
                        relative_path.display()
                    )));
                }
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    Ok(PathBuf::from(format!("/{}", parts.join("/"))))
}

/// Whether one remote absolute path is equal to or below another.
pub fn remote_path_is_within(candidate: &Path, root: &Path) -> CoreResult<bool> {
    let candidate = normalize_remote_absolute(candidate)?;
    let root = normalize_remote_absolute(root)?;
    Ok(candidate == root
        || (root == "/" && candidate.starts_with('/'))
        || candidate
            .strip_prefix(&root)
            .is_some_and(|suffix| suffix.starts_with('/')))
}

/// Whether two remote roots are equal or one contains the other.
pub fn remote_paths_overlap(left: &Path, right: &Path) -> CoreResult<bool> {
    Ok(remote_path_is_within(left, right)? || remote_path_is_within(right, left)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_quote_escapes_single_quotes() {
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn safe_join_keeps_relative_paths_under_base() {
        assert_eq!(
            safe_join(Path::new("/work/ws"), Path::new("src/main.rs")).unwrap(),
            PathBuf::from("/work/ws/src/main.rs")
        );
        assert_eq!(
            safe_join(Path::new("/work/ws"), Path::new("src/../README.md")).unwrap(),
            PathBuf::from("/work/ws/README.md")
        );
    }

    #[test]
    fn safe_join_rejects_escape_paths() {
        assert!(safe_join(Path::new("/work/ws"), Path::new("../secret")).is_err());
        assert!(safe_join(Path::new("/work/ws"), Path::new("/etc/passwd")).is_err());
    }

    #[test]
    fn remote_paths_use_posix_semantics_on_any_host() {
        assert_eq!(
            normalize_remote_absolute(Path::new("\\remote\\work\\src")).unwrap(),
            "/remote/work/src"
        );
        assert_eq!(
            safe_join(Path::new("/remote/work"), Path::new("src\\main.rs")).unwrap(),
            PathBuf::from("/remote/work/src/main.rs")
        );
        assert!(normalize_remote_absolute(Path::new("C:\\data")).is_err());
        assert!(safe_join(Path::new("/remote/work"), Path::new("C:\\data")).is_err());
    }

    #[test]
    fn remote_overlap_respects_component_boundaries() {
        assert!(
            remote_paths_overlap(Path::new("/remote/work"), Path::new("/remote/work/data"))
                .unwrap()
        );
        assert!(
            !remote_paths_overlap(Path::new("/remote/work"), Path::new("/remote/workbench"))
                .unwrap()
        );
    }
}
