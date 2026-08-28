//! Shared bounded JSON and private atomic-file I/O.

use std::io::{Read as _, Write as _};
use std::path::Path;

/// Reject a byte count above `limit`, naming the caller's artifact in the
/// diagnostic.
pub fn ensure_size(size: u64, limit: u64, noun: &str) -> std::io::Result<()> {
    if size > limit {
        return Err(std::io::Error::other(format!(
            "{noun} is {size} bytes; maximum supported size is {limit} bytes"
        )));
    }
    Ok(())
}

/// Serialize pretty JSON without ever growing the output beyond `limit`.
pub fn serialize_json_pretty_bounded<T: serde::Serialize>(
    value: &T,
    limit: u64,
    noun: &str,
) -> std::io::Result<Vec<u8>> {
    let mut output = BoundedOutput::new(limit, noun);
    serde_json::to_writer_pretty(&mut output, value).map_err(std::io::Error::other)?;
    Ok(output.bytes)
}

struct BoundedOutput<'a> {
    bytes: Vec<u8>,
    limit: u64,
    noun: &'a str,
}

impl<'a> BoundedOutput<'a> {
    fn new(limit: u64, noun: &'a str) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            noun,
        }
    }
}

impl std::io::Write for BoundedOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        ensure_size(
            (self.bytes.len() as u64).saturating_add(bytes.len() as u64),
            self.limit,
            self.noun,
        )?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Read at most `limit` bytes, checking both metadata and the actual stream so
/// growth or non-regular inputs cannot bypass the bound.
pub fn read_bounded(path: &Path, limit: u64, noun: &str) -> std::io::Result<Vec<u8>> {
    let contextual = |operation: &str, source: std::io::Error| {
        std::io::Error::new(
            source.kind(),
            format!("{operation} {noun} {}: {source}", path.display()),
        )
    };
    let mut file = std::fs::File::open(path).map_err(|error| contextual("read", error))?;
    ensure_size(
        file.metadata()
            .map_err(|error| contextual("inspect", error))?
            .len(),
        limit,
        noun,
    )?;
    let mut raw = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(limit.saturating_add(1))
        .read_to_end(&mut raw)
        .map_err(|error| contextual("read", error))?;
    ensure_size(raw.len() as u64, limit, noun)?;
    Ok(raw)
}

/// Atomically replace `path` with a mode-0600 file, syncing file contents and
/// the containing directory before reporting success.
pub fn atomic_write_private(path: &Path, bytes: &[u8], noun: &str) -> std::io::Result<()> {
    atomic_write_private_with(path, bytes, noun, |_| Ok(()))
}

/// Testable form of [`atomic_write_private`]. The hook runs after the
/// temporary file is durable and before rename; failures remove the temporary
/// file and preserve the previous destination.
fn atomic_write_private_with(
    path: &Path,
    bytes: &[u8],
    noun: &str,
    before_rename: impl FnOnce(&Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("backup");
    let mut opened = None;
    for attempt in 0..100u32 {
        let candidate = parent.join(format!(".{name}.tmp-{}-{attempt}", std::process::id()));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        match options.open(&candidate) {
            Ok(file) => {
                opened = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    let (temporary, mut file) = opened.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("could not allocate {noun} temporary file"),
        )
    })?;
    let result = (|| {
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        before_rename(&temporary)?;
        std::fs::rename(&temporary, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "exocortex-bounded-io-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn canonical_bounds_are_inclusive_and_preserve_the_diagnostic_noun() {
        assert!(ensure_size(3, 3, "sentinel artifact").is_ok());
        let error = ensure_size(4, 3, "sentinel artifact").unwrap_err();
        assert!(error.to_string().contains("sentinel artifact"));
        assert!(serialize_json_pretty_bounded(&"x", 3, "sentinel artifact").is_ok());
        assert!(serialize_json_pretty_bounded(&"x", 2, "sentinel artifact").is_err());

        let dir = fixture("read");
        let path = dir.join("artifact.json");
        std::fs::write(&path, b"123").unwrap();
        assert_eq!(read_bounded(&path, 3, "sentinel artifact").unwrap(), b"123");
        assert!(read_bounded(&path, 2, "sentinel artifact")
            .unwrap_err()
            .to_string()
            .contains("sentinel artifact"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn canonical_atomic_replace_is_private_durable_and_cleans_up_on_failure() {
        let dir = fixture("atomic");
        let path = dir.join("artifact.json");
        std::fs::write(&path, b"previous").unwrap();
        atomic_write_private_with(&path, b"replacement", "sentinel artifact", |_| {
            Err(std::io::Error::other("injected before rename"))
        })
        .unwrap_err();
        assert_eq!(std::fs::read(&path).unwrap(), b"previous");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);

        atomic_write_private(&path, b"replacement", "sentinel artifact").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
