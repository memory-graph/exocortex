//! D67: credential discovery for the CLI fallback. An agent whose MCP
//! session died has no `runtime.env` to source — the live owner's
//! endpoint and secrets are published beside the graph as the attach
//! record (D44-S2), and THAT record is the re-resolution source the
//! long-lived client already consults on owner replacement (D55). This
//! module resolves it for one-shot CLI processes, fail-closed through
//! the ONE validator (`AttachInfo::validate`): a malformed or absent
//! record is an error, never a guess.

use std::path::{Path, PathBuf};

use exocortex_wire::attach::AttachInfo;

/// A resolved attach record plus the data home it was read from (the
/// stderr notice names both so an operator can see WHICH home served).
#[derive(Debug, Clone)]
pub struct Discovered {
    pub info: AttachInfo,
    pub data_home: PathBuf,
}

/// The data home to consult, in precedence order: an explicit
/// `--data-dir`, then the wrapper-stamped
/// `EXOCORTEX_STANDALONE_DATA_HOME` (present in every standalone
/// session's runtime env, owner and attacher alike), then the platform
/// default (§4.3 — the same shape the node's own `data_home()` uses, so
/// a bare `exocortex-cli` finds the default installation's node).
pub fn standalone_data_home(explicit: Option<PathBuf>) -> PathBuf {
    if let Some(dir) = explicit {
        return dir;
    }
    if let Ok(stamped) = std::env::var("EXOCORTEX_STANDALONE_DATA_HOME") {
        if !stamped.is_empty() {
            return PathBuf::from(stamped);
        }
    }
    platform_data_home()
}

/// The §4.3 platform default data home. `HOME`-less processes fall
/// back to a temp path (the `dirs_fallback` precedent in the MCP
/// client): discovery there simply finds no record and reports it.
pub fn platform_data_home() -> PathBuf {
    let Ok(home) = std::env::var("HOME") else {
        return std::env::temp_dir().join("exocortex");
    };
    if home.is_empty() {
        return std::env::temp_dir().join("exocortex");
    }
    if cfg!(target_os = "macos") {
        Path::new(&home)
            .join("Library")
            .join("Application Support")
            .join("exocortex")
    } else {
        Path::new(&home)
            .join(".local")
            .join("share")
            .join("exocortex")
    }
}

/// Read + validate the attach record from `<data_home>/attach.json`.
/// An explicitly requested org that disagrees with the record's org is
/// REFUSED, never adopted (the R14-final rule: attachers do not
/// hydrate a foreign partition — and the CLI must not write one
/// either). Returns the record on success.
pub fn discover(data_home: &Path, explicit_org: Option<&str>) -> Result<Discovered, String> {
    let info = AttachInfo::read(data_home)?;
    if let Some(org) = explicit_org {
        if org != info.org {
            return Err(format!(
                "the attach record at {} serves org {} but org {org} was requested — refusing foreign-org credentials (drop --org/EXOCORTEX_ORG or point --data-dir at that org's home)",
                data_home.join("attach.json").display(),
                info.org
            ));
        }
    }
    Ok(Discovered {
        info,
        data_home: data_home.to_path_buf(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_record(dir: &Path, org: &str) {
        std::fs::create_dir_all(dir).unwrap();
        let info = AttachInfo {
            backend: "http://127.0.0.1:54326".into(),
            org: org.into(),
            auth_token: "ab".repeat(32),
            hmac_key: "cd".repeat(32),
            sse_key: "ef".repeat(32),
        };
        std::fs::write(
            dir.join("attach.json"),
            serde_json::to_string(&info).unwrap(),
        )
        .unwrap();
    }

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "exo-standalone-creds-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::remove_dir_all(&dir).ok();
        dir
    }

    #[test]
    fn discovery_reads_validates_and_adopts_the_record_org() {
        let dir = scratch("adopt");
        write_record(&dir, "personal");
        let found = discover(&dir, None).expect("valid record resolves");
        assert_eq!(found.info.org, "personal");
        assert_eq!(found.info.backend, "http://127.0.0.1:54326");
        assert_eq!(found.data_home, dir);
        // A matching explicit org passes through.
        assert!(discover(&dir, Some("personal")).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn discovery_refuses_a_foreign_org_fail_closed() {
        let dir = scratch("foreign");
        write_record(&dir, "personal");
        let err = discover(&dir, Some("loanlight")).expect_err("mismatch refused");
        assert!(
            err.contains("refusing foreign-org credentials"),
            "names the refusal: {err}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn discovery_fails_closed_without_a_record() {
        let dir = scratch("absent");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(discover(&dir, None).is_err());
        std::fs::write(dir.join("attach.json"), "{ not json").unwrap();
        assert!(discover(&dir, None).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn explicit_data_dir_wins_then_the_platform_default_shape() {
        let explicit = PathBuf::from("/tmp/explicit-home");
        assert_eq!(standalone_data_home(Some(explicit.clone())), explicit);
        // The platform default mirrors the node's §4.3 shape: a
        // three-segment join under HOME on macOS, ~/.local/share
        // elsewhere. HOME is present in every test runner; assert the
        // tail segments so the shape stays honest without pinning the
        // machine's HOME.
        let home = std::env::var("HOME").unwrap();
        let default = platform_data_home();
        let expected_tail: Vec<String> = if cfg!(target_os = "macos") {
            ["Library", "Application Support", "exocortex"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            [".local", "share", "exocortex"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        };
        let mut expected = PathBuf::from(&home);
        for segment in &expected_tail {
            expected.push(segment);
        }
        assert_eq!(default, expected);
    }
}
