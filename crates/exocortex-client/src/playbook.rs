//! D1/D3 (agent-instructions PRD): the versioned Agent Playbook, compiled
//! into the client binary. One version string governs the playbook and
//! the `CLAUDE.md`/`AGENTS.md` block — they are two views of one
//! versioned artifact (there is no separate block version). The
//! generated sections inside the playbook (`<!-- gen:kinds -->`,
//! `<!-- gen:rejects -->`) are emitted by `cargo xtask gen-playbook`;
//! the drift gate fails the build if they disagree with the pack or the
//! `RejectCode` enum.

use std::path::Path;

/// The playbook version this binary carries (D3).
pub const PLAYBOOK_VERSION: &str = "1.0.0";

/// The full playbook (Appendix A of the PRD).
pub const PLAYBOOK: &str = include_str!("playbook/v1_0_0.md");

/// The `CLAUDE.md`/`AGENTS.md` block (Appendix B of the PRD) — the
/// load-bearing ≤300-word surface that rides in the agent's context on
/// every turn.
pub const BLOCK: &str = include_str!("playbook/block_v1_0_0.md");

/// The block's hard length bound (§11): enforced by the drift gate
/// (`cargo xtask gen-playbook`), not by hope.
pub const BLOCK_WORD_LIMIT: usize = 300;

/// `sha256:<hex>` content hash of the compiled playbook (D3).
pub fn playbook_hash() -> String {
    format!(
        "sha256:{}",
        exocortex_wire::signing::content_digest_hex(PLAYBOOK.as_bytes())
    )
}

/// `sha256:<hex>` content hash of the instruction block (D3).
pub fn block_hash() -> String {
    format!(
        "sha256:{}",
        exocortex_wire::signing::content_digest_hex(BLOCK.as_bytes())
    )
}

/// D5: install the playbook under the OS data home (or `--data-dir`),
/// idempotently. Writes the version-tagged file, (re)points the
/// `playbook.md` symlink at it, and writes `version.txt`. Returns the
/// user-facing notice when a NEW version was installed (first run or
/// upgrade); `None` when the current version was already present.
pub fn install(data_dir: &Path) -> std::io::Result<Option<String>> {
    let versioned_name = format!("playbook-v{PLAYBOOK_VERSION}.md");
    let versioned = data_dir.join(&versioned_name);
    let current = data_dir.join("playbook.md");
    let installed_marker = data_dir.join("version.txt");

    let already_current = installed_marker.exists()
        && std::fs::read_to_string(&installed_marker)
            .map(|v| v.contains(&format!("playbook={PLAYBOOK_VERSION}")))
            .unwrap_or(false);

    std::fs::create_dir_all(data_dir)?;
    std::fs::write(&versioned, PLAYBOOK)?;
    // Replacing the symlink: remove the old link (or stale file) first.
    if current.symlink_metadata().is_ok() {
        let _ = std::fs::remove_file(&current);
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(&versioned_name, &current)?;
    #[cfg(not(unix))]
    std::fs::write(&current, PLAYBOOK)?; // copy fallback where symlinks are absent

    let version_row = format!(
        "client={} playbook={}\n",
        env!("CARGO_PKG_VERSION"),
        PLAYBOOK_VERSION
    );
    std::fs::write(&installed_marker, version_row)?;

    if already_current {
        Ok(None)
    } else {
        Ok(Some(format!(
            "[exocortex] playbook v{PLAYBOOK_VERSION} installed at {} — reference it from your harness instructions.",
            current.display()
        )))
    }
}

/// Count words in the block (the drift gate's bound check reuses this
/// shape: whitespace-separated tokens).
pub fn block_word_count() -> usize {
    BLOCK.split_whitespace().count()
}

/// D48: the begin marker carrying the block version. `--dump-block`
/// emits the block between these markers so `--install-block` can find
/// and replace exactly one region on every rerun.
pub fn block_begin_marker() -> String {
    format!("<!-- exocortex-block v{PLAYBOOK_VERSION} -->")
}

/// D48: the end marker (see `block_begin_marker`).
pub fn block_end_marker() -> &'static str {
    "<!-- /exocortex-block -->"
}

/// D48: the emitted form — the block wrapped in its versioned markers.
pub fn marked_block() -> String {
    format!(
        "{}\n\n{}\n{}\n",
        block_begin_marker(),
        BLOCK.trim_end(),
        block_end_marker()
    )
}

/// D48: install the marked block into a harness instruction file
/// (`CLAUDE.md`/`AGENTS.md`) idempotently. Every existing marked region
/// — matched by the version-agnostic `<!-- exocortex-block ` prefix, so
/// an upgrade replaces an OLDER version's region too — is removed
/// (healing a marker-era double append), then one fresh region lands
/// where the first one was — or at the end of a file that has never
/// carried the block. Content outside the markers is never touched.
pub fn install_block_into(existing: &str) -> String {
    const BEGIN_PREFIX: &str = "<!-- exocortex-block ";
    let end = block_end_marker();
    let marked = marked_block();
    let mut rest = existing.to_string();
    let mut first_insert = None;
    loop {
        let Some(start) = rest.find(BEGIN_PREFIX) else {
            break;
        };
        let Some(marker_line) = rest[start..].find('\n') else {
            break;
        };
        let after_line = start + marker_line + 1;
        let Some(end_rel) = rest[after_line..].find(end) else {
            break;
        };
        // Consume the newline following the end marker so reruns do not
        // accumulate blank lines.
        let mut stop = after_line + end_rel + end.len();
        if rest[stop..].starts_with('\n') {
            stop += 1;
        }
        if first_insert.is_none() {
            first_insert = Some(start);
        }
        rest.replace_range(start..stop, "");
    }
    match first_insert {
        Some(at) => format!("{}{}{}", &rest[..at], marked, &rest[at..]),
        None => {
            let mut out = existing.to_string();
            if !out.is_empty() {
                if !out.ends_with('\n') {
                    out.push('\n');
                }
                out.push('\n');
            }
            out.push_str(&marked);
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// P2/D3: the compiled playbook carries its version in the title.
    #[test]
    fn playbook_title_carries_version() {
        assert!(PLAYBOOK.starts_with(&format!("# Exocortex Agent Playbook v{PLAYBOOK_VERSION}")));
    }

    /// §11: the block rides in context on every turn — its size bound is
    /// a product property, not a preference. The drift gate checks the
    /// same bound against the source file; this keeps the compiled
    /// artifact honest even if the gate is skipped.
    #[test]
    fn block_within_word_bound() {
        assert!(
            block_word_count() <= BLOCK_WORD_LIMIT,
            "instruction block is {} words; bound is {}",
            block_word_count(),
            BLOCK_WORD_LIMIT
        );
    }

    /// P2: the generated sections exist in the compiled artifact.
    #[test]
    fn generated_markers_present() {
        assert!(PLAYBOOK.contains("<!-- gen:kinds"));
        assert!(PLAYBOOK.contains("<!-- /gen:kinds -->"));
        assert!(PLAYBOOK.contains("<!-- gen:rejects"));
        assert!(PLAYBOOK.contains("<!-- /gen:rejects -->"));
    }

    /// D5: install is idempotent and reports upgrades only.
    #[test]
    fn install_is_idempotent() {
        let dir = std::env::temp_dir().join(format!("exo-pb-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let first = install(&dir).unwrap();
        assert!(first.is_some(), "first run installs and notifies");
        let second = install(&dir).unwrap();
        assert!(second.is_none(), "second run is quiet");
        assert!(dir.join("playbook.md").exists());
        assert!(dir
            .join(format!("playbook-v{PLAYBOOK_VERSION}.md"))
            .exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// D48: the emitted (marked) form is what rides in the harness
    /// context — markers included — so the 300-word budget must hold for
    /// `marked_block`, not just the bare file.
    #[test]
    fn marked_block_within_word_bound() {
        let words = marked_block().split_whitespace().count();
        assert!(
            words <= BLOCK_WORD_LIMIT,
            "emitted instruction block is {words} words with markers; bound is {BLOCK_WORD_LIMIT}"
        );
    }

    /// D48: `--install-block` is idempotent — running it twice leaves
    /// exactly one copy (GitHub issue #7: `--dump-block >> CLAUDE.md`
    /// appended a second copy on every rerun).
    #[test]
    fn install_block_is_idempotent() {
        let once = install_block_into("project instructions\n");
        let twice = install_block_into(&once);
        assert_eq!(once, twice, "second run changes nothing");
        assert_eq!(
            once.matches(block_end_marker()).count(),
            1,
            "exactly one block region"
        );
        assert!(
            once.starts_with("project instructions\n"),
            "surrounding content is preserved"
        );
    }

    /// D48: a rerun REPLACES the stale marked region in place — the file
    /// never grows a second copy, and content after the region survives.
    #[test]
    fn install_block_replaces_the_marked_region() {
        let stale =
            "# my repo\n\n<!-- exocortex-block v0.0.1 -->\n\nold block\n<!-- /exocortex-block -->\n\ntrailing notes\n";
        let updated = install_block_into(stale);
        assert_eq!(updated.matches(block_end_marker()).count(), 1);
        assert!(!updated.contains("old block"), "stale region replaced");
        assert!(!updated.contains("v0.0.1"));
        assert!(updated.contains("trailing notes"));
        assert!(updated.contains(BLOCK.trim_end()));
    }

    /// D48: a marker-era double append heals back to one region.
    #[test]
    fn install_block_heals_double_regions() {
        let doubled = format!("{}{}", marked_block(), marked_block());
        let healed = install_block_into(&doubled);
        assert_eq!(
            healed.matches(block_end_marker()).count(),
            1,
            "one region after healing"
        );
    }
}
