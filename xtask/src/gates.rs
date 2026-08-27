use anyhow::Result;
use std::path::{Path, PathBuf};

pub(crate) const DEAD_CONTROLS: &[(&str, &str)] = &[
    ("admit_and_publish", "crates/exocortex-cluster/src/node.rs"),
    ("check_deadline", "crates/exocortex-ops/src/operations.rs"),
    ("on_write", "crates/exocortex-ingest/src/service.rs"),
    (
        "with_admin_ceilings",
        "crates/exocortex-server/src/backend.rs",
    ),
    ("drain_all", "crates/exocortex-client/src/main.rs"),
    ("advance_local_lsn", "crates/exocortex-client/src/mcp.rs"),
    ("apply_local", "crates/exocortex-client/src/mcp.rs"),
];

fn llm_markers() -> Vec<String> {
    [
        ["async-", "openai"].concat(),
        ["openai", "_api"].concat(),
        ["api.", "openai.com"].concat(),
        ["anthropic", ".com"].concat(),
        ["api.", "anthropic"].concat(),
        ["generativelanguage.", "googleapis.com"].concat(),
        ["mistral", ".ai"].concat(),
        ["cohere", ".ai"].concat(),
        ["llm", "_client"].concat(),
    ]
    .into()
}

pub(crate) fn validate_storage_targets(root: &Path) -> Result<()> {
    let base = root.join("crates/exocortex-storage");
    for target in ["integration.rs", "fencing_live.rs"] {
        anyhow::ensure!(
            base.join("tests").join(target).is_file(),
            "storage-conformance: live target tests/{target} is missing"
        );
    }
    let manifest = std::fs::read_to_string(base.join("Cargo.toml"))?;
    anyhow::ensure!(
        manifest
            .lines()
            .any(|line| line.trim() == "integration = []"),
        "storage-conformance: exocortex-storage must declare the integration feature"
    );
    Ok(())
}

pub(crate) fn dead_enforcement_violations(
    root: &Path,
    controls: &[(&str, &str)],
) -> Result<Vec<String>> {
    let mut violations = Vec::new();
    for (name, witness) in controls {
        let path = root.join(witness);
        if !path.is_file() {
            violations.push(format!("witness file {witness} for `{name}` is missing"));
            continue;
        }
        let source = std::fs::read_to_string(&path)?;
        if !contains_production_call(&source, name) {
            violations.push(format!(
                "`{name}` has no executable production call in {witness}"
            ));
        }
    }
    Ok(violations)
}

fn contains_production_call(source: &str, name: &str) -> bool {
    let source = strip_comments_and_strings(source);
    let needle = format!("{name}(");
    source.match_indices(&needle).any(|(at, _)| {
        let prefix = source[..at].trim_end();
        !prefix.ends_with("fn")
            && !prefix.ends_with("fn ")
            && !prefix.ends_with("pub fn")
            && !prefix.ends_with("async fn")
    })
}

pub(crate) fn no_llm_violations(root: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();
    for entry in ["crates", "xtask", "scripts", "proto", ".github"] {
        let path = root.join(entry);
        if path.exists() {
            walk_source_files(&path, &mut files)?;
        }
    }
    for name in ["Cargo.toml", "Dockerfile"] {
        let path = root.join(name);
        if path.is_file() {
            files.push(path);
        }
    }

    let mut violations = Vec::new();
    for path in files {
        let source = std::fs::read_to_string(&path)?;
        for marker in llm_markers() {
            if source.contains(&marker) {
                let rel = path.strip_prefix(root).unwrap_or(&path);
                violations.push(format!("{}: contains `{marker}`", rel.display()));
            }
        }
    }
    Ok(violations)
}

pub(crate) fn dependency_tree_violations(crate_name: &str, tree: &str) -> Vec<String> {
    let kernel_banned = [
        "duckdb".into(),
        "iceberg".into(),
        "delta_kernel".into(),
        "deltalake".into(),
        "aws-sdk-s3".into(),
        "aws-sdk-glue".into(),
        ["async-", "openai"].concat(),
        ["anthropic", "-sdk"].concat(),
        "reqwest".into(),
    ];
    let packages: Vec<&str> = tree.lines().skip(1).collect();
    let mut violations = Vec::new();
    if crate_name == "exocortex-kernel" {
        for banned in kernel_banned {
            if packages.iter().any(|line| package_line_is(line, &banned)) {
                violations.push(format!("banned dependency `{banned}` is reachable"));
            }
        }
    }
    if ["exocortex-adapter-sdk", "exocortex-worker"].contains(&crate_name)
        && packages
            .iter()
            .any(|line| package_line_is(line, "exocortex-kernel"))
    {
        violations.push("exocortex-kernel is reachable".into());
    }
    if crate_name == "exocortex-adapter-sdk" {
        let internal: Vec<_> = packages
            .iter()
            .filter_map(|line| package_name(line))
            .filter(|name| name.starts_with("exocortex-"))
            .collect();
        if internal != ["exocortex-wire"] {
            violations.push(format!(
                "expected only exocortex-wire, found {}",
                internal.join(", ")
            ));
        }
    }
    violations
}

fn package_line_is(line: &str, expected: &str) -> bool {
    package_name(line).is_some_and(|name| name == expected)
}

fn package_name(line: &str) -> Option<&str> {
    line.split_whitespace()
        .find(|token| token.starts_with(char::is_alphanumeric))
}

pub(crate) fn signing_hygiene_violations(root: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();
    walk_rust_files(&root.join("crates"), &mut files)?;
    let mut violations = Vec::new();
    for path in files {
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .display()
            .to_string();
        let source = std::fs::read_to_string(&path)?;
        if rel == "crates/exocortex-wire/src/signing.rs" {
            continue;
        }
        let code = strip_comments_and_strings(&source);
        for name in ["sign_batch", "compute_checksum", "canonical_checksum"] {
            if code.contains(&format!("fn {name}(")) {
                violations.push(format!("{rel}: local batch-signing function `{name}`"));
            }
        }
        if code.contains("IngestBatch")
            && (code.contains("hmac::") || code.contains("Hmac<") || code.contains("Mac::"))
        {
            violations.push(format!(
                "{rel}: combines IngestBatch with a local HMAC implementation"
            ));
        }
        if code.contains("checksum: String::new()")
            && !code.contains("prepare_batch")
            && !code.contains("canonical_checksum")
        {
            violations.push(format!(
                "{rel}: constructs a blank batch checksum without a canonical signer"
            ));
        }
    }
    Ok(violations)
}

fn walk_source_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("target" | ".git")
            ) {
                continue;
            }
            walk_source_files(&path, out)?;
        } else if matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("rs" | "toml" | "proto" | "sh" | "yml" | "yaml")
        ) {
            out.push(path);
        }
    }
    Ok(())
}

fn walk_rust_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            walk_rust_files(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
    Ok(())
}

fn strip_comments_and_strings(source: &str) -> String {
    #[derive(Clone, Copy)]
    enum State {
        Code,
        LineComment,
        BlockComment(usize),
        String,
        Char,
    }
    let bytes = source.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut state = State::Code;
    let mut i = 0;
    while i < bytes.len() {
        let current = bytes[i];
        let next = bytes.get(i + 1).copied();
        match state {
            State::Code if current == b'/' && next == Some(b'/') => {
                state = State::LineComment;
                out.extend_from_slice(b"  ");
                i += 2;
            }
            State::Code if current == b'/' && next == Some(b'*') => {
                state = State::BlockComment(1);
                out.extend_from_slice(b"  ");
                i += 2;
            }
            State::Code if current == b'"' => {
                state = State::String;
                out.push(b' ');
                i += 1;
            }
            State::Code
                if current == b'\''
                    && (bytes.get(i + 2) == Some(&b'\'')
                        || (next == Some(b'\\') && bytes.get(i + 3) == Some(&b'\''))) =>
            {
                state = State::Char;
                out.push(b' ');
                i += 1;
            }
            State::LineComment if current == b'\n' => {
                state = State::Code;
                out.push(b'\n');
                i += 1;
            }
            State::LineComment => {
                out.push(b' ');
                i += 1;
            }
            State::BlockComment(depth) if current == b'/' && next == Some(b'*') => {
                state = State::BlockComment(depth + 1);
                out.extend_from_slice(b"  ");
                i += 2;
            }
            State::BlockComment(depth) if current == b'*' && next == Some(b'/') => {
                state = if depth == 1 {
                    State::Code
                } else {
                    State::BlockComment(depth - 1)
                };
                out.extend_from_slice(b"  ");
                i += 2;
            }
            State::BlockComment(depth) => {
                state = State::BlockComment(depth);
                out.push(if current == b'\n' { b'\n' } else { b' ' });
                i += 1;
            }
            State::String | State::Char if current == b'\\' => {
                out.extend_from_slice(b"  ");
                i += 2;
            }
            State::String if current == b'"' => {
                state = State::Code;
                out.push(b' ');
                i += 1;
            }
            State::Char if current == b'\'' => {
                state = State::Code;
                out.push(b' ');
                i += 1;
            }
            State::String | State::Char => {
                out.push(if current == b'\n' { b'\n' } else { b' ' });
                i += 1;
            }
            State::Code => {
                out.push(current);
                i += 1;
            }
        }
    }
    String::from_utf8(out).expect("source started as UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "exocortex-gate-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn write(root: &Path, relative: &str, contents: &str) {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }

    #[test]
    fn storage_conformance_rejects_missing_live_target_fixture() {
        let root = fixture("storage");
        write(
            &root,
            "crates/exocortex-storage/Cargo.toml",
            "[features]\nintegration = []\n",
        );
        write(&root, "crates/exocortex-storage/tests/integration.rs", "");
        assert!(validate_storage_targets(&root).is_err());
    }

    #[test]
    fn dead_enforcement_rejects_comment_only_fixture() {
        let root = fixture("dead");
        write(
            &root,
            "crates/example/src/lib.rs",
            "fn fence() {} // fence() is definitely live\n",
        );
        let violations =
            dead_enforcement_violations(&root, &[("fence", "crates/example/src/lib.rs")]).unwrap();
        assert_eq!(violations.len(), 1);
    }

    #[test]
    fn no_llm_rejects_generated_target_fixture() {
        let root = fixture("llm");
        let endpoint = ["https://api.", "openai.com/v1"].concat();
        write(&root, "crates/example/build.rs", &endpoint);
        assert_eq!(no_llm_violations(&root).unwrap().len(), 1);
    }

    #[test]
    fn kernel_purity_rejects_banned_transitive_fixture() {
        let tree = [
            "exocortex-kernel v0.1.0",
            "└── renamed-http v1.0.0",
            "    └── reqwest v0.12.0",
        ]
        .join("\n");
        assert_eq!(
            dependency_tree_violations("exocortex-kernel", &tree),
            ["banned dependency `reqwest` is reachable"]
        );
    }

    #[test]
    fn signing_hygiene_rejects_aliased_local_hmac_fixture() {
        let root = fixture("signing");
        write(
            &root,
            "crates/example/src/lib.rs",
            "use hmac::Hmac as OtherMac; use wire::IngestBatch; fn forge(_: IngestBatch) { let _: Option<OtherMac<sha2::Sha256>> = None; }",
        );
        assert_eq!(signing_hygiene_violations(&root).unwrap().len(), 1);
    }
}
