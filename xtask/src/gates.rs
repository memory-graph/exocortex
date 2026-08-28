use anyhow::Result;
use std::path::{Path, PathBuf};

pub(crate) const DEAD_CONTROLS: &[(&str, &str, Option<&str>)] = &[
    (
        "admit_and_publish",
        "crates/exocortex-cluster/src/node.rs",
        Some("self.admit_and_publish"),
    ),
    (
        "check_deadline",
        "crates/exocortex-ops/src/operations.rs",
        Some("ctx.check_deadline"),
    ),
    (
        "on_writes_once",
        "crates/exocortex-ingest/src/service.rs",
        Some("dreams.on_writes_once"),
    ),
    (
        "new_with_admin_policies",
        "crates/exocortex-server/src/backend.rs",
        Some("IngestServer::new_with_admin_policies"),
    ),
    (
        "drain_all",
        "crates/exocortex-client/src/main.rs",
        Some("exocortex_client::drain::drain_all"),
    ),
    (
        "advance_local_lsn",
        "crates/exocortex-client/src/mcp.rs",
        Some("self.cache.advance_local_lsn"),
    ),
    (
        "apply_local",
        "crates/exocortex-client/src/mcp.rs",
        Some("self.cache.apply_local"),
    ),
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

fn manifest_dependencies(manifest: &str) -> Vec<(String, String)> {
    let mut dependencies = Vec::new();
    let mut dependency_table = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            let section = &trimmed[1..trimmed.len() - 1];
            dependency_table = section == "dependencies"
                || section == "build-dependencies"
                || section == "workspace.dependencies"
                || section.ends_with(".dependencies")
                || section.ends_with(".build-dependencies");
            continue;
        }
        if !dependency_table || trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((alias, value)) = trimmed.split_once('=') else {
            continue;
        };
        let alias = alias.trim().trim_matches(['\'', '"']).to_string();
        if alias.is_empty() {
            continue;
        }
        let package = value
            .split(',')
            .find_map(|field| {
                let (key, value) = field.split_once('=')?;
                (key.trim().trim_start_matches('{').trim() == "package")
                    .then(|| value.trim().trim_matches([' ', '\'', '"', '}']).to_string())
            })
            .filter(|package| !package.is_empty())
            .unwrap_or_else(|| alias.clone());
        dependencies.push((alias, package));
    }
    dependencies
}

fn reviewed_outbound_dependency(manifest: &str, package: &str) -> bool {
    matches!(
        (manifest, package),
        ("Cargo.toml", "eventsource-client")
            | ("Cargo.toml", "hyper")
            | ("crates/exocortex-client/Cargo.toml", "eventsource-client")
            | ("crates/exocortex-client/Cargo.toml", "hyper")
            | ("crates/exocortex-cluster/Cargo.toml", "eventsource-client")
    )
}

pub(crate) const STORAGE_LIVE_CANARIES: &[(&str, &str)] = &[
    ("integration", "roundtrip_memory"),
    ("fencing_live", "stale_lease_write_is_fenced_live"),
];

pub(crate) fn validate_storage_targets(root: &Path) -> Result<()> {
    let base = root.join("crates/exocortex-storage");
    for (target, _) in STORAGE_LIVE_CANARIES {
        anyhow::ensure!(
            base.join("tests").join(format!("{target}.rs")).is_file(),
            "storage-conformance: live target tests/{target}.rs is missing"
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

pub(crate) fn validate_storage_target_listing(
    target: &str,
    canary: &str,
    listing: &str,
) -> Result<()> {
    anyhow::ensure!(
        listing.lines().any(|line| {
            line.split_once(':')
                .is_some_and(|(name, kind)| name == canary && kind.trim() == "test")
        }),
        "storage-conformance: live target {target} did not list required canary `{canary}`; the target is empty or configured out"
    );
    Ok(())
}

pub(crate) fn dead_enforcement_violations(
    root: &Path,
    controls: &[(&str, &str, Option<&str>)],
) -> Result<Vec<String>> {
    let mut violations = Vec::new();
    for (name, witness, qualified_call) in controls {
        let path = root.join(witness);
        if !path.is_file() {
            violations.push(format!("witness file {witness} for `{name}` is missing"));
            continue;
        }
        let source = std::fs::read_to_string(&path)?;
        if !contains_reachable_production_call(&source, name, *qualified_call) {
            violations.push(format!(
                "`{name}` has no executable production call in {witness}"
            ));
        }
    }
    Ok(violations)
}

#[derive(Debug)]
struct RustFunction {
    name: String,
    body_start: usize,
    body_end: usize,
    root: bool,
    method: bool,
    configured_out: bool,
}

fn contains_reachable_production_call(
    source: &str,
    name: &str,
    qualified_call: Option<&str>,
) -> bool {
    let source = strip_comments_and_strings(source);
    let functions = rust_functions(&source);
    let mut reachable = vec![false; functions.len()];
    for (index, function) in functions.iter().enumerate() {
        reachable[index] = function.root && !function.configured_out;
    }
    loop {
        let mut changed = false;
        for caller in 0..functions.len() {
            if !reachable[caller] {
                continue;
            }
            let body = &source[functions[caller].body_start..functions[caller].body_end];
            for callee in 0..functions.len() {
                if !reachable[callee]
                    && !functions[callee].configured_out
                    && contains_call(
                        body,
                        &functions[callee].name,
                        Some(functions[callee].method),
                    )
                {
                    reachable[callee] = true;
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }

    let needle = format!("{name}(");
    let target_is_method = functions
        .iter()
        .find(|function| function.name == name)
        .map(|function| function.method);
    functions.iter().enumerate().any(|(index, function)| {
        if !reachable[index] {
            return false;
        }
        let body = &source[function.body_start..function.body_end];
        if let Some(qualified_call) = qualified_call {
            return qualified_call_offsets(body, qualified_call).any(|offset| {
                !configured_out_at(body, offset) && !after_unconditional_exit(body, offset)
            });
        }
        body.match_indices(&needle).any(|(relative, _)| {
            is_call_to(body, relative, target_is_method)
                && !configured_out_at(body, relative)
                && !after_unconditional_exit(body, relative)
        })
    })
}

fn qualified_call_offsets<'a>(body: &'a str, pattern: &'a str) -> impl Iterator<Item = usize> + 'a {
    body.char_indices().filter_map(move |(start, first)| {
        if !pattern.starts_with(first)
            || body[..start]
                .chars()
                .next_back()
                .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return None;
        }
        let mut source = body[start..].char_indices().peekable();
        for expected in pattern.chars() {
            while source
                .peek()
                .is_some_and(|(_, found)| found.is_whitespace())
            {
                source.next();
            }
            if source.next().map(|(_, found)| found) != Some(expected) {
                return None;
            }
        }
        while source
            .peek()
            .is_some_and(|(_, found)| found.is_whitespace())
        {
            source.next();
        }
        (source.next().map(|(_, found)| found) == Some('(')).then_some(start)
    })
}

fn contains_call(body: &str, name: &str, method: Option<bool>) -> bool {
    let needle = format!("{name}(");
    body.match_indices(&needle)
        .any(|(offset, _)| is_call_to(body, offset, method))
}

fn is_call_to(body: &str, offset: usize, method: Option<bool>) -> bool {
    let preceding = body[..offset]
        .bytes()
        .rev()
        .find(|byte| !byte.is_ascii_whitespace());
    match preceding {
        Some(byte) if byte == b'_' || byte.is_ascii_alphanumeric() => false,
        Some(b'.' | b':') => method != Some(false),
        _ => true,
    }
}

fn rust_functions(source: &str) -> Vec<RustFunction> {
    let mut functions = Vec::new();
    let mut cursor = 0;
    while let Some(relative) = source[cursor..].find("fn ") {
        let at = cursor + relative;
        if at > 0 && source.as_bytes()[at - 1].is_ascii_alphanumeric() {
            cursor = at + 3;
            continue;
        }
        let name_start = at + 3;
        let name_end = source[name_start..]
            .find(|character: char| !character.is_ascii_alphanumeric() && character != '_')
            .map(|offset| name_start + offset)
            .unwrap_or(source.len());
        let name = source[name_start..name_end].to_string();
        let Some(body_start) = function_body_start(source, name_end) else {
            cursor = name_end;
            continue;
        };
        let Some(body_end) = matching_brace(source, body_start) else {
            break;
        };
        let line_start = source[..at].rfind('\n').map_or(0, |line| line + 1);
        let mut attribute_start = line_start;
        while attribute_start > 0 {
            let previous_end = attribute_start - 1;
            let previous_start = source[..previous_end]
                .rfind('\n')
                .map_or(0, |line| line + 1);
            if !source[previous_start..previous_end]
                .trim()
                .starts_with("#[")
            {
                break;
            }
            attribute_start = previous_start;
        }
        let header = &source[attribute_start..body_start];
        let root = name == "main"
            || name == "handle"
            || inside_trait_impl(source, at)
            || header
                .split_whitespace()
                .any(|token| token.starts_with("pub"));
        functions.push(RustFunction {
            name,
            body_start: body_start + 1,
            body_end,
            root,
            method: inside_impl(source, at),
            configured_out: header.contains("#[cfg("),
        });
        cursor = body_start + 1;
    }
    functions
}

fn inside_impl(source: &str, function_at: usize) -> bool {
    let mut search_end = function_at;
    while let Some(impl_at) = source[..search_end].rfind("impl") {
        let before_is_ident = impl_at > 0
            && (source.as_bytes()[impl_at - 1].is_ascii_alphanumeric()
                || source.as_bytes()[impl_at - 1] == b'_');
        let after = source.as_bytes().get(impl_at + 4).copied();
        if before_is_ident || !after.is_some_and(|byte| byte.is_ascii_whitespace() || byte == b'<')
        {
            search_end = impl_at;
            continue;
        }
        let Some(open_offset) = source[impl_at..function_at].find('{') else {
            search_end = impl_at;
            continue;
        };
        let open = impl_at + open_offset;
        if matching_brace(source, open).is_some_and(|close| close > function_at) {
            return true;
        }
        search_end = impl_at;
    }
    false
}

fn function_body_start(source: &str, signature_start: usize) -> Option<usize> {
    let mut parentheses = 0usize;
    let mut brackets = 0usize;
    let mut angles = 0usize;
    for (offset, byte) in source.as_bytes()[signature_start..].iter().enumerate() {
        match byte {
            b'(' => parentheses += 1,
            b')' => parentheses = parentheses.saturating_sub(1),
            b'[' => brackets += 1,
            b']' => brackets = brackets.saturating_sub(1),
            b'<' if parentheses == 0 && brackets == 0 => angles += 1,
            b'>' if parentheses == 0 && brackets == 0 => angles = angles.saturating_sub(1),
            b';' if parentheses == 0 && brackets == 0 && angles == 0 => return None,
            b'{' if parentheses == 0 && brackets == 0 && angles == 0 => {
                return Some(signature_start + offset)
            }
            _ => {}
        }
    }
    None
}

fn inside_trait_impl(source: &str, function_at: usize) -> bool {
    let mut search_end = function_at;
    while let Some(impl_at) = source[..search_end].rfind("impl") {
        let before_is_ident = impl_at > 0
            && (source.as_bytes()[impl_at - 1].is_ascii_alphanumeric()
                || source.as_bytes()[impl_at - 1] == b'_');
        let after = source.as_bytes().get(impl_at + 4).copied();
        if before_is_ident || !after.is_some_and(|byte| byte.is_ascii_whitespace() || byte == b'<')
        {
            search_end = impl_at;
            continue;
        }
        let Some(open_offset) = source[impl_at..function_at].find('{') else {
            search_end = impl_at;
            continue;
        };
        let open = impl_at + open_offset;
        if matching_brace(source, open).is_some_and(|close| close > function_at) {
            return source[impl_at..open].contains(" for ");
        }
        search_end = impl_at;
    }
    false
}

fn matching_brace(source: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (offset, byte) in source.as_bytes()[open..].iter().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(open + offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn configured_out_at(body: &str, call: usize) -> bool {
    let prefix = &body[..call];
    let cfg = prefix.rfind("#[cfg(");
    let boundary = prefix.rfind([';', '}']).unwrap_or(0);
    cfg.is_some_and(|cfg| cfg > boundary)
}

fn after_unconditional_exit(body: &str, call: usize) -> bool {
    let prefix = &body[..call];
    let mut depth = 0usize;
    let mut depth_at_exit = None;
    for (index, byte) in prefix.as_bytes().iter().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth = depth.saturating_sub(1);
                if depth_at_exit.is_some_and(|exit_depth| exit_depth > depth) {
                    depth_at_exit = None;
                }
            }
            _ => {
                if ["return;", "break;", "continue;", "panic!(", "unreachable!("]
                    .iter()
                    .any(|marker| prefix[index..].starts_with(marker))
                {
                    depth_at_exit = Some(depth);
                }
            }
        }
    }
    depth_at_exit == Some(depth)
}

pub(crate) fn no_llm_violations(root: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();
    for entry in ["crates", "xtask", "scripts", "proto", ".github"] {
        let path = root.join(entry);
        if path.exists() {
            walk_files(
                &path,
                &mut files,
                &["rs", "toml", "proto", "sh", "yml", "yaml"],
            )?;
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
        let is_manifest = path.file_name().and_then(|name| name.to_str()) == Some("Cargo.toml");
        if !is_manifest {
            for marker in llm_markers() {
                if source.contains(&marker) {
                    let rel = path.strip_prefix(root).unwrap_or(&path);
                    violations.push(format!("{}: contains `{marker}`", rel.display()));
                }
            }
        }
        if is_manifest {
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .display()
                .to_string();
            for (alias, package) in manifest_dependencies(&source) {
                let normalized = package.replace('_', "-").to_ascii_lowercase();
                let llm_dependencies = [
                    ["async-", "openai"].concat(),
                    ["anthropic", "-sdk"].concat(),
                    ["llm", "-client"].concat(),
                    ["ollama", "-rs"].concat(),
                    ["rig", "-core"].concat(),
                    ["mistralai", "-client"].concat(),
                ];
                if llm_dependencies
                    .iter()
                    .any(|marker| normalized.contains(marker))
                {
                    violations.push(format!(
                        "{rel}: LLM dependency `{package}` (declared as `{alias}`)"
                    ));
                }
                if [
                    "reqwest",
                    "ureq",
                    "surf",
                    "isahc",
                    "awc",
                    "hyper",
                    "hyper-util",
                    "eventsource-client",
                ]
                .contains(&normalized.as_str())
                    && !reviewed_outbound_dependency(&rel, &normalized)
                {
                    violations.push(format!(
                        "{rel}: unreviewed outbound client dependency `{package}` (declared as `{alias}`)"
                    ));
                }
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
        ["async-", "openai"].concat(),
        ["anthropic", "-sdk"].concat(),
        "reqwest".into(),
    ];
    let packages: Vec<&str> = tree.lines().skip(1).collect();
    let mut violations = Vec::new();
    if crate_name == "exocortex-kernel" {
        for package in packages.iter().filter_map(|line| package_name(line)) {
            if package.starts_with("exocortex-") {
                violations.push(format!("internal dependency `{package}` is reachable"));
            }
        }
        for banned in kernel_banned {
            if packages.iter().any(|line| package_line_is(line, &banned)) {
                violations.push(format!("banned dependency `{banned}` is reachable"));
            }
        }
        for package in packages.iter().filter_map(|line| package_name(line)) {
            if package.starts_with("aws-sdk-") {
                violations.push(format!("banned dependency `{package}` is reachable"));
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

pub(crate) fn kernel_boundary_violations(root: &Path) -> Result<Vec<String>> {
    const ALLOWED_DIRECT: &[&str] = &[
        "serde",
        "serde_json",
        "bincode",
        "schemars",
        "smol_str",
        "smallvec",
        "thiserror",
        "tracing",
        "chrono",
        "uuid",
        "blake3",
        "sha2",
        "inventory",
        "lasso",
        "crepe",
    ];
    let kernel = root.join("crates/exocortex-kernel");
    let manifest = std::fs::read_to_string(kernel.join("Cargo.toml"))?;
    let mut violations = Vec::new();
    for (alias, package) in manifest_dependencies(&manifest) {
        if !ALLOWED_DIRECT.contains(&package.as_str()) {
            violations.push(format!(
                "kernel direct dependency `{package}` (declared as `{alias}`) is not pure-approved"
            ));
        }
    }
    let mut files = Vec::new();
    walk_files(&kernel.join("src"), &mut files, &["rs"])?;
    for path in files {
        let source = strip_comments_and_strings(&std::fs::read_to_string(&path)?);
        let statements = source.split(';').map(str::trim).collect::<Vec<_>>();
        let mut std_aliases = vec!["std".to_string()];
        for statement in &statements {
            for prefix in ["use std as ", "use ::std as ", "extern crate std as "] {
                if let Some(alias) = statement.strip_prefix(prefix).and_then(first_identifier) {
                    std_aliases.push(alias.to_owned());
                }
            }
            if statement.starts_with("use std::{") || statement.starts_with("use ::std::{") {
                if let Some(alias) = statement
                    .split_once("self as ")
                    .and_then(|(_, tail)| first_identifier(tail))
                {
                    std_aliases.push(alias.to_owned());
                }
            }
        }
        for marker in [
            "std::fs::",
            "std::io::",
            "std::net::",
            "std::process::",
            "tokio::",
            "async_std::",
        ] {
            if source.contains(marker) {
                violations.push(format!(
                    "{} uses I/O boundary `{marker}`",
                    path.strip_prefix(root).unwrap_or(&path).display()
                ));
            }
        }
        for alias in std_aliases.iter().filter(|alias| alias.as_str() != "std") {
            for module in ["fs", "io", "net", "process"] {
                let marker = format!("{alias}::{module}::");
                if source.contains(&marker) {
                    violations.push(format!(
                        "{} uses standard I/O through root alias `{marker}`",
                        path.strip_prefix(root).unwrap_or(&path).display()
                    ));
                }
            }
        }
        for statement in statements {
            if (statement.starts_with("use std")
                || statement.starts_with("use ::std")
                || statement.starts_with("extern crate std"))
                && statement
                    .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                    .any(|token| ["fs", "io", "net", "process"].contains(&token))
            {
                violations.push(format!(
                    "{} imports an aliased standard I/O boundary: {statement}",
                    path.strip_prefix(root).unwrap_or(&path).display()
                ));
            }
        }
    }
    Ok(violations)
}

fn first_identifier(value: &str) -> Option<&str> {
    let end = value
        .char_indices()
        .find_map(|(index, character)| {
            (!character.is_ascii_alphanumeric() && character != '_').then_some(index)
        })
        .unwrap_or(value.len());
    (end > 0).then_some(&value[..end])
}

pub(crate) fn cypher_outside_storage_violations(root: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();
    walk_files(&root.join("crates"), &mut files, &["rs"])?;
    let mut violations = Vec::new();
    for path in files {
        let rel = path.strip_prefix(root).unwrap_or(&path);
        if rel.starts_with("crates/exocortex-storage") {
            continue;
        }
        let source = std::fs::read_to_string(&path)?;
        let query = rust_string_literals(&source)
            .into_iter()
            .chain(composed_rust_string_literals(&source))
            .any(|literal| looks_like_cypher(&literal));
        if query {
            violations.push(format!(
                "{} contains executable-looking Cypher outside exocortex-storage",
                rel.display()
            ));
        }
    }
    Ok(violations)
}

/// Return strings assembled by the standard literal-composition macros. A
/// query split across `concat!`/`format!` arguments is still executable code;
/// unrelated literals elsewhere in the file must remain independent.
fn composed_rust_string_literals(source: &str) -> Vec<String> {
    let mut composed = Vec::new();
    for macro_name in ["concat!", "format!"] {
        let mut cursor = 0usize;
        while let Some(relative) = source[cursor..].find(macro_name) {
            let at = cursor + relative + macro_name.len();
            let Some(open_relative) = source[at..].find('(') else {
                break;
            };
            let open = at + open_relative;
            let Some(close) = matching_delimiter(source, open, b'(', b')') else {
                break;
            };
            composed.push(rust_string_literals(&source[open + 1..close]).join(""));
            cursor = close + 1;
        }
    }
    for suffix in ["].concat()", "].join(\"\")"] {
        let mut cursor = 0usize;
        while let Some(relative) = source[cursor..].find(suffix) {
            let close = cursor + relative;
            let Some(open) = matching_delimiter_backwards(source, close, b'[', b']') else {
                break;
            };
            composed.push(rust_string_literals(&source[open + 1..close]).join(""));
            cursor = close + suffix.len();
        }
    }
    for statement in source
        .split(';')
        .filter(|statement| statement.contains('+'))
    {
        let literals = rust_string_literals(statement);
        if literals.len() > 1 {
            composed.push(literals.join(""));
        }
    }
    composed
}

fn matching_delimiter_backwards(
    source: &str,
    close: usize,
    opening: u8,
    closing: u8,
) -> Option<usize> {
    let mut depth = 1usize;
    for index in (0..close).rev() {
        match source.as_bytes()[index] {
            byte if byte == closing => depth += 1,
            byte if byte == opening => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn matching_delimiter(source: &str, open: usize, opening: u8, closing: u8) -> Option<usize> {
    let mut depth = 0usize;
    for (offset, byte) in source.as_bytes()[open..].iter().enumerate() {
        if *byte == opening {
            depth += 1;
        } else if *byte == closing {
            depth = depth.checked_sub(1)?;
            if depth == 0 {
                return Some(open + offset);
            }
        }
    }
    None
}

fn looks_like_cypher(literal: &str) -> bool {
    let literal = literal.to_ascii_uppercase();
    literal.contains("GRAPH.QUERY")
        || literal.contains("CYPHER ")
        || (literal.contains("MATCH (")
            && [" RETURN ", " MERGE (", " CREATE (", " DELETE ", " SET "]
                .iter()
                .any(|marker| literal.contains(marker)))
}

/// Extract normal and raw Rust string literals. Inspecting each literal as a
/// unit avoids combining unrelated words from comments, diagnostics, and
/// identifiers into a query that does not exist in executable source.
fn rust_string_literals(source: &str) -> Vec<String> {
    let bytes = source.as_bytes();
    let mut literals = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'"' || (bytes[index] == b'b' && bytes.get(index + 1) == Some(&b'"')) {
            if bytes[index] == b'b' {
                index += 1;
            }
            index += 1;
            let mut literal = String::new();
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' if index + 1 < bytes.len() => {
                        literal.push(bytes[index + 1] as char);
                        index += 2;
                    }
                    b'"' => {
                        index += 1;
                        break;
                    }
                    byte => {
                        literal.push(byte as char);
                        index += 1;
                    }
                }
            }
            literals.push(literal);
            continue;
        }
        let raw_start = match bytes[index] {
            b'r' => Some(index + 1),
            b'b' if bytes.get(index + 1) == Some(&b'r') => Some(index + 2),
            _ => None,
        };
        if let Some(mut cursor) = raw_start {
            let mut hashes = 0usize;
            while bytes.get(cursor) == Some(&b'#') {
                hashes += 1;
                cursor += 1;
            }
            if bytes.get(cursor) == Some(&b'"') {
                cursor += 1;
                let content_start = cursor;
                let terminator = vec![b'#'; hashes];
                let mut closed = false;
                while cursor < bytes.len() {
                    if bytes[cursor] == b'"'
                        && bytes.get(cursor + 1..cursor + 1 + hashes) == Some(terminator.as_slice())
                    {
                        literals.push(source[content_start..cursor].to_owned());
                        index = cursor + 1 + hashes;
                        closed = true;
                        break;
                    }
                    cursor += 1;
                }
                if closed {
                    continue;
                }
            }
        }
        index += 1;
    }
    literals
}

pub(crate) fn kernel_pack_coupling_violations(root: &Path) -> Result<Vec<String>> {
    let kernel = root.join("crates/exocortex-kernel");
    let mut files = vec![kernel.join("Cargo.toml")];
    walk_files(&kernel.join("src"), &mut files, &["rs"])?;
    let mut violations = Vec::new();
    for path in files {
        let source = std::fs::read_to_string(&path)?;
        for needle in ["exocortex-pack-dev-v1", "exocortex_pack_dev_v1"] {
            if source.contains(needle) {
                violations.push(format!(
                    "{} names the dev-v1 pack directly",
                    path.strip_prefix(root).unwrap_or(&path).display()
                ));
            }
        }
    }
    Ok(violations)
}

pub(crate) fn validate_acceptance_matrix(root: &Path) -> Result<()> {
    const HEADER: &str = "criterion\tstatus\trequirement\texecutable_evidence\tcommand\ttracking";
    let path = root.join("docs/acceptance/section-23.tsv");
    let matrix = std::fs::read_to_string(&path)?;
    let mut lines = matrix.lines();
    anyhow::ensure!(
        lines.next() == Some(HEADER),
        "acceptance matrix header is malformed"
    );
    let plan = std::fs::read_to_string(root.join("docs/master-plan.prd"))?;
    let mut seen = std::collections::BTreeSet::new();

    for (offset, line) in lines.enumerate() {
        let line_number = offset + 2;
        anyhow::ensure!(
            !line.trim().is_empty(),
            "blank matrix row at line {line_number}"
        );
        let columns: Vec<_> = line.split('\t').collect();
        anyhow::ensure!(
            columns.len() == 6,
            "matrix line {line_number} must have six tab-separated columns"
        );
        let criterion: u8 = columns[0].parse().map_err(|_| {
            anyhow::anyhow!("matrix line {line_number} has a non-numeric criterion")
        })?;
        anyhow::ensure!(
            (1..=30).contains(&criterion),
            "criterion {criterion} is outside §23"
        );
        anyhow::ensure!(
            seen.insert(criterion),
            "criterion {criterion} is duplicated"
        );
        anyhow::ensure!(
            !columns[2].trim().is_empty(),
            "criterion {criterion} has no requirement text"
        );

        match columns[1] {
            "verified" => {
                anyhow::ensure!(
                    columns[5] == "-",
                    "verified criterion {criterion} must not carry tracking"
                );
                anyhow::ensure!(
                    columns[3] != "-" && columns[4] != "-",
                    "verified criterion {criterion} needs executable evidence and a command"
                );
            }
            "partial-deferred" | "deferred" | "partial-gap" | "gap" => {
                anyhow::ensure!(
                    columns[5] != "-",
                    "{} criterion {criterion} needs a tracked plan row",
                    columns[1]
                );
                let tracking_id = columns[5].split_whitespace().next().unwrap_or_default();
                anyhow::ensure!(plan.contains(&format!("| {tracking_id} |")), "criterion {criterion} tracking id {tracking_id} is absent from the master plan");
                if columns[1] == "gap" {
                    anyhow::ensure!(
                        columns[3] == "-" && columns[4] == "-",
                        "gap criterion {criterion} must not claim executable evidence"
                    );
                } else if matches!(columns[1], "partial-deferred" | "partial-gap") {
                    anyhow::ensure!(
                        columns[3] != "-" && columns[4] != "-",
                        "partial criterion {criterion} needs evidence for its completed portion"
                    );
                }
            }
            other => anyhow::bail!("criterion {criterion} has unknown status `{other}`"),
        }

        if columns[3] != "-" {
            for locator in columns[3].split(';') {
                let (relative, needle) = locator.split_once("::").ok_or_else(|| {
                    anyhow::anyhow!(
                        "criterion {criterion} evidence `{locator}` is not path::symbol"
                    )
                })?;
                let source = std::fs::read_to_string(root.join(relative)).map_err(|_| {
                    anyhow::anyhow!("criterion {criterion} evidence file `{relative}` is missing")
                })?;
                let searchable = if relative.ends_with(".rs") {
                    strip_comments_and_strings(&source)
                } else if is_shell_source(relative, &source) {
                    active_shell_commands(&source).join("\n")
                } else {
                    source.clone()
                };
                anyhow::ensure!(
                    searchable.contains(needle),
                    "criterion {criterion} evidence symbol `{needle}` is absent from {relative}"
                );
                validate_executable_evidence(
                    root, criterion, relative, needle, &source, columns[3], columns[4],
                )?;
            }
        }
    }
    anyhow::ensure!(
        seen.len() == 30,
        "acceptance matrix covers {} of 30 criteria",
        seen.len()
    );
    Ok(())
}

fn validate_executable_evidence(
    root: &Path,
    criterion: u8,
    relative: &str,
    needle: &str,
    source: &str,
    all_evidence: &str,
    command: &str,
) -> Result<()> {
    if is_shell_source(relative, source) {
        let active = active_shell_commands(source).join("\n");
        anyhow::ensure!(
            active.contains(needle),
            "criterion {criterion} evidence `{relative}::{needle}` exists only in shell comments or inert text"
        );
        anyhow::ensure!(
            shell_evidence_is_executed(root, relative, all_evidence, command)?,
            "criterion {criterion} shell evidence `{relative}::{needle}` is not exercised or inspected by `{command}`"
        );
        return Ok(());
    }
    if !relative.ends_with(".rs") {
        return Ok(());
    }
    let clean_source = strip_comments_and_strings(source);
    let symbol = needle
        .strip_prefix("fn ")
        .unwrap_or(needle)
        .split(|character: char| character == '(' || character.is_whitespace())
        .next()
        .unwrap_or_default();
    let function_pattern = format!("fn {symbol}");
    let function_offset =
        clean_source
            .match_indices(&function_pattern)
            .find_map(|(offset, text)| {
                let end = offset + text.len();
                (!clean_source
                    .as_bytes()
                    .get(end)
                    .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_'))
                .then_some(offset)
            });
    let Some(function_offset) = function_offset else {
        // Some Rust evidence is deliberately a constant, method call, or
        // invariant-bearing expression rather than a test function.
        anyhow::ensure!(
            !(relative.starts_with("tests/") || relative.contains("/tests/")),
            "criterion {criterion} evidence `{relative}::{needle}` is not an executable test"
        );
        return Ok(());
    };
    let prefix = &source[..function_offset];
    let declaration_line_start = prefix.rfind('\n').map_or(0, |line| line + 1);
    let attribute_prefix = &prefix[..declaration_line_start];
    let function_attributes = attribute_prefix
        .lines()
        .rev()
        .skip_while(|line| line.trim().is_empty())
        .take_while(|line| line.trim_start().starts_with("#["))
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n");
    let is_test =
        function_attributes.contains("#[test") || function_attributes.contains("#[tokio::test");
    anyhow::ensure!(
        !(relative.starts_with("tests/") || relative.contains("/tests/")) || is_test,
        "criterion {criterion} evidence `{relative}::{needle}` is not an executable test"
    );
    if !is_test {
        return Ok(());
    }
    anyhow::ensure!(
        !function_attributes.contains("#[ignore"),
        "criterion {criterion} evidence `{relative}::{needle}` is ignored"
    );
    let file_attributes = source
        .lines()
        .take_while(|line| {
            let line = line.trim_start();
            line.is_empty() || line.starts_with("//") || line.starts_with("#![")
        })
        .filter(|line| line.trim_start().starts_with("#![cfg"))
        .collect::<Vec<_>>()
        .join("\n");
    for configuration in [file_attributes.as_str(), function_attributes.as_str()]
        .into_iter()
        .filter(|attributes| attributes.contains("cfg"))
    {
        anyhow::ensure!(
            command_enables_configuration(command, configuration),
            "criterion {criterion} evidence `{relative}::{needle}` is configured out by `{configuration}`"
        );
    }
    let target = std::path::Path::new(relative)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or_default();
    anyhow::ensure!(
        command_executes_test(command, relative, target, symbol),
        "criterion {criterion} command does not execute evidence `{relative}::{needle}`"
    );
    Ok(())
}

fn shell_evidence_is_executed(
    root: &Path,
    relative: &str,
    all_evidence: &str,
    command: &str,
) -> Result<bool> {
    let tokens = command
        .split_whitespace()
        .map(|token| token.trim_matches(['\'', '"']))
        .collect::<Vec<_>>();
    if tokens.first().is_some_and(|token| *token == relative)
        || tokens
            .windows(2)
            .any(|pair| matches!(pair[0], "sh" | "bash") && pair[1] == relative)
    {
        return Ok(true);
    }
    let Some(position) = tokens.iter().position(|token| *token == "xtask") else {
        return Ok(false);
    };
    let Some(gate) = tokens.get(position + 1) else {
        return Ok(false);
    };
    let function = gate.replace('-', "_");
    let locator = format!("xtask/src/main.rs::fn {function}()");
    if !all_evidence.split(';').any(|item| item == locator) {
        return Ok(false);
    }
    let source = std::fs::read_to_string(root.join("xtask/src/main.rs"))?;
    let pattern = format!("fn {function}");
    let Some(start) = source.find(&pattern) else {
        return Ok(false);
    };
    let Some(open) = function_body_start(&source, start + pattern.len()) else {
        return Ok(false);
    };
    let Some(close) = matching_brace(&source, open) else {
        return Ok(false);
    };
    let body = &source[open..=close];
    let clean = strip_comments_and_strings(body);
    Ok(["read_to_string", "Command::new", "include_str!"]
        .into_iter()
        .any(|call| {
            let needle = format!("{call}(\"{relative}\")");
            body.match_indices(&needle).any(|(offset, _)| {
                clean[offset..]
                    .get(..call.len())
                    .is_some_and(|candidate| candidate == call)
            })
        }))
}

fn is_shell_source(relative: &str, source: &str) -> bool {
    relative.ends_with(".sh")
        || source
            .lines()
            .next()
            .is_some_and(|line| line.starts_with("#!") && line.contains("sh"))
}

pub(crate) fn active_shell_commands(script: &str) -> Vec<String> {
    fn without_comment(line: &str) -> &str {
        let mut single = false;
        let mut double = false;
        let mut escaped = false;
        for (index, character) in line.char_indices() {
            if escaped {
                escaped = false;
                continue;
            }
            match character {
                '\\' if !single => escaped = true,
                '\'' if !double => single = !single,
                '"' if !single => double = !double,
                '#' if !single
                    && !double
                    && line[..index]
                        .chars()
                        .next_back()
                        .is_none_or(char::is_whitespace) =>
                {
                    return &line[..index];
                }
                _ => {}
            }
        }
        line
    }

    fn heredocs(command: &str) -> std::collections::VecDeque<(String, bool)> {
        let bytes = command.as_bytes();
        let mut found = std::collections::VecDeque::new();
        let (mut cursor, mut single, mut double, mut escaped) = (0, false, false, false);
        while cursor + 1 < bytes.len() {
            let byte = bytes[cursor];
            if escaped {
                escaped = false;
                cursor += 1;
                continue;
            }
            match byte {
                b'\\' if !single => escaped = true,
                b'\'' if !double => single = !single,
                b'"' if !single => double = !double,
                b'<' if !single && !double && bytes.get(cursor + 1) == Some(&b'<') => {
                    if bytes.get(cursor + 2) == Some(&b'<') {
                        cursor += 3;
                        continue;
                    }
                    let mut token_start = cursor + 2;
                    let strip_tabs = bytes.get(token_start) == Some(&b'-');
                    if strip_tabs {
                        token_start += 1;
                    }
                    while bytes.get(token_start).is_some_and(u8::is_ascii_whitespace) {
                        token_start += 1;
                    }
                    let quote = bytes
                        .get(token_start)
                        .copied()
                        .filter(|byte| matches!(byte, b'\'' | b'"'));
                    if quote.is_some() {
                        token_start += 1;
                    }
                    let mut token_end = token_start;
                    while let Some(current) = bytes.get(token_end) {
                        if quote.map_or_else(|| current.is_ascii_whitespace(), |q| *current == q) {
                            break;
                        }
                        token_end += 1;
                    }
                    if token_end > token_start {
                        found.push_back((command[token_start..token_end].to_owned(), strip_tabs));
                    }
                    cursor = token_end + usize::from(quote.is_some());
                    continue;
                }
                _ => {}
            }
            cursor += 1;
        }
        found
    }

    let mut commands = Vec::new();
    let mut current = String::new();
    let mut heredoc_bodies = std::collections::VecDeque::new();
    for line in script.lines() {
        if let Some((delimiter, strip_tabs)) = heredoc_bodies.front() {
            let candidate = if *strip_tabs {
                line.trim_start_matches('\t')
            } else {
                line
            };
            if candidate == delimiter {
                heredoc_bodies.pop_front();
            }
            continue;
        }
        let line = without_comment(line).trim();
        if line.is_empty() || line.starts_with("#!") {
            continue;
        }
        let continued = line.ends_with('\\');
        let fragment = line.strip_suffix('\\').unwrap_or(line).trim_end();
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(fragment);
        if !continued {
            let command = std::mem::take(&mut current);
            heredoc_bodies = heredocs(&command);
            commands.push(command);
        }
    }
    if !current.is_empty() {
        commands.push(current);
    }
    commands
}

fn command_executes_test(command: &str, relative: &str, target: &str, symbol: &str) -> bool {
    let expected_package = relative
        .strip_prefix("crates/")
        .and_then(|path| path.split('/').next());
    command.split("&&").any(|segment| {
        let tokens = segment.split_whitespace().collect::<Vec<_>>();
        if !tokens.windows(2).any(|pair| pair == ["cargo", "test"]) {
            return false;
        }
        if let Some(package) = expected_package {
            let package_selected = tokens
                .windows(2)
                .any(|pair| (pair[0] == "-p" || pair[0] == "--package") && pair[1] == package)
                || tokens.iter().any(|token| {
                    token
                        .strip_prefix("--package=")
                        .is_some_and(|value| value == package)
                });
            if !package_selected && !tokens.contains(&"--workspace") {
                return false;
            }
        }
        let exact_filter = tokens.iter().any(|token| *token == symbol);
        let exact_target = tokens
            .windows(2)
            .any(|pair| pair[0] == "--test" && pair[1] == target)
            || tokens
                .iter()
                .any(|token| token.strip_prefix("--test=") == Some(target));
        exact_filter || exact_target
    })
}

fn command_enables_configuration(command: &str, configuration: &str) -> bool {
    if configuration.contains("cfg(test)") && !command.contains("cargo test") {
        return false;
    }
    let mut remainder = configuration;
    let mut saw_known_condition = configuration.contains("cfg(test)");
    while let Some(feature_start) = remainder.find("feature") {
        remainder = &remainder[feature_start + "feature".len()..];
        let Some(quote) = remainder.find('"') else {
            return false;
        };
        remainder = &remainder[quote + 1..];
        let Some(end_quote) = remainder.find('"') else {
            return false;
        };
        let feature = &remainder[..end_quote];
        saw_known_condition = true;
        if !command_enables_feature(command, feature) {
            return false;
        }
        remainder = &remainder[end_quote + 1..];
    }
    // Platform, negated, and arbitrary predicate gates cannot be proved by a
    // cargo-test command string, so acceptance evidence fails closed.
    saw_known_condition
        && !configuration.contains("not(")
        && !["target_", "unix", "windows", "debug_assertions"]
            .iter()
            .any(|predicate| configuration.contains(predicate))
}

fn command_enables_feature(command: &str, expected: &str) -> bool {
    if command
        .split_whitespace()
        .any(|token| token == "--all-features")
    {
        return true;
    }
    let tokens = command.split_whitespace().collect::<Vec<_>>();
    tokens.iter().enumerate().any(|(index, token)| {
        let selected = token.strip_prefix("--features=").or_else(|| {
            (*token == "--features")
                .then(|| tokens.get(index + 1).copied())
                .flatten()
        });
        selected.is_some_and(|features| {
            features
                .split(',')
                .flat_map(str::split_whitespace)
                .any(|feature| feature == expected)
        })
    })
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
    walk_files(&root.join("crates"), &mut files, &["rs"])?;
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
        if code.contains("hmac::") || code.contains("Hmac<") || code.contains("Mac>::") {
            violations.push(format!(
                "{rel}: local HMAC implementation outside exocortex-wire::signing"
            ));
        }
        for name in ["sign_batch", "compute_checksum", "canonical_checksum"] {
            if code.contains(&format!("fn {name}(")) {
                violations.push(format!("{rel}: local batch-signing function `{name}`"));
            }
        }
        let functions = rust_functions(&code);
        let signer_functions: std::collections::BTreeSet<_> = functions
            .iter()
            .filter(|function| {
                let body = &code[function.body_start..function.body_end];
                body.contains("prepare_batch(")
                    || body.contains("canonical_checksum(")
                    || body.contains("sign_batch(")
            })
            .map(|function| function.name.as_str())
            .collect();
        for function in &functions {
            let body = &code[function.body_start..function.body_end];
            let constructs_blank = body.contains("checksum: String::new()");
            let submits_batch = [".submit(", "submit_batch(", "commit_ingest("]
                .iter()
                .any(|call| body.contains(call));
            let signs_batch = body.contains("prepare_batch(")
                || body.contains("canonical_checksum(")
                || body.contains("sign_batch(")
                || signer_functions
                    .iter()
                    .any(|name| body.contains(&format!("{name}(")));
            if constructs_blank && submits_batch && !signs_batch {
                violations.push(format!(
                    "{rel}: function `{}` submits a blank batch checksum without a canonical signer",
                    function.name
                ));
            }
        }
    }
    let mut manifests = Vec::new();
    walk_files(&root.join("crates"), &mut manifests, &["toml"])?;
    for path in manifests {
        let rel = path.strip_prefix(root).unwrap_or(&path);
        if rel == Path::new("crates/exocortex-wire/Cargo.toml") {
            continue;
        }
        let source = std::fs::read_to_string(&path)?;
        for (alias, package) in manifest_dependencies(&source) {
            if package.replace('_', "-") == "hmac" {
                violations.push(format!(
                    "{} declares HMAC dependency `{alias}` outside exocortex-wire",
                    rel.display()
                ));
            }
        }
    }
    Ok(violations)
}

fn walk_files(dir: &Path, out: &mut Vec<PathBuf>, extensions: &[&str]) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
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
            walk_files(&path, out, extensions)?;
        } else if path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| extensions.contains(&ext))
        {
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
        RawString(usize),
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
            State::Code if current == b'r' || (current == b'b' && next == Some(b'r')) => {
                let prefix = if current == b'b' { 2 } else { 1 };
                let mut cursor = i + prefix;
                while bytes.get(cursor) == Some(&b'#') {
                    cursor += 1;
                }
                if bytes.get(cursor) == Some(&b'"') {
                    let hashes = cursor - (i + prefix);
                    state = State::RawString(hashes);
                    out.resize(out.len() + cursor - i + 1, b' ');
                    i = cursor + 1;
                } else {
                    out.push(current);
                    i += 1;
                }
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
            State::RawString(hashes) if current == b'"' => {
                let closes = (0..hashes).all(|hash| bytes.get(i + 1 + hash) == Some(&b'#'));
                if closes {
                    state = State::Code;
                    out.resize(out.len() + hashes + 1, b' ');
                    i += hashes + 1;
                } else {
                    out.push(b' ');
                    i += 1;
                }
            }
            State::RawString(hashes) => {
                state = State::RawString(hashes);
                out.push(if current == b'\n' { b'\n' } else { b' ' });
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
    fn storage_conformance_rejects_missing_empty_and_configured_out_live_targets() {
        let root = fixture("storage");
        write(
            &root,
            "crates/exocortex-storage/Cargo.toml",
            "[features]\nintegration = []\n",
        );
        write(&root, "crates/exocortex-storage/tests/integration.rs", "");
        assert!(validate_storage_targets(&root).is_err());

        write(&root, "crates/exocortex-storage/tests/fencing_live.rs", "");
        assert!(validate_storage_targets(&root).is_ok());
        assert!(validate_storage_target_listing("integration", "roundtrip_memory", "").is_err());
        assert!(validate_storage_target_listing(
            "integration",
            "roundtrip_memory",
            "configured_out_helper: test\n"
        )
        .is_err());
        assert!(validate_storage_target_listing(
            "integration",
            "roundtrip_memory",
            "roundtrip_memory: test\n"
        )
        .is_ok());
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
            dead_enforcement_violations(&root, &[("fence", "crates/example/src/lib.rs", None)])
                .unwrap();
        assert_eq!(violations.len(), 1);

        write(
            &root,
            "crates/example/src/lib.rs",
            "pub fn root(x: &Guard) { x.fence(); } fn fence() {}\n",
        );
        let violations =
            dead_enforcement_violations(&root, &[("fence", "crates/example/src/lib.rs", None)])
                .unwrap();
        assert_eq!(
            violations.len(),
            1,
            "an unrelated method call must not make a same-named free control reachable"
        );

        write(
            &root,
            "crates/example/src/lib.rs",
            "struct Guard; impl Guard { fn fence(&self) {} } struct Other; impl Other { fn fence(&self) {} } pub fn root(other: &Other) { other.fence(); }\n",
        );
        assert_eq!(
            dead_enforcement_violations(
                &root,
                &[("fence", "crates/example/src/lib.rs", Some("guard.fence"),)],
            )
            .unwrap()
            .len(),
            1,
            "an unrelated receiver must not make a same-named method control reachable"
        );
        write(
            &root,
            "crates/example/src/lib.rs",
            "struct Guard; impl Guard { fn fence(&self) {} } pub fn root(guard: &Guard) { guard.fence(); }\n",
        );
        assert!(dead_enforcement_violations(
            &root,
            &[("fence", "crates/example/src/lib.rs", Some("guard.fence"),)],
        )
        .unwrap()
        .is_empty());

        for source in [
            "struct Guard; impl Guard { fn fence(&self) {} } pub fn root(guard: &Guard) { return; guard.fence(); }\n",
            "struct Guard; impl Guard { fn fence(&self) {} } pub fn root(guard: &Guard) { #[cfg(any())] guard.fence(); }\n",
        ] {
            write(&root, "crates/example/src/lib.rs", source);
            assert_eq!(
                dead_enforcement_violations(
                    &root,
                    &[(
                        "fence",
                        "crates/example/src/lib.rs",
                        Some("guard.fence"),
                    )],
                )
                .unwrap()
                .len(),
                1,
                "qualified witnesses must still be executable and reachable"
            );
        }
    }

    #[test]
    fn dead_control_manifest_tracks_the_idempotent_dreams_boundary() {
        assert!(DEAD_CONTROLS.contains(&(
            "on_writes_once",
            "crates/exocortex-ingest/src/service.rs",
            Some("dreams.on_writes_once")
        )));
        assert!(!DEAD_CONTROLS
            .iter()
            .any(|(name, _, _)| *name == "on_writes"));
    }

    #[test]
    fn dead_enforcement_rejects_unreachable_configured_out_and_uncalled_witnesses() {
        for (name, source) in [
            (
                "unreachable",
                "pub fn root() { return; fence(); }\nfn fence() {}\n",
            ),
            (
                "configured-out",
                "pub fn root() {}\n#[cfg(any())]\nfn disabled() { fence(); }\nfn fence() {}\n",
            ),
            (
                "uncalled",
                "pub fn root() {}\nfn abandoned() { fence(); }\nfn fence() {}\n",
            ),
        ] {
            let root = fixture(name);
            write(&root, "crates/example/src/lib.rs", source);
            assert_eq!(
                dead_enforcement_violations(
                    &root,
                    &[("fence", "crates/example/src/lib.rs", None)],
                )
                    .unwrap()
                    .len(),
                1,
                "{name} witness must fail closed"
            );
        }

        let root = fixture("reachable");
        write(
            &root,
            "crates/example/src/lib.rs",
            "pub fn root() { helper(); }\nfn helper() { fence(); }\nfn fence() {}\n",
        );
        assert!(dead_enforcement_violations(
            &root,
            &[("fence", "crates/example/src/lib.rs", None)],
        )
        .unwrap()
        .is_empty());

        let root = fixture("trait-entrypoint");
        write(
            &root,
            "crates/example/src/lib.rs",
            "trait Api { fn submit(&self); } struct Server; impl Api for Server { fn submit(&self) { fence(); } } fn fence() {}\n",
        );
        assert!(
            dead_enforcement_violations(&root, &[("fence", "crates/example/src/lib.rs", None)],)
                .unwrap()
                .is_empty(),
            "public trait implementations are production entrypoints"
        );

        let root = fixture("array-signature");
        write(
            &root,
            "crates/example/src/lib.rs",
            "pub fn root() { helper([0; 32]); }\nfn helper(_key: [u8; 32]) { fence(); }\nfn fence() {}\n",
        );
        assert!(
            dead_enforcement_violations(&root, &[("fence", "crates/example/src/lib.rs", None)],)
                .unwrap()
                .is_empty(),
            "array-type semicolons are not function declaration terminators"
        );

        let root = fixture("const-generic-signature");
        write(
            &root,
            "crates/example/src/lib.rs",
            "struct Marker<const N: usize>;\npub fn root() { let _ = helper(); }\nfn helper() -> Marker<{ fence() }> { Marker }\nconst fn fence() -> usize { 1 }\n",
        );
        assert_eq!(
            dead_enforcement_violations(&root, &[("fence", "crates/example/src/lib.rs", None)],)
                .unwrap()
                .len(),
            1,
            "a const-generic expression in a signature is not a runtime enforcement call"
        );
    }

    #[test]
    fn no_llm_rejects_generated_target_fixture() {
        let root = fixture("llm");
        let endpoint = ["https://api.", "openai.com/v1"].concat();
        write(&root, "crates/example/build.rs", &endpoint);
        assert_eq!(no_llm_violations(&root).unwrap().len(), 1);
    }

    #[test]
    fn no_llm_rejects_provider_neutral_and_renamed_clients() {
        let root = fixture("provider-neutral");
        write(
            &root,
            "crates/example/Cargo.toml",
            "[dependencies]\nweb = { package = \"reqwest\", version = \"1\" }\n",
        );
        write(
            &root,
            "crates/example/src/lib.rs",
            "fn send(url: &str) { let _ = web::Client::new().post(url); }\n",
        );
        assert_eq!(no_llm_violations(&root).unwrap().len(), 1);

        let root = fixture("renamed-llm");
        let package = ["async-", "openai"].concat();
        write(
            &root,
            "crates/example/Cargo.toml",
            &format!("[dependencies]\nbrain = {{ package = \"{package}\", version = \"1\" }}\n"),
        );
        assert_eq!(no_llm_violations(&root).unwrap().len(), 1);
    }

    #[test]
    fn no_llm_accepts_the_reviewed_sse_client_dependency() {
        let root = fixture("reviewed-outbound");
        write(
            &root,
            "Cargo.toml",
            "[workspace.dependencies]\neventsource-client = \"1\"\nhyper = \"1\"\n",
        );
        write(
            &root,
            "crates/exocortex-client/Cargo.toml",
            "[dependencies]\neventsource-client = { workspace = true }\nhyper = { workspace = true }\n",
        );
        assert!(no_llm_violations(&root).unwrap().is_empty());

        write(
            &root,
            "crates/example/Cargo.toml",
            "[dependencies]\nhyper = { workspace = true }\n",
        );
        assert_eq!(
            no_llm_violations(&root).unwrap(),
            ["crates/example/Cargo.toml: unreviewed outbound client dependency `hyper` (declared as `hyper`)"],
        );
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
    fn kernel_boundary_rejects_unknown_dependencies_and_standard_io() {
        let root = fixture("kernel-boundary");
        write(
            &root,
            "crates/exocortex-kernel/Cargo.toml",
            "[dependencies]\nserde = \"1\"\nexocortex-storage = \"1\"\n",
        );
        write(
            &root,
            "crates/exocortex-kernel/src/lib.rs",
            "fn leak() { let _ = std::fs::read(\"x\"); }\n",
        );
        let violations = kernel_boundary_violations(&root).unwrap();
        assert_eq!(violations.len(), 2, "{violations:?}");
    }

    #[test]
    fn kernel_boundary_rejects_target_aliases_and_aliased_io() {
        let root = fixture("kernel-target-alias");
        write(
            &root,
            "crates/exocortex-kernel/Cargo.toml",
            "[target.'cfg(unix)'.dependencies]\nwire = { package = \"exocortex-storage\", version = \"1\" }\n",
        );
        write(
            &root,
            "crates/exocortex-kernel/src/lib.rs",
            "use std::fs as disk; fn leak() { let _ = disk::read(\"x\"); }\n",
        );
        let violations = kernel_boundary_violations(&root).unwrap();
        assert_eq!(violations.len(), 2, "{violations:?}");

        write(
            &root,
            "crates/exocortex-kernel/src/lib.rs",
            "use std as system; fn leak() { let _ = system::fs::read(\"x\"); }\n",
        );
        let violations = kernel_boundary_violations(&root).unwrap();
        assert_eq!(violations.len(), 2, "{violations:?}");

        write(
            &root,
            "crates/exocortex-kernel/src/lib.rs",
            "use ::std::fs as disk; fn leak() { let _ = disk::read(\"x\"); }\n",
        );
        let violations = kernel_boundary_violations(&root).unwrap();
        assert_eq!(violations.len(), 2, "{violations:?}");
    }

    #[test]
    fn cypher_guard_rejects_query_outside_storage() {
        let root = fixture("cypher-boundary");
        write(
            &root,
            "crates/exocortex-server/src/lib.rs",
            "fn query() -> &'static str { \"MATCH (n) RETURN n\" }\n",
        );
        write(
            &root,
            "crates/exocortex-client/src/lib.rs",
            "// MATCH ( is documentation; unrelated strings must not combine.\nconst A: &str = \"MATCH (\"; const B: &str = \" RETURN \";\n",
        );
        assert_eq!(cypher_outside_storage_violations(&root).unwrap().len(), 1);
        write(
            &root,
            "crates/exocortex-server/src/lib.rs",
            "fn query() -> String { [\"MATCH (n)\", \" RETURN n\"].concat() }\n",
        );
        assert_eq!(cypher_outside_storage_violations(&root).unwrap().len(), 1);
        write(
            &root,
            "crates/exocortex-server/src/lib.rs",
            "fn query() -> &'static str { concat!(\"MATCH (n)\", \" RETURN n\") }\n",
        );
        assert_eq!(cypher_outside_storage_violations(&root).unwrap().len(), 1);
        write(
            &root,
            "crates/exocortex-storage/src/lib.rs",
            "fn query() -> &'static str { \"MATCH (n) RETURN n\" }\n",
        );
        std::fs::remove_file(root.join("crates/exocortex-server/src/lib.rs")).unwrap();
        assert!(cypher_outside_storage_violations(&root).unwrap().is_empty());
    }

    #[test]
    fn kernel_purity_rejects_every_aws_sdk_crate_but_allows_worker_readers() {
        let kernel_tree = [
            "exocortex-kernel v0.1.0",
            "└── renamed-table-reader v1.0.0",
            "    └── aws-sdk-athena v1.2.3",
        ]
        .join("\n");
        assert_eq!(
            dependency_tree_violations("exocortex-kernel", &kernel_tree),
            ["banned dependency `aws-sdk-athena` is reachable"]
        );

        let worker_tree = [
            "exocortex-worker v0.1.0",
            "├── duckdb v1.0.0",
            "└── aws-sdk-athena v1.2.3",
        ]
        .join("\n");
        assert!(dependency_tree_violations("exocortex-worker", &worker_tree).is_empty());
    }

    #[test]
    fn kernel_pack_independence_rejects_a_direct_dev_v1_name_fixture() {
        let root = fixture("kernel-pack-name");
        write(
            &root,
            "crates/exocortex-kernel/Cargo.toml",
            "[dependencies]\nexocortex-pack-dev-v1 = \"1\"\n",
        );
        write(
            &root,
            "crates/exocortex-kernel/src/lib.rs",
            "use exocortex_pack_dev_v1::MemoryType;\n",
        );
        assert_eq!(kernel_pack_coupling_violations(&root).unwrap().len(), 2);
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

    #[test]
    fn signing_hygiene_rejects_renamed_hmac_dependency() {
        let root = fixture("signing-manifest-alias");
        write(
            &root,
            "crates/example/Cargo.toml",
            "[dependencies]\ncrypto = { package = \"hmac\", version = \"1\" }\n",
        );
        assert_eq!(signing_hygiene_violations(&root).unwrap().len(), 1);
    }

    #[test]
    fn signing_hygiene_does_not_accept_an_unrelated_signer() {
        let root = fixture("signing-control-flow");
        write(
            &root,
            "crates/example/src/lib.rs",
            "fn submit(client: Client) { let batch = IngestBatch { checksum: String::new() }; client.submit(batch); } fn unrelated(mut batch: IngestBatch) { prepare_batch(&[0; 32], &mut batch); }",
        );
        assert_eq!(signing_hygiene_violations(&root).unwrap().len(), 1);
    }

    #[test]
    fn acceptance_coverage_rejects_missing_and_stale_evidence() {
        let root = fixture("acceptance");
        write(
            &root,
            "docs/master-plan.prd",
            "| R6-B30-14 | missing stamp |\n",
        );
        write(&root, "tests/direct.rs", "#[test]\nfn direct_case() {}\n");
        let header = "criterion\tstatus\trequirement\texecutable_evidence\tcommand\ttracking\n";
        let mut rows = String::from(header);
        for criterion in 1..=30 {
            rows.push_str(&format!(
                "{criterion}\tverified\trequirement {criterion}\ttests/direct.rs::direct_case\tcargo test direct_case\t-\n"
            ));
        }
        write(&root, "docs/acceptance/section-23.tsv", &rows);
        assert!(validate_acceptance_matrix(&root).is_ok());

        write(
            &root,
            "scripts/check",
            "#!/bin/sh\n# shell_evidence exists only in a comment\ntrue\n",
        );
        let shell_rows = rows.replacen(
            "tests/direct.rs::direct_case\tcargo test direct_case",
            "scripts/check::shell_evidence\tsh scripts/check",
            1,
        );
        write(&root, "docs/acceptance/section-23.tsv", &shell_rows);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "comment-only shell text must not satisfy executable evidence"
        );
        write(
            &root,
            "scripts/check",
            "#!/bin/sh\nshell_evidence=1\ntest \"$shell_evidence\" = 1\n",
        );
        assert!(validate_acceptance_matrix(&root).is_ok());
        let unrelated_shell = shell_rows.replace("sh scripts/check", "sh scripts/unrelated");
        write(&root, "docs/acceptance/section-23.tsv", &unrelated_shell);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "the recorded command must execute or inspect the cited shell artifact"
        );
        let mentioned_shell = shell_rows.replace("sh scripts/check", "printf scripts/check");
        write(&root, "docs/acceptance/section-23.tsv", &mentioned_shell);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "a textual path argument is not execution"
        );
        write(
            &root,
            "scripts/check",
            "#!/bin/sh\ncat <<'EOF'\nshell_evidence\nEOF\n",
        );
        write(&root, "docs/acceptance/section-23.tsv", &shell_rows);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "heredoc body data must not satisfy executable shell evidence"
        );
        write(
            &root,
            "scripts/check",
            "#!/bin/sh\ncat <<'A' <<\"B\"\nfirst body\nA\nshell_evidence\nB\n",
        );
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "every heredoc body in one command must remain inert"
        );
        let inspected_rows = shell_rows
            .replacen(
                "scripts/check::shell_evidence",
                "scripts/check::shell_evidence;xtask/src/main.rs::fn inspect()",
                1,
            )
            .replacen("sh scripts/check", "cargo xtask inspect", 1);
        write(&root, "scripts/check", "#!/bin/sh\nshell_evidence=1\n");
        write(
            &root,
            "xtask/src/main.rs",
            "fn inspect() { let _ = \"scripts/check\"; }\n",
        );
        write(&root, "docs/acceptance/section-23.tsv", &inspected_rows);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "an inspecting gate must use the shell path in executable I/O syntax"
        );
        write(
            &root,
            "xtask/src/main.rs",
            "fn inspect() { let _ = std::fs::read_to_string(\"scripts/check\"); }\n",
        );
        assert!(validate_acceptance_matrix(&root).is_ok());
        write(&root, "docs/acceptance/section-23.tsv", &rows);

        write(
            &root,
            "tests/direct.rs",
            "macro_rules! case { ($name:ident, $body:block) => {}; (@test $name:ident) => { #[test] fn $name() {} }; }\ncase!(direct_case, {});\n",
        );
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "a test in an unselected macro arm must not make the named invocation executable"
        );
        write(&root, "tests/direct.rs", "#[test]\nfn direct_case() {}\n");

        let missing = rows.replace("30\tverified", "29\tverified");
        write(&root, "docs/acceptance/section-23.tsv", &missing);
        assert!(validate_acceptance_matrix(&root).is_err());

        let stale = rows.replace(
            "tests/direct.rs::direct_case",
            "tests/direct.rs::removed_case",
        );
        write(&root, "docs/acceptance/section-23.tsv", &stale);
        assert!(validate_acceptance_matrix(&root).is_err());

        write(
            &root,
            "tests/direct.rs",
            "// direct_case was removed; this comment is not executable evidence\n",
        );
        write(&root, "docs/acceptance/section-23.tsv", &rows);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "a deleted test name surviving only in a comment must not satisfy acceptance evidence"
        );

        write(&root, "tests/direct.rs", "fn direct_case() {}\n");
        write(&root, "docs/acceptance/section-23.tsv", &rows);
        assert!(validate_acceptance_matrix(&root).is_err());

        write(
            &root,
            "tests/direct.rs",
            "#[test]\n#[ignore]\nfn direct_case() {}\n",
        );
        assert!(validate_acceptance_matrix(&root).is_err());

        write(&root, "tests/direct.rs", "#[test]\nfn direct_case() {}\n");
        let mismatched = rows.replace("cargo test direct_case", "cargo test unrelated");
        write(&root, "docs/acceptance/section-23.tsv", &mismatched);
        assert!(validate_acceptance_matrix(&root).is_err());

        let prefix_collision =
            rows.replace("cargo test direct_case", "cargo test direct_case_removed");
        write(&root, "docs/acceptance/section-23.tsv", &prefix_collision);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "a prefix/suffix filter collision must not select the cited test"
        );

        write(
            &root,
            "crates/example/tests/direct.rs",
            "#[test]\nfn direct_case() {}\n",
        );
        let wrong_package = rows
            .replace(
                "tests/direct.rs::direct_case",
                "crates/example/tests/direct.rs::direct_case",
            )
            .replace(
                "cargo test direct_case",
                "cargo test -p other --test direct",
            );
        write(&root, "docs/acceptance/section-23.tsv", &wrong_package);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "a same-stem target in another package must not select the cited test"
        );

        write(
            &root,
            "tests/direct.rs",
            "const TEXT: &str = r#\"x\" #[test]\nfn direct_case() {} \"#;\n",
        );
        write(&root, "docs/acceptance/section-23.tsv", &rows);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "test syntax inside a raw Rust string must not count as executable evidence"
        );

        write(
            &root,
            "tests/direct.rs",
            "#![cfg(feature = \"disabled-evidence\")]\n#[test]\nfn direct_case() {}\n",
        );
        write(&root, "docs/acceptance/section-23.tsv", &rows);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "a symbol compiled out by the recorded command is not executable evidence"
        );
        let enabled = rows.replace(
            "cargo test direct_case",
            "cargo test --features disabled-evidence direct_case",
        );
        write(&root, "docs/acceptance/section-23.tsv", &enabled);
        assert!(validate_acceptance_matrix(&root).is_ok());
    }
}
