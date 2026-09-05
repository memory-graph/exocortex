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
            // D19: the SaaS-API adapter family's source transport. The
            // ONLY outbound HTTP in the workspace outside the reviewed
            // SSE/backend legs; bearer-authenticated JSON+GraphQL to
            // the operator-pinned Linear/GitHub endpoints, body-capped,
            // rate-limit-aware (exocortex-api-client).
            | ("crates/exocortex-api-client/Cargo.toml", "hyper")
            | ("crates/exocortex-client/Cargo.toml", "eventsource-client")
            | ("crates/exocortex-client/Cargo.toml", "hyper")
            | ("crates/exocortex-cluster/Cargo.toml", "eventsource-client")
    )
}

pub(crate) const STORAGE_LIVE_CANARIES: &[(&str, &str)] = &[
    ("integration", "roundtrip_memory"),
    ("fencing_live", "stale_lease_write_is_fenced_live"),
    (
        "fingerprint_migration",
        "legacy_v1_pin_boots_migrates_and_reboots",
    ),
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
            let body = body_without_nested_functions(&source, &functions[caller], &functions);
            for callee in 0..functions.len() {
                if !reachable[callee]
                    && !functions[callee].configured_out
                    && contains_call(
                        &body,
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
        let body = body_without_nested_functions(&source, function, &functions);
        if let Some(qualified_call) = qualified_call {
            return qualified_call_offsets(&body, qualified_call)
                .any(|offset| call_is_reachable(&body, offset));
        }
        body.match_indices(&needle).any(|(relative, _)| {
            is_call_to(&body, relative, target_is_method) && call_is_reachable(&body, relative)
        })
    })
}

fn body_without_nested_functions(
    source: &str,
    function: &RustFunction,
    functions: &[RustFunction],
) -> String {
    let mut body = source.as_bytes()[function.body_start..function.body_end].to_vec();
    for nested in functions.iter().filter(|nested| {
        nested.body_start > function.body_start && nested.body_end < function.body_end
    }) {
        let start = nested.body_start - function.body_start;
        let end = nested.body_end - function.body_start;
        body[start..end].fill(b' ');
    }
    String::from_utf8(body).expect("masking Rust function bodies preserves UTF-8")
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
        .any(|(offset, _)| is_call_to(body, offset, method) && call_is_reachable(body, offset))
}

fn is_call_to(body: &str, offset: usize, method: Option<bool>) -> bool {
    if body[..offset]
        .trim_end()
        .strip_suffix("fn")
        .is_some_and(|prefix| {
            prefix
                .chars()
                .next_back()
                .is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_')
        })
    {
        return false;
    }
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

fn call_is_reachable(body: &str, offset: usize) -> bool {
    call_is_reachable_outside_closure(body, offset) && !inside_uncalled_closure(body, offset)
}

fn call_is_reachable_outside_closure(body: &str, offset: usize) -> bool {
    !configured_out_at(body, offset)
        && !after_unconditional_exit(body, offset)
        && !inside_constant_false_block(body, offset)
}

fn inside_constant_false_block(body: &str, offset: usize) -> bool {
    let mut open_braces = Vec::new();
    for (index, byte) in body.as_bytes()[..offset].iter().enumerate() {
        match byte {
            b'{' => open_braces.push(index),
            b'}' => {
                open_braces.pop();
            }
            _ => {}
        }
    }
    open_braces.into_iter().any(|open| {
        let header_start = body[..open].rfind([';', '{', '}']).map_or(0, |at| at + 1);
        let header = body[header_start..open].trim();
        header.ends_with("if false") || header.ends_with("while false")
    })
}

fn inside_uncalled_closure(body: &str, offset: usize) -> bool {
    enclosing_let_closures(body, offset)
        .into_iter()
        .any(|(binding, close)| {
            let suffix = &body[close + 1..];
            !suffix.match_indices(&format!("{binding}(")).any(|(at, _)| {
                // Full reachability: an invocation nested inside a second
                // uncalled closure is not a live call site.
                is_call_to(suffix, at, Some(false)) && call_is_reachable(suffix, at)
            })
        })
}

fn enclosing_let_closures(body: &str, offset: usize) -> Vec<(String, usize)> {
    let mut closures = Vec::new();
    for (let_at, _) in body[..offset].match_indices("let ") {
        if let_at > 0
            && (body.as_bytes()[let_at - 1].is_ascii_alphanumeric()
                || body.as_bytes()[let_at - 1] == b'_')
        {
            continue;
        }
        let declaration = &body[let_at + 4..];
        let Some(equal_relative) = declaration.find('=') else {
            continue;
        };
        if declaration[..equal_relative].contains(';') {
            continue;
        }
        let binding = declaration[..equal_relative]
            .trim()
            .strip_prefix("mut ")
            .unwrap_or(declaration[..equal_relative].trim())
            .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
            .next()
            .unwrap_or_default();
        if binding.is_empty() {
            continue;
        }
        let mut value_at = let_at + 4 + equal_relative + 1;
        while body
            .as_bytes()
            .get(value_at)
            .is_some_and(u8::is_ascii_whitespace)
        {
            value_at += 1;
        }
        for prefix in ["async ", "move "] {
            if body[value_at..].starts_with(prefix) {
                value_at += prefix.len();
            }
        }
        if body.as_bytes().get(value_at) != Some(&b'|') {
            continue;
        }
        let pipe_end = if body.as_bytes().get(value_at + 1) == Some(&b'|') {
            value_at + 2
        } else {
            let Some(relative) = body[value_at + 1..].find('|') else {
                continue;
            };
            value_at + 2 + relative
        };
        let Some((body_start, close)) = closure_body_bounds(body, pipe_end) else {
            continue;
        };
        if offset >= body_start && offset < close {
            closures.push((binding.to_owned(), close));
        }
    }
    closures
}

fn closure_body_bounds(body: &str, pipe_end: usize) -> Option<(usize, usize)> {
    let mut body_start = pipe_end;
    while body
        .as_bytes()
        .get(body_start)
        .is_some_and(u8::is_ascii_whitespace)
    {
        body_start += 1;
    }
    if body[body_start..].starts_with("->") {
        let open = body[body_start + 2..].find('{')? + body_start + 2;
        return matching_delimiter(body, open, b'{', b'}').map(|close| (open, close));
    }
    if body.as_bytes().get(body_start) == Some(&b'{') {
        return matching_delimiter(body, body_start, b'{', b'}').map(|close| (body_start, close));
    }
    closure_expression_end(body, body_start).map(|close| (body_start, close))
}

fn closure_expression_end(body: &str, start: usize) -> Option<usize> {
    let mut parentheses = 0usize;
    let mut brackets = 0usize;
    let mut braces = 0usize;
    for (relative, byte) in body.as_bytes()[start..].iter().enumerate() {
        match byte {
            b'(' => parentheses += 1,
            b')' => parentheses = parentheses.checked_sub(1)?,
            b'[' => brackets += 1,
            b']' => brackets = brackets.checked_sub(1)?,
            b'{' => braces += 1,
            b'}' if braces > 0 => braces -= 1,
            b';' if parentheses == 0 && brackets == 0 && braces == 0 => {
                return Some(start + relative);
            }
            _ => {}
        }
    }
    None
}

fn rust_functions(source: &str) -> Vec<RustFunction> {
    let mut functions: Vec<RustFunction> = Vec::new();
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
        let Some(body_end) = matching_delimiter(source, body_start, b'{', b'}') else {
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
        let nested_in_function = functions
            .iter()
            .any(|function| at > function.body_start && at < function.body_end);
        let root = !nested_in_function
            && (name == "main"
                || name == "handle"
                || inside_trait_impl(source, at)
                || header
                    .split_whitespace()
                    .any(|token| token.starts_with("pub")));
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
    enclosing_impl_span(source, function_at).is_some()
}

/// The nearest `impl` block (by brace span) enclosing `function_at`.
fn enclosing_impl_span(source: &str, function_at: usize) -> Option<(usize, usize)> {
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
        if matching_delimiter(source, open, b'{', b'}').is_some_and(|close| close > function_at) {
            return Some((impl_at, open));
        }
        search_end = impl_at;
    }
    None
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
    enclosing_impl_span(source, function_at)
        .is_some_and(|(impl_at, open)| source[impl_at..open].contains(" for "))
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
        // §23 carries numeric ids 1..=30; the OC-PRD success criteria
        // ride the same matrix under the `oc` namespace (S1-S6); PX2's
        // pack-verb criteria ride it under the `px` namespace (px1..=px8,
        // palantir-expansion PRD §3.2 acceptance).
        let is_core = matches!(columns[0].parse::<u8>(), Ok(n) if (1..=30).contains(&n));
        let is_oc = matches!(
            columns[0].as_bytes(),
            [b'o', b'c', n] if (b'1'..=b'6').contains(n)
        );
        let is_px = matches!(
            columns[0].as_bytes(),
            [b'p', b'x', n] if (b'1'..=b'8').contains(n)
        );
        let is_ac = matches!(
            columns[0].as_bytes(),
            [b'a', b'c', n] if (b'1'..=b'5').contains(n)
        );
        anyhow::ensure!(
            is_core || is_oc || is_px || is_ac,
            "criterion {} is outside §23 (1..=30), the OC/PX2/AC rows",
            columns[0]
        );
        let criterion = columns[0].to_string();
        anyhow::ensure!(
            seen.insert(criterion.clone()),
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
                    root, &criterion, relative, needle, &source, columns[3], columns[4],
                )?;
            }
        }
    }
    // §23's own 30 criteria plus the OC-PRD S-rows (oc namespace) plus
    // the PX2 pack-verb rows (px namespace).
    let core = seen
        .iter()
        .filter(|c| !c.starts_with("oc") && !c.starts_with("px") && !c.starts_with("ac"))
        .count();
    anyhow::ensure!(
        core == 30,
        "acceptance matrix covers {} of 30 §23 criteria",
        core
    );
    let oc = seen.iter().filter(|c| c.starts_with("oc")).count();
    anyhow::ensure!(
        (1..=6).contains(&oc),
        "acceptance matrix covers {} of the 6 OC-PRD S-rows",
        oc
    );
    let px = seen.iter().filter(|c| c.starts_with("px")).count();
    anyhow::ensure!(
        (1..=8).contains(&px),
        "acceptance matrix covers {} of the 8 PX2 pack-verb rows",
        px
    );
    let ac = seen.iter().filter(|c| c.starts_with("ac")).count();
    anyhow::ensure!(
        (1..=5).contains(&ac),
        "acceptance matrix covers {} of the 5 adapter-contract rows",
        ac
    );
    Ok(())
}

fn validate_executable_evidence(
    root: &Path,
    criterion: &str,
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
    let invocations = command_invocations(command);
    if invocations.iter().any(|tokens| {
        tokens.first().is_some_and(|token| *token == relative)
            || matches!(tokens.as_slice(), [shell, script, ..] if matches!(*shell, "sh" | "bash") && *script == relative)
    }) {
        return Ok(true);
    }
    let Some(tokens) = invocations
        .iter()
        .find(|tokens| matches!(tokens.as_slice(), ["cargo", "xtask", _, ..]))
    else {
        return Ok(false);
    };
    let gate = tokens[2];
    let function = gate.replace('-', "_");
    let locator = format!("xtask/src/main.rs::fn {function}()");
    if !all_evidence.split(';').any(|item| item == locator) {
        return Ok(false);
    }
    let source = std::fs::read_to_string(root.join("xtask/src/main.rs"))?;
    let clean_source = strip_comments_and_strings(&source);
    let pattern = format!("fn {function}");
    let Some(start) = clean_source.find(&pattern) else {
        return Ok(false);
    };
    let Some(open) = function_body_start(&clean_source, start + pattern.len()) else {
        return Ok(false);
    };
    let functions = rust_functions(&clean_source);
    let Some(entry) = functions
        .iter()
        .position(|candidate| candidate.name == function && candidate.body_start == open + 1)
    else {
        return Ok(false);
    };
    let mut reachable = vec![false; functions.len()];
    reachable[entry] = true;
    loop {
        let mut changed = false;
        for caller in 0..functions.len() {
            if !reachable[caller] {
                continue;
            }
            let body = body_without_nested_functions(&clean_source, &functions[caller], &functions);
            for callee in 0..functions.len() {
                if !reachable[callee]
                    && !functions[callee].configured_out
                    && contains_call(
                        &body,
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
    Ok(functions.iter().enumerate().any(|(index, function)| {
        if !reachable[index] {
            return false;
        }
        let body = body_without_nested_functions(&source, function, &functions);
        let clean = body_without_nested_functions(&clean_source, function, &functions);
        ["read_to_string", "Command::new", "include_str!"]
            .into_iter()
            .any(|call| {
                let needle = format!("{call}(\"{relative}\")");
                body.match_indices(&needle).any(|(offset, _)| {
                    clean[offset..]
                        .get(..call.len())
                        .is_some_and(|candidate| candidate == call)
                        && call_is_reachable(&clean, offset)
                })
            })
    }))
}

fn command_invocations(command: &str) -> Vec<Vec<&str>> {
    command
        .split("&&")
        .filter_map(|segment| {
            let tokens = segment
                .split_whitespace()
                .map(|token| token.trim_matches(['\'', '"']))
                .collect::<Vec<_>>();
            let executable = tokens
                .iter()
                .position(|token| !is_environment_assignment(token))?;
            Some(tokens[executable..].to_vec())
        })
        .collect()
}

fn is_environment_assignment(token: &str) -> bool {
    let Some((name, _)) = token.split_once('=') else {
        return false;
    };
    !name.is_empty()
        && !name.as_bytes()[0].is_ascii_digit()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
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
    command_invocations(command).into_iter().any(|tokens| {
        if !matches!(tokens.as_slice(), ["cargo", "test", ..]) {
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

/// OC-PRD S3: every compatibility-fingerprint comparison consults the
/// D2 policy table — `exocortex_kernel::compatibility` (or its
/// wire-side projection `exocortex_wire::compatibility` for kernel-free
/// components). A production source that compares fingerprint bytes
/// directly is a boundary that did not declare its rule.
/// PX3: the seam inventory. Every declared seam carries exactly one
/// conformance suite (crate + source file + canary test); every test
/// FILE in a seam crate's tests/ directory is claimed by exactly one
/// row. The bijection, not a count: adding a legitimate seam costs a
/// suite, and adding a suite costs a row — a tests/ file that appears
/// in a seam crate without a row fails the gate, and a row whose file
/// or canary is missing fails the gate.
pub(crate) const SEAM_INVENTORY: &[(&str, &str, &str, &str)] = &[
    // (seam, package, file under the crate, canary test)
    (
        "kernel-pack",
        "exocortex-kernel",
        "tests/pack_registration.rs",
        "registered_pack_loads_with_kernel_constants_bound",
    ),
    (
        "kernel-compatibility",
        "exocortex-kernel",
        "tests/compatibility.rs",
        "boundary_rules_come_from_the_policy_table",
    ),
    (
        "kernel-actions-macro",
        "exocortex-kernel",
        "tests/actions_macro_spike.rs",
        "actions_bodies_expand_and_type_check",
    ),
    (
        "kernel-pack-verbs",
        "exocortex-kernel",
        "tests/pack_verbs.rs",
        "pack_actions_register_with_typed_bodies_and_ceilings",
    ),
    (
        "kernel-ids",
        "exocortex-kernel",
        "tests/ids.rs",
        "external_identity_is_layout_immune_property",
    ),
    (
        "kernel-validator",
        "exocortex-kernel",
        "tests/validator.rs",
        "valid_solves_solution_problem_is_accepted",
    ),
    (
        "kernel-embedding",
        "exocortex-kernel",
        "tests/embedding.rs",
        "embedding_vector_and_model_revision_round_trip_together",
    ),
    (
        "pack-dev-v1",
        "exocortex-pack-dev-v1",
        "tests/loads_correctly.rs",
        "golden_fingerprint_is_pinned",
    ),
    (
        "pack-mortgage-v1",
        "exocortex-pack-mortgage-v1",
        "tests/loads_correctly.rs",
        "both_packs_register_into_one_ontology",
    ),
    (
        "pack-study-v1",
        "exocortex-pack-study-v1",
        "tests/loads_correctly.rs",
        "all_three_packs_register_into_one_ontology",
    ),
    (
        "wire-signing",
        "exocortex-wire",
        "src/signing.rs",
        "invalidation_envelope_signing_is_canonical_and_tamper_evident",
    ),
    (
        "cache",
        "exocortex-cache",
        "tests/cache.rs",
        "traversal_never_crosses_an_invisible_intermediate_node",
    ),
    (
        "cache-allocation",
        "exocortex-cache",
        "tests/alloc.rs",
        "read_hot_path_snapshot_load_is_allocation_free",
    ),
    (
        "reasoning",
        "exocortex-reasoning",
        "tests/rules.rs",
        "durable_submission_survives_worker_restart",
    ),
    (
        "storage-cypher",
        "exocortex-storage",
        "tests/cypher.rs",
        "no_cypher_outside_the_catalogue_module",
    ),
    (
        "storage-fencing",
        "exocortex-storage",
        "tests/fencing.rs",
        "batch_row_failure_rolls_back_every_memory_and_lsn",
    ),
    (
        "storage-fencing-live",
        "exocortex-storage",
        "tests/fencing_live.rs",
        "stale_lease_write_is_fenced_live",
    ),
    (
        "storage-fingerprint-migration",
        "exocortex-storage",
        "tests/fingerprint_migration.rs",
        "legacy_v1_pin_boots_migrates_and_reboots",
    ),
    (
        "storage-in-memory-props",
        "exocortex-storage",
        "tests/in_memory_props.rs",
        "bi_temporal_roundtrip_prop",
    ),
    (
        "storage-integration-live",
        "exocortex-storage",
        "tests/integration.rs",
        "roundtrip_memory",
    ),
    (
        "storage-live-bench",
        "exocortex-storage",
        "tests/live_bench.rs",
        "indexed_relationship_point_read_meets_live_falkor_budget",
    ),
    (
        "change-log",
        "exocortex-cluster",
        "tests/change_log.rs",
        "floor_is_the_oldest_buffered_lsn_not_the_frontier",
    ),
    (
        "cluster",
        "exocortex-cluster",
        "tests/cluster.rs",
        "envelope_hmac_verifies_and_rejects_tampering",
    ),
    (
        "cluster-cross-node",
        "exocortex-cluster",
        "tests/cross_node.rs",
        "cross_node_commit_reaches_peer_hub",
    ),
    (
        "cluster-rolling-upgrade",
        "exocortex-cluster",
        "tests/rolling_upgrade.rs",
        "superset_accepts_subset_producer_subset_rejects_legibly",
    ),
    (
        "cluster-wire-fanout",
        "exocortex-cluster",
        "tests/wire_fanout.rs",
        "invalidations_fan_out_across_three_nodes_at_floor_throughput",
    ),
    (
        "cluster-sse-e2e",
        "exocortex-cluster",
        "tests/sse_e2e.rs",
        "sse_client_observes_upsert_within_200ms",
    ),
    (
        "ingest",
        "exocortex-ingest",
        "tests/ingest.rs",
        "e2e_valid_batch_accepted_with_monotonic_lsn",
    ),
    (
        "ingest-e2e",
        "exocortex-ingest",
        "tests/e2e.rs",
        "fifty_row_batch_accepted_lsn_monotonic",
    ),
    (
        "ingest-rights",
        "exocortex-ingest",
        "tests/rights.rs",
        "record_rights_override_source_defaults_and_absence_stays_none",
    ),
    (
        "ingest-embedding-runtime",
        "exocortex-ingest",
        "tests/embedding_runtime.rs",
        "max_batch_embedding_is_one_blocking_invocation_without_worker_starvation",
    ),
    (
        "ingest-external-key",
        "exocortex-ingest",
        "tests/external_key.rs",
        "identity_derives_from_raw_uuid_bytes",
    ),
    (
        "ingest-grouping",
        "exocortex-ingest",
        "tests/grouping.rs",
        "two_batches_group_under_one_conversation_across_restart",
    ),
    (
        "ingest-round1",
        "exocortex-ingest",
        "tests/round1_e2e.rs",
        "dreams_cycle_over_ingested_data",
    ),
    (
        "ingest-reindex",
        "exocortex-ingest",
        "tests/reindex.rs",
        "reindex_restamps_a_swapped_graph_to_one_model",
    ),
    (
        "ingest-similarity-seeding",
        "exocortex-ingest",
        "tests/similarity_seeding.rs",
        "seeding_writes_computed_edges_in_the_dreams_window",
    ),
    (
        "ingest-schema-evolution",
        "exocortex-ingest",
        "tests/schema_evolution.rs",
        "drifted_schema_hash_rejects_every_row_until_re_registration",
    ),
    (
        "ingest-preflight",
        "exocortex-ingest",
        "tests/preflight.rs",
        "preflight_verdicts_match_submit_verdicts_row_for_row",
    ),
    (
        "ingest-manifest-parity",
        "exocortex-ingest",
        "tests/manifest_parity.rs",
        "manifest_verdicts_agree_row_for_row",
    ),
    (
        "write-path-parity",
        "exocortex-ingest",
        "tests/write_path_parity.rs",
        "verdicts_agree_row_for_row",
    ),
    (
        "ops-registry",
        "exocortex-ops",
        "tests/parity.rs",
        "parity_every_operation_on_both_surfaces_with_schemas",
    ),
];

/// Validate the bijection statically: every row resolves to a real
/// file carrying its canary, and every tests/ file in a seam crate is
/// claimed by a row.
pub(crate) fn seam_inventory_violations(root: &Path) -> Result<Vec<String>> {
    let mut violations = Vec::new();
    let mut claimed: std::collections::BTreeSet<(String, String)> = Default::default();
    for (seam, package, file, canary) in SEAM_INVENTORY {
        let path = root.join("crates").join(package).join(file);
        if !path.is_file() {
            violations.push(format!(
                "seam `{seam}`: crates/{package}/{file} does not exist"
            ));
            continue;
        }
        let source = std::fs::read_to_string(&path)?;
        let clean = strip_comments_and_strings(&source);
        let plain_fn = clean.contains(&format!("fn {canary}"));
        // Macro-generated tests (itest!) name their symbol at the
        // invocation site instead of a fn declaration.
        let macro_test =
            clean.contains(&format!("{canary},")) || clean.contains(&format!("{canary}("));
        if !plain_fn && !macro_test {
            violations.push(format!(
                "seam `{seam}`: canary `{canary}` is absent from crates/{package}/{file}"
            ));
        }
        let stem = path
            .file_stem()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or_default()
            .to_string();
        if file.starts_with("tests/") && !claimed.insert((package.to_string(), stem.clone())) {
            violations.push(format!(
                "seam `{seam}`: tests/{stem}.rs in {package} is claimed twice"
            ));
        }
    }
    // The other direction: unclaimed suite files in seam crates. A
    // tests/ file that appears without a row is a seam nobody
    // inventoried.
    let mut seam_crates: std::collections::BTreeSet<&str> = Default::default();
    for (_, package, _, _) in SEAM_INVENTORY {
        seam_crates.insert(package);
    }
    for package in seam_crates {
        let dir = root.join("crates").join(package).join("tests");
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(stem) = name.strip_suffix(".rs") else {
                continue;
            };
            if !claimed.contains(&(package.to_string(), stem.to_string())) {
                violations.push(format!(
                    "crates/{package}/tests/{stem}.rs matches no seam row — a legitimate new seam costs a row and a canary, not silence"
                ));
            }
        }
    }
    Ok(violations)
}

pub(crate) fn compatibility_policy_violations(root: &Path) -> Result<Vec<String>> {
    const HOMES: &[&str] = &[
        "crates/exocortex-kernel/src/compatibility.rs",
        "crates/exocortex-wire/src/compatibility.rs",
    ];
    let mut files = Vec::new();
    walk_files(&root.join("crates"), &mut files, &["rs"])?;
    let mut violations = Vec::new();
    for path in files {
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .display()
            .to_string();
        let in_src = rel.contains("/src/");
        if !in_src || HOMES.contains(&rel.as_str()) {
            continue;
        }
        let source = std::fs::read_to_string(&path)?;
        let code = strip_comments_and_strings(&source);
        for line in code.lines() {
            let compares = line.contains("==") || line.contains("!=");
            let touches_fingerprint = [
                "ontology_fingerprint",
                ".fingerprint",
                "fingerprint.0",
                "fp.0",
                "build_fingerprint",
            ]
            .iter()
            .any(|needle| line.contains(needle));
            if compares && touches_fingerprint {
                violations.push(format!(
                    "{rel}: raw fingerprint comparison outside the policy table: {}",
                    line.trim()
                ));
            }
        }
    }
    Ok(violations)
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

/// D21-e (adapter-contract PRD D5): the contract conformance registry.
/// Every obligation the PRD places on adapters and the SDK is pinned to
/// a canary in real source; every adapter crate in the workspace is
/// claimed by a row. A new adapter that skips the contract fails CI —
/// the same shape (and failure mode) as the seam inventory.
///
/// Row shape: (obligation, crate, file, canary).
pub(crate) const ADAPTER_CONTRACT: &[(&str, &str, &str, &str)] = &[
    // (a) Projection discipline — every workspace adapter declares one.
    (
        "projection-declared",
        "exocortex-adapter-git",
        "src/lib.rs",
        "fn projection(",
    ),
    // (a) D1: the first table-flavored adapter — projection is not
    // optional for it anywhere (server, mock, and this registry).
    (
        "projection-declared",
        "exocortex-adapter-parquet",
        "src/lib.rs",
        "fn projection(",
    ),
    // (a) D1 iceberg flavor: same contract, its own reader.
    (
        "projection-declared",
        "exocortex-adapter-iceberg",
        "src/lib.rs",
        "fn projection(",
    ),
    // (a) D1 delta flavor: same contract, its own reader.
    (
        "projection-declared",
        "exocortex-adapter-delta",
        "src/lib.rs",
        "fn projection(",
    ),
    // (a) D20 CDC flavor: same contract, its own reader.
    (
        "projection-declared",
        "exocortex-adapter-postgres",
        "src/lib.rs",
        "fn projection(",
    ),
    // (a) D19 Linear flavor: SaaS transcription under the same
    // contract (direct GraphQL, bounded updatedAt windows).
    (
        "projection-declared",
        "exocortex-adapter-linear",
        "src/lib.rs",
        "fn projection(",
    ),
    // (a) D19 GitHub flavor: same contract, its own reader.
    (
        "projection-declared",
        "exocortex-adapter-github",
        "src/lib.rs",
        "fn projection(",
    ),
    // (a) Bounds stop the window before the wire (SDK-side, A2).
    (
        "bounds-enforced",
        "exocortex-adapter-sdk",
        "src/lib.rs",
        "max_rows_per_window",
    ),
    // (b) PreflightBatch shares Submit's implementation, verbatim parity.
    (
        "preflight-shares-submit",
        "exocortex-ingest",
        "tests/preflight.rs",
        "preflight_verdicts_match_submit_verdicts_row_for_row",
    ),
    (
        "preflight-commits-nothing",
        "exocortex-ingest",
        "tests/preflight.rs",
        "preflight_commits_nothing_and_leaves_no_idempotency_claim",
    ),
    // (b) The registry face is parity-covered like every operation.
    (
        "preflight-registry-parity",
        "exocortex-server",
        "tests/http_parity.rs",
        "preflight_batch_answers_identically_over_http_and_the_registry",
    ),
    // (c) The rulebook as data: published, fingerprinted, interpreted.
    (
        "manifest-published",
        "exocortex-wire",
        "proto/ingest.proto",
        "rpc GetValidationManifest",
    ),
    (
        "manifest-scheme-refused",
        "exocortex-wire",
        "src/manifest.rs",
        "manifest_version != MANIFEST_VERSION",
    ),
    (
        "manifest-interpreter-parity",
        "exocortex-ingest",
        "tests/manifest_parity.rs",
        "manifest_verdicts_agree_row_for_row",
    ),
    (
        "manifest-local-validation",
        "exocortex-adapter-sdk",
        "src/lib.rs",
        "validate_units",
    ),
    // (d) Every row of the D4 verdict table has a test.
    (
        "schema-policy-unmapped-add",
        "exocortex-ingest",
        "tests/schema_evolution.rs",
        "unmapped_addition_is_accepted_and_writes_exactly_one_audit_row",
    ),
    (
        "schema-policy-fail-closed",
        "exocortex-ingest",
        "tests/schema_evolution.rs",
        "mapped_column_removal_retype_and_rename_fail_closed",
    ),
    (
        "schema-policy-drift",
        "exocortex-ingest",
        "tests/schema_evolution.rs",
        "drifted_schema_hash_rejects_every_row_until_re_registration",
    ),
    (
        "schema-policy-rewind",
        "exocortex-ingest",
        "tests/schema_evolution.rs",
        "rewound_snapshot_is_rejected_with_its_own_code",
    ),
];

/// Validate the adapter contract statically: every row resolves to a
/// real file carrying its canary, and every `crates/exocortex-adapter-*`
/// directory is claimed by at least one projection-declared row.
pub(crate) fn adapter_contract_violations(root: &Path) -> Result<Vec<String>> {
    let mut violations = Vec::new();
    let mut claimed: std::collections::BTreeSet<String> = Default::default();
    for (obligation, package, file, canary) in ADAPTER_CONTRACT {
        let path = root.join("crates").join(package).join(file);
        if !path.is_file() {
            violations.push(format!(
                "obligation `{obligation}`: crates/{package}/{file} does not exist"
            ));
            continue;
        }
        let source = std::fs::read_to_string(&path)?;
        if !source.contains(canary) {
            violations.push(format!(
                "obligation `{obligation}`: canary `{canary}` is absent from crates/{package}/{file}"
            ));
        }
        if *obligation == "projection-declared" {
            claimed.insert(package.to_string());
        }
    }
    // The bijection direction: an adapter crate nobody claimed is a
    // contract gap. (`exocortex-adapter-sdk` is the contract itself,
    // not an adapter; `exocortex-adapter-table` is the shared
    // mapping library the flavor adapters link — it reads no source
    // and owns no flavor, so it carries no projection row. Every
    // ADAPTER crate — one that registers a source — must declare a
    // projection.)
    if let Ok(entries) = std::fs::read_dir(root.join("crates")) {
        for entry in entries {
            let name = entry?.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(adapter) = name.strip_prefix("exocortex-adapter-") {
                if adapter == "sdk" || adapter == "table" {
                    continue;
                }
                if !claimed.contains(name) {
                    violations.push(format!(
                        "adapter crate `{name}` is not claimed by a projection-declared row — \
                         declare a projection and add it to ADAPTER_CONTRACT"
                    ));
                }
            }
        }
    }
    Ok(violations)
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
    fn adapter_contract_rejects_missing_canaries_and_unclaimed_adapters() {
        let root = fixture("adapter-contract");
        // A claimed adapter with its canary present.
        write(
            &root,
            "crates/exocortex-adapter-git/src/lib.rs",
            "pub fn projection(max_window: u64) -> u8 { max_window as u8 }",
        );
        // An adapter crate nobody claimed: the bijection direction fires.
        write(
            &root,
            "crates/exocortex-adapter-new/src/lib.rs",
            "// ships without declaring anything",
        );
        let violations = adapter_contract_violations(&root).unwrap();
        assert!(
            violations
                .iter()
                .any(|v| v.contains("exocortex-adapter-new") && v.contains("projection-declared")),
            "unclaimed adapter must be named: {violations:?}"
        );
        assert!(
            violations.iter().any(|v| v.contains("does not exist")),
            "missing obligation files must be named: {violations:?}"
        );
    }

    #[test]
    fn seam_inventory_rejects_unclaimed_suites_and_missing_canaries() {
        let root = fixture("seam-inventory");
        // All kernel suite files exist with their canaries, except
        // compatibility.rs, whose canary is absent.
        write(
            &root,
            "crates/exocortex-kernel/tests/pack_registration.rs",
            "#[test]
fn registered_pack_loads_with_kernel_constants_bound() {}
",
        );
        write(
            &root,
            "crates/exocortex-kernel/tests/compatibility.rs",
            "#[test]
fn something_else() {}
",
        );
        write(
            &root,
            "crates/exocortex-kernel/tests/actions_macro_spike.rs",
            "#[test]
fn actions_bodies_expand_and_type_check() {}
",
        );
        write(
            &root,
            "crates/exocortex-kernel/tests/ids.rs",
            "#[test]
fn external_identity_is_layout_immune_property() {}
",
        );
        write(
            &root,
            "crates/exocortex-kernel/tests/validator.rs",
            "#[test]
fn valid_solves_solution_problem_is_accepted() {}
",
        );
        write(
            &root,
            "crates/exocortex-kernel/tests/embedding.rs",
            "#[test]
fn embedding_vector_and_model_revision_round_trip_together() {}
",
        );
        // (Rows for other crates report their missing files; the two
        // failure directions under test are the missing canary and the
        // unclaimed suite file.)

        // A row whose canary vanished from its file, and an unclaimed
        // suite file in a seam crate.
        write(
            &root,
            "crates/exocortex-kernel/tests/ids.rs",
            "#[test]
fn renamed() {}
",
        );
        // An unclaimed suite file in a seam crate.
        write(
            &root,
            "crates/exocortex-kernel/tests/rogue.rs",
            "#[test]
fn unclaimed() {}
",
        );
        let violations = seam_inventory_violations(&root).unwrap();
        assert!(
            violations
                .iter()
                .any(|v| v.contains("external_identity_is_layout_immune_property")),
            "missing canary reported: {violations:?}"
        );
        assert!(
            violations.iter().any(|v| v.contains("rogue.rs")),
            "unclaimed suite reported: {violations:?}"
        );
    }
    #[test]
    fn compatibility_policy_rejects_raw_fingerprint_comparisons() {
        let root = fixture("compat-policy");
        // A boundary that compares fingerprint bytes directly instead
        // of consulting the policy table.
        write(
            &root,
            "crates/exocortex-example/src/gate.rs",
            "fn check(env: &Envelope, mine: &[u8; 32]) -> bool {
    env.ontology_fingerprint.as_slice() == mine.as_slice()
}
",
        );
        // Policy-homed comparisons never trip.
        write(
            &root,
            "crates/exocortex-kernel/src/compatibility.rs",
            "fn inner(a: &[u8; 32], b: &[u8; 32]) -> bool { a == b }
",
        );
        // Tests and non-src layouts are out of scope.
        write(
            &root,
            "crates/exocortex-example/tests/gate.rs",
            "assert!(env.ontology_fingerprint.as_slice() == mine.as_slice());
",
        );
        let violations = compatibility_policy_violations(&root).unwrap();
        assert_eq!(
            violations.len(),
            1,
            "exactly the raw comparison is rejected: {violations:?}"
        );
        assert!(violations[0].contains("gate.rs"), "{violations:?}");
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
        assert!(validate_storage_targets(&root).is_err());
        write(
            &root,
            "crates/exocortex-storage/tests/fingerprint_migration.rs",
            "",
        );
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

        for source in [
            "pub fn root() { fn nested() { fence(); } } fn fence() {}\n",
            "pub fn root() { let unused = || { fence(); }; let _ = unused; } fn fence() {}\n",
            "pub fn root() { let unused = || { let value = 1; let _ = value; fence(); }; let _ = unused; } fn fence() {}\n",
            "pub fn root() { let unused = || fence(); let _ = unused; } fn fence() {}\n",
            "pub fn root() { let unused = || -> bool { fence(); true }; let _ = unused; } fn fence() {}\n",
            "pub fn root() { let inner = || { fence(); }; let outer = || { inner(); }; let _ = (inner, outer); } fn fence() {}\n",
            "pub fn root() { let recursive = || { recursive(); fence(); }; let _ = recursive; } fn fence() {}\n",
            "pub fn root() { let unused = || { fence(); }; #[cfg(any())] unused(); } fn fence() {}\n",
            "pub fn root() { let unused = || { fence(); }; if false { unused(); } } fn fence() {}\n",
        ] {
            write(&root, "crates/example/src/lib.rs", source);
            assert_eq!(
                dead_enforcement_violations(
                    &root,
                    &[("fence", "crates/example/src/lib.rs", None)],
                )
                .unwrap()
                .len(),
                1,
                "uncalled nested functions and closures are not production witnesses"
            );
        }

        write(
            &root,
            "crates/example/src/lib.rs",
            "pub fn root() { fn nested() { fence(); } nested(); } fn fence() {}\n",
        );
        assert!(dead_enforcement_violations(
            &root,
            &[("fence", "crates/example/src/lib.rs", None)],
        )
        .unwrap()
        .is_empty());

        for source in [
            "pub fn root() { let used = || fence(); used(); } fn fence() {}\n",
            "pub fn root() { let used = || -> bool { fence(); true }; used(); } fn fence() {}\n",
        ] {
            write(&root, "crates/example/src/lib.rs", source);
            assert!(dead_enforcement_violations(
                &root,
                &[("fence", "crates/example/src/lib.rs", None)],
            )
            .unwrap()
            .is_empty());
        }

        write(
            &root,
            "crates/example/src/lib.rs",
            "pub fn root() { let used = || { let value = 1; let _ = value; fence(); }; used(); } fn fence() {}\n",
        );
        assert!(dead_enforcement_violations(
            &root,
            &[("fence", "crates/example/src/lib.rs", None)],
        )
        .unwrap()
        .is_empty());
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
        for criterion in 1..=6 {
            rows.push_str(&format!(
                "oc{criterion}	verified	requirement oc{criterion}	tests/direct.rs::direct_case	cargo test direct_case	-
"
            ));
        }
        for criterion in 1..=8 {
            rows.push_str(&format!(
                "px{criterion}	verified	requirement px{criterion}	tests/direct.rs::direct_case	cargo test direct_case	-
"
            ));
        }
        for criterion in 1..=5 {
            rows.push_str(&format!(
                "ac{criterion}	verified	requirement ac{criterion}	tests/direct.rs::direct_case	cargo test direct_case	-
"
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
        for inert_command in [
            "printf sh scripts/check",
            "printf bash scripts/check",
            "printf cargo xtask inspect",
        ] {
            let inert_rows = shell_rows.replace("sh scripts/check", inert_command);
            write(&root, "docs/acceptance/section-23.tsv", &inert_rows);
            assert!(
                validate_acceptance_matrix(&root).is_err(),
                "an inert command must not satisfy shell evidence: {inert_command}"
            );
        }
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
        for inert_body in [
            "fn inspect() { return; let _ = std::fs::read_to_string(\"scripts/check\"); }\n",
            "fn inspect() { #[cfg(any())] let _ = std::fs::read_to_string(\"scripts/check\"); }\n",
            "fn inspect() { if false { let _ = std::fs::read_to_string(\"scripts/check\"); } }\n",
            "fn inspect() { fn unused() { let _ = std::fs::read_to_string(\"scripts/check\"); } }\n",
            "fn inspect() { let unused = || { let value = 1; let _ = value; let _ = std::fs::read_to_string(\"scripts/check\"); }; let _ = unused; }\n",
            "fn inspect() { let unused = || std::fs::read_to_string(\"scripts/check\"); let _ = unused; }\n",
            "fn inspect() { let unused = || -> bool { let _ = std::fs::read_to_string(\"scripts/check\"); true }; let _ = unused; }\n",
        ] {
            write(&root, "xtask/src/main.rs", inert_body);
            assert!(
                validate_acceptance_matrix(&root).is_err(),
                "inspecting Rust I/O must be reachable"
            );
        }
        write(
            &root,
            "xtask/src/main.rs",
            "fn inspect() { fn used() { let _ = std::fs::read_to_string(\"scripts/check\"); } used(); }\n",
        );
        assert!(
            validate_acceptance_matrix(&root).is_ok(),
            "called nested I/O remains executable evidence"
        );
        write(
            &root,
            "xtask/src/main.rs",
            "fn inspect() { let inner = || { let _ = std::fs::read_to_string(\"scripts/check\"); }; let outer = || { inner(); }; outer(); }\n",
        );
        assert!(
            validate_acceptance_matrix(&root).is_ok(),
            "closure I/O invoked through a called outer closure remains executable evidence"
        );
        write(
            &root,
            "xtask/src/main.rs",
            "fn inspect() { let inner = || { let _ = std::fs::read_to_string(\"scripts/check\"); }; let outer = || { inner(); }; let _ = (inner, outer); }\n",
        );
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "closure I/O invoked only through an uncalled outer closure is inert"
        );
        write(
            &root,
            "xtask/src/main.rs",
            "fn inspect() { let used = || { let value = 1; let _ = value; let _ = std::fs::read_to_string(\"scripts/check\"); }; used(); }\n",
        );
        assert!(
            validate_acceptance_matrix(&root).is_ok(),
            "called multi-statement closure I/O remains executable evidence"
        );
        for source in [
            "fn inspect() { let used = || std::fs::read_to_string(\"scripts/check\"); used(); }\n",
            "fn inspect() { let used = || -> bool { let _ = std::fs::read_to_string(\"scripts/check\"); true }; used(); }\n",
        ] {
            write(&root, "xtask/src/main.rs", source);
            assert!(
                validate_acceptance_matrix(&root).is_ok(),
                "called expression or return-typed closure I/O remains executable evidence"
            );
        }
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

        let inert_test = rows.replace("cargo test direct_case", "printf cargo test direct_case");
        write(&root, "docs/acceptance/section-23.tsv", &inert_test);
        assert!(
            validate_acceptance_matrix(&root).is_err(),
            "an inert cargo-test mention must not satisfy Rust evidence"
        );

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
