use anyhow::Result;
use clap::{Parser, Subcommand};

mod gates;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Compute + print the effective OntologyFingerprint of the pack set
    /// that would be linked into `exocortex-server`. Used in CI to detect
    /// unintended ontology drift between commits.
    Fingerprint,
    /// Generate MCP + OpenAPI schemas from the operation registry.
    /// Fails if generated schemas are out of date (CI gate). Pass --write to
    /// regenerate and commit the goldens.
    GenSchemas {
        /// Regenerate the golden files instead of verifying.
        #[arg(long)]
        write: bool,
    },
    /// D1/P2 (agent-instructions PRD): regenerate the playbook's
    /// generated sections (kind catalogue, reject table) from the pack
    /// and the RejectCode enum. Fails on drift (CI gate); --write
    /// regenerates. Also enforces the instruction block's ≤300-word
    /// bound and its kind claims.
    GenPlaybook {
        /// Regenerate instead of verify.
        #[arg(long)]
        write: bool,
    },
    /// Run the R-I1/R-I5 kernel purity check: shells out to
    /// `cargo tree -p exocortex-kernel -e no-dev` and greps for banned crates.
    KernelPurity,
    /// Run the interactive-read latency benchmark (R-Lat1 SLO gate):
    /// builds both bench binaries in release mode and runs them; either one
    /// exiting non-zero (SLO breach) fails the task.
    Bench,
    /// CR-19 grep gate (R-D6): no LLM client crate or API endpoint string
    /// anywhere under `crates/` or `xtask/` source. Complements
    /// `kernel-purity` (which scopes to the kernel dep tree).
    NoLlm,
    /// CR-19 runtime audit: run the complete workspace suite with conventional
    /// provider endpoints redirected to a connection-counting loopback trap.
    MockProviderAudit,
    ProtoSync,
    WireStandalone,
    SigningHygiene,
    /// Metrics expose no identity labels or caller-controlled cardinality.
    MetricsHygiene,
    /// GATE1 (audit §2.1): one storage suite, both backends — the double
    /// always, live Falkor when FALKOR_URL is set — asserting identical
    /// results.
    StorageConformance,
    /// GATE1 (audit §2.4 / W2): the offline and ingest write validators
    /// agree on a golden verdict table.
    WritePathParity,
    /// GATE1 (audit §2.2): enforcement fns in security/invariant paths
    /// must have production callers (grep-based lint).
    DeadEnforcement,
    /// GATE1 (audit §2.3): every network-reachable endpoint rejects an
    /// unauthenticated call.
    AuthCoverage,
    /// GATE1 (audit §2.4): pack rule ids vs engine outputs; MCP result vs
    /// the registry handler.
    ArtifactEquivalence,
    /// PRD §23: validate the authoritative 30-row requirement-to-evidence matrix.
    AcceptanceCoverage,
    /// PRD §23.1/#3: one entrypoint selects every mode and each runs all rules.
    DeploymentAcceptance,
    /// PRD §23.2: non-Rust ontology catalogues are generated from the pack.
    OntologySurfaces,
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Fingerprint => fingerprint(),
        Cmd::GenSchemas { write } => gen_schemas(write),
        Cmd::GenPlaybook { write } => gen_playbook(write),
        Cmd::KernelPurity => kernel_purity(),
        Cmd::Bench => bench(),
        Cmd::NoLlm => no_llm(),
        Cmd::MockProviderAudit => mock_provider_audit(),
        Cmd::ProtoSync => proto_sync(),
        Cmd::WireStandalone => wire_standalone(),
        Cmd::SigningHygiene => signing_hygiene(),
        Cmd::MetricsHygiene => metrics_hygiene(),
        Cmd::StorageConformance => storage_conformance(),
        Cmd::WritePathParity => write_path_parity(),
        Cmd::DeadEnforcement => dead_enforcement(),
        Cmd::AuthCoverage => auth_coverage(),
        Cmd::ArtifactEquivalence => artifact_equivalence(),
        Cmd::AcceptanceCoverage => acceptance_coverage(),
        Cmd::DeploymentAcceptance => deployment_acceptance(),
        Cmd::OntologySurfaces => ontology_surfaces(),
    }
}

fn deployment_acceptance() -> Result<()> {
    let installer = std::process::Command::new("sh")
        .arg("scripts/test-release-installer.sh")
        .status()?;
    anyhow::ensure!(
        installer.success(),
        "release installer checksum test failed"
    );
    let compose =
        std::fs::read_to_string("crates/exocortex-cluster/tests/docker-compose-cluster.yml")?;
    let storage_compose =
        std::fs::read_to_string("crates/exocortex-storage/tests/docker-compose.yml")?;
    let mut workflow_sources = Vec::new();
    for entry in std::fs::read_dir(".github/workflows")? {
        let path = entry?.path();
        if matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("yml" | "yaml")
        ) {
            workflow_sources.push((path.display().to_string(), std::fs::read_to_string(&path)?));
        }
    }
    workflow_sources.sort_by(|left, right| left.0.cmp(&right.0));
    let workflows = workflow_sources
        .iter()
        .map(|(path, source)| (path.as_str(), source.as_str()))
        .collect::<Vec<_>>();
    let dockerfile = std::fs::read_to_string("Dockerfile")?;
    let workspace_manifest = std::fs::read_to_string("Cargo.toml")?;
    let ingest_manifest = std::fs::read_to_string("crates/exocortex-ingest/Cargo.toml")?;
    let chaos_script = std::fs::read_to_string("scripts/chaos-leader-kill.sh")?;
    let protoc_installer = std::fs::read_to_string("scripts/install-protoc.sh")?;
    let model_fetcher = std::fs::read_to_string("scripts/fetch-embedding-model.sh")?;
    let release_installer = std::fs::read_to_string("scripts/release-install.sh")?;
    let verify_release = std::fs::read_to_string("scripts/verify-release.sh")?;
    let embedding_source = std::fs::read_to_string("crates/exocortex-ingest/src/embedding.rs")?;
    let server_main = std::fs::read_to_string("crates/exocortex-server/src/main.rs")?;
    validate_release_hardening(
        &workflows,
        &dockerfile,
        &protoc_installer,
        &[&compose, &storage_compose],
    )?;
    validate_fastembed_release(
        &workflows,
        &dockerfile,
        &verify_release,
        &model_fetcher,
        &release_installer,
        &embedding_source,
        &server_main,
    )?;
    validate_fastembed_dependency_contract(&workspace_manifest, &ingest_manifest)?;
    validate_chaos_compose(&compose)?;
    validate_chaos_script(&chaos_script)?;
    run(
        &[
            "check",
            "-p",
            "exocortex-server",
            "--all-targets",
            "--features",
            "fastembed",
        ],
        &[],
    )?;
    for node in ["node1", "node2", "node3"] {
        let marker = format!("  {node}:\n");
        let section = compose
            .split_once(&marker)
            .map(|(_, tail)| tail)
            .and_then(|tail| {
                tail.split_once("\n  node")
                    .map(|(section, _)| section)
                    .or(Some(tail))
            })
            .ok_or_else(|| anyhow::anyhow!("chaos compose is missing service {node}"))?;
        anyhow::ensure!(
            section.contains("    build:\n")
                && section.contains("      context: ../../..\n")
                && section.contains("      dockerfile: Dockerfile\n"),
            "chaos compose service {node} must build the current root Dockerfile; image-only services can silently run a stale local tag"
        );
    }
    run(
        &[
            "build",
            "-p",
            "exocortex-client",
            "-p",
            "exocortex-server",
            "-p",
            "exocortex-worker",
        ],
        &[],
    )?;
    let bin_dir = std::path::Path::new("target/debug").canonicalize()?;
    for binary in ["exocortex-mcp-client", "exocortex-node", "exocortex-worker"] {
        let help = std::process::Command::new(bin_dir.join(binary))
            .arg("--help")
            .output()?;
        anyhow::ensure!(help.status.success(), "{binary} --help failed");
        let help = String::from_utf8(help.stdout)?;
        for secret_flag in ["--auth-token", "--hmac-key", "--cluster-secret"] {
            anyhow::ensure!(
                !help.contains(secret_flag),
                "{binary} exposes secret-bearing argv flag {secret_flag}"
            );
        }
    }
    let topology = std::process::Command::new("sh")
        .arg("scripts/tests/exocortex-entrypoint.sh")
        .status()?;
    anyhow::ensure!(topology.success(), "entrypoint topology test failed");
    probe_deployed_rules(&bin_dir)?;
    run(
        &[
            "test",
            "-p",
            "exocortex-client",
            "--test",
            "standalone_wrapper",
        ],
        &[],
    )?;
    println!("deployment-acceptance ok: chaos nodes build the current image; the installed entrypoint entered all 3 selected topologies; all 9 rules executed after topology initialization; standalone served a real MCP request while supervising its node; no secret-bearing argv flags");
    Ok(())
}

fn probe_deployed_rules(bin_dir: &std::path::Path) -> Result<()> {
    let fixture = std::env::temp_dir().join(format!(
        "exocortex-deployment-rule-probe-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&fixture);
    std::fs::create_dir_all(&fixture)?;
    let result = (|| -> Result<()> {
        let client = std::process::Command::new("scripts/exocortex")
            .args([
                "--mode",
                "mcp-client",
                "--verify-rules",
                "--data-dir",
                fixture.join("client").to_str().unwrap(),
            ])
            .env("EXOCORTEX_BIN_DIR", bin_dir)
            .output()?;
        ensure_rule_probe("mcp-client", &client)?;

        let principal_policy = fixture.join("principals.json");
        std::fs::write(
            &principal_policy,
            r#"[{"bearer_token":"deployment-rule-probe-token-00000000","org_id":"org","user_id":"probe","project_ids":[],"team_ids":[],"max_visibility":3}]"#,
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&principal_policy, std::fs::Permissions::from_mode(0o600))?;
        }
        let source_policy = fixture.join("sources.json");
        std::fs::write(
            &source_policy,
            r#"[{"org_id":"org","source_uri":"deployment://probe","producer_id":"probe","ceiling":3,"producer_kind":4,"hmac_key":"4242424242424242424242424242424242424242424242424242424242424242"}]"#,
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&source_policy, std::fs::Permissions::from_mode(0o600))?;
        }
        let backend = std::process::Command::new("scripts/exocortex")
            .args([
                "--mode",
                "backend-node",
                "--verify-rules",
                "--storage",
                "memory",
                "--bind",
                "127.0.0.1:0",
                "--allow-plaintext-loopback",
                "--gossip-addr",
                "127.0.0.1:0",
                "--principal-policy",
                principal_policy.to_str().unwrap(),
                "--source-policy",
                source_policy.to_str().unwrap(),
            ])
            .env("EXOCORTEX_BIN_DIR", bin_dir)
            .env(
                "EXOCORTEX_CLUSTER_SECRET",
                "4242424242424242424242424242424242424242424242424242424242424242",
            )
            .output()?;
        ensure_rule_probe("backend-node", &backend)?;
        Ok(())
    })();
    let _ = std::fs::remove_dir_all(&fixture);
    result
}

fn ensure_rule_probe(mode: &str, output: &std::process::Output) -> Result<()> {
    anyhow::ensure!(
        output.status.success(),
        "{mode} deployed rule probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    anyhow::ensure!(
        stdout.contains(&format!("rules-ok mode={mode} count=9")),
        "{mode} deployed rule probe did not execute all 9 rules: {stdout}"
    );
    Ok(())
}

fn validate_release_hardening(
    workflows: &[(&str, &str)],
    dockerfile: &str,
    protoc_installer: &str,
    compose_files: &[&str],
) -> Result<()> {
    validate_workflows(workflows)?;
    validate_dockerfile(dockerfile)?;
    validate_protoc_installer(protoc_installer)?;
    validate_compose_files(compose_files)
}

fn validate_fastembed_release(
    workflows: &[(&str, &str)],
    dockerfile: &str,
    verify_release: &str,
    model_fetcher: &str,
    release_installer: &str,
    embedding_source: &str,
    server_main: &str,
) -> Result<()> {
    const REVISION: &str = "ea104dacec62c0de699686887e3f920caeb4f3e3";
    const DIRECTORY: &str = "Xenova_bge-small-en-v1.5-ea104dacec62c0de699686887e3f920caeb4f3e3";
    const DIGESTS: [&str; 5] = [
        "828e1496d7fabb79cfa4dcd84fa38625c0d3d21da474a00f08db0f559940cf35",
        "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
        "fa73f90bf92c8cace1fbcb709626306f2bdbc9ea3e5b5f94b440df9b6aa56350",
        "b6d346be366a7d1d48332dbc9fdf3bf8960b5d879522b7799ddba59e76237ee3",
        "9261e7d79b44c8195c1cada2b453e55b00aeb81e907a6664974b4d7776172ab3",
    ];
    let release = workflows
        .iter()
        .find(|(path, _)| path.ends_with("release.yml") || path.ends_with("release.yaml"))
        .map(|(_, source)| *source)
        .ok_or_else(|| anyhow::anyhow!("release workflow is missing"))?;
    anyhow::ensure!(
        release.contains("--features exocortex-server/fastembed")
            && release.contains("scripts/fetch-embedding-model.sh \"dist/$DIST/models\""),
        "release artifacts must enable FastEmbed and package its verified model sidecar"
    );
    anyhow::ensure!(
        dockerfile.contains("-p exocortex-server --bin exocortex-node --features fastembed"),
        "Docker image must enable the server fastembed feature"
    );
    anyhow::ensure!(
        verify_release
            .contains("cargo check -p exocortex-server --all-targets --features fastembed"),
        "verify-release must compile the exact production fastembed feature"
    );
    anyhow::ensure!(
        dockerfile.contains(&format!("EXOCORTEX_BGE_SMALL_MODEL_DIR=/opt/exocortex/models/{DIRECTORY} /repo/target/release/exocortex-node --verify-embedder")) && dockerfile.contains(
            "COPY --from=build --chown=65532:65532 /opt/exocortex/models /opt/exocortex/models",
        ),
        "Docker image must execute the production embedder and carry its verified immutable sidecar"
    );
    anyhow::ensure!(
        !dockerfile.contains("/resolve/main/")
            && !model_fetcher.contains("/resolve/main/")
            && dockerfile.matches(&format!("/resolve/{REVISION}/")).count() == 5
            && model_fetcher.contains(&format!("revision=\"{REVISION}\""))
            && DIGESTS.iter().all(|digest| {
                dockerfile.contains(&format!("--checksum=sha256:{digest}"))
                    && model_fetcher.contains(digest)
                    && embedding_source.contains(digest)
            }),
        "production model must use one immutable upstream revision and matching build, fetch, and runtime digests"
    );
    anyhow::ensure!(
        embedding_source.contains("TextEmbedding::try_new_from_user_defined")
            && !embedding_source.contains("TextEmbedding::try_new(")
            && embedding_source.contains(&format!(
                "hf:Xenova/bge-small-en-v1.5@{REVISION}"
            ))
            && embedding_source.contains("exocortex_wire::signing::content_digest_hex")
            && embedding_source.contains("actual_sha256 != expected_sha256")
            && embedding_source.contains("BGE_SMALL_MAX_LENGTH: usize = 512")
            && embedding_source.contains("with_max_length(BGE_SMALL_MAX_LENGTH)"),
        "production embedder must verify local bytes, construct offline, and stamp the immutable revision"
    );
    anyhow::ensure!(
        release_installer.contains("$src/models")
            && release_installer.contains("share/exocortex/models"),
        "release installer must install the packaged model sidecar"
    );
    anyhow::ensure!(
        server_main.contains("const EXPECTED_PREFIX: [f32; 8]")
            && server_main.contains("max_error <= 1.0e-4")
            && server_main.contains("bge-small known-output mismatch")
            && server_main.contains("bge-small long-input truncation probe")
            && server_main.contains("repeat(400)"),
        "release probe must enforce the pinned model's known output and established input window"
    );
    Ok(())
}

fn validate_fastembed_dependency_contract(
    workspace_manifest: &str,
    ingest_manifest: &str,
) -> Result<()> {
    anyhow::ensure!(
        workspace_manifest.contains(
            "tonic       = { version = \"0.12\", features = [\"tls\", \"tls-native-roots\", \"gzip\"] }",
        ),
        "remote HTTPS gRPC clients must compile with an explicit trust-root source"
    );
    anyhow::ensure!(
        workspace_manifest.contains(
            "fastembed = { version = \"=5.2.0\", default-features = false, features = [\"ort-download-binaries\"] }",
        ),
        "production FastEmbed must stay exact-pinned without an online model downloader"
    );
    anyhow::ensure!(
        workspace_manifest.contains("image = \"=0.25.5\"")
            && !workspace_manifest.contains("ort-sys ="),
        "production embedding dependencies must preserve the Rust 1.85 image boundary without a direct ort-sys resolver pin"
    );
    anyhow::ensure!(
        ingest_manifest.contains("fastembed = [\"dep:fastembed\", \"dep:image\"]")
            && !ingest_manifest.contains("sha2 =")
            && !ingest_manifest.contains("ort-sys ="),
        "ingest FastEmbed feature must use wire-owned hashing, preserve the Rust-1.85 image boundary, and avoid direct sha2/ort-sys dependencies"
    );
    Ok(())
}

const MUTABLE_PACKAGE_COMMANDS: &[&str] = &[
    "apt-get ",
    "apt install ",
    "apk add ",
    "brew install ",
    "dnf install ",
    "microdnf install ",
    "yum install ",
    "zypper install ",
];

fn rejects_mutable_package_resolution(source: &str) -> bool {
    MUTABLE_PACKAGE_COMMANDS
        .iter()
        .any(|command| source.contains(command))
}

fn validate_workflows(workflows: &[(&str, &str)]) -> Result<()> {
    anyhow::ensure!(!workflows.is_empty(), "no GitHub workflows found");
    let mut saw_release = false;
    for (path, workflow) in workflows {
        anyhow::ensure!(
            workflow.contains("permissions:\n  contents: read\n"),
            "{path} must default to read-only repository permission"
        );
        anyhow::ensure!(
            !rejects_mutable_package_resolution(workflow),
            "{path} must not install build inputs from a mutable package repository"
        );
        let required_installs = if path.ends_with("release.yml") || path.ends_with("release.yaml") {
            2
        } else if path.ends_with("ci.yml") || path.ends_with("ci.yaml") {
            1
        } else {
            0
        };
        anyhow::ensure!(
            workflow.matches("scripts/install-protoc.sh").count() >= required_installs,
            "{path} must install checksum-verified protoc in every build environment"
        );
        if path.ends_with("release.yml") || path.ends_with("release.yaml") {
            saw_release = true;
            anyhow::ensure!(
                workflow.contains("--features exocortex-server/fastembed"),
                "release workflow must build the server with the production fastembed feature"
            );
            let release_job = workflow
                .split_once("\n  release:\n")
                .map(|(_, section)| section)
                .ok_or_else(|| anyhow::anyhow!("release workflow is missing the release job"))?;
            anyhow::ensure!(
                release_job.contains("    permissions:\n      contents: write\n"),
                "only the release job may receive contents: write"
            );
            anyhow::ensure!(
                release_job.contains("scripts/publish-release-assets.sh"),
                "release job must use the immutable draft-first asset publisher"
            );
            anyhow::ensure!(
                !release_job.contains("--clobber") && !release_job.contains("|| true"),
                "release job must not suppress conflicts or overwrite repeated-tag assets"
            );
        }
        for line in workflow.lines() {
            let directive = line.trim().trim_start_matches("- ");
            if !directive.starts_with("uses:") {
                continue;
            }
            let action = directive
                .split_once('@')
                .map(|(_, revision)| revision.split_whitespace().next().unwrap_or_default())
                .unwrap_or_default();
            anyhow::ensure!(
                action.len() == 40 && action.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "{path} action must be pinned to a full commit SHA: {line}"
            );
        }
    }
    anyhow::ensure!(saw_release, "release workflow is missing");
    Ok(())
}

fn validate_dockerfile(dockerfile: &str) -> Result<()> {
    let docker_bases = dockerfile
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("FROM "))
        .collect::<Vec<_>>();
    anyhow::ensure!(!docker_bases.is_empty(), "Dockerfile has no base image");
    for base in docker_bases {
        let image = base
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| anyhow::anyhow!("malformed Dockerfile base: {base}"))?;
        if image == "scratch" || image == "protoc-${TARGETARCH}" {
            continue;
        }
        let digest = image
            .rsplit_once("@sha256:")
            .map(|(_, digest)| digest)
            .unwrap_or_default();
        anyhow::ensure!(
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "Dockerfile base image must be pinned to a full sha256 digest: {base}"
        );
    }
    anyhow::ensure!(
        !rejects_mutable_package_resolution(dockerfile),
        "Dockerfile must not resolve build or runtime inputs through a mutable package repository"
    );
    anyhow::ensure!(
        dockerfile.matches("ADD --checksum=sha256:").count() == 7
            && dockerfile.contains("protoc-28.3-linux-x86_64.zip")
            && dockerfile.contains("protoc-28.3-linux-aarch_64.zip")
            && dockerfile.contains("gcr.io/distroless/cc-debian12:nonroot@sha256:"),
        "Dockerfile must checksum both protoc archives and all five model artifacts, and use the pinned CA-root runtime"
    );
    anyhow::ensure!(
        dockerfile
            .lines()
            .any(|line| line.trim() == "USER 65532:65532"),
        "runtime Dockerfile must select the unprivileged exocortex user"
    );
    anyhow::ensure!(
        dockerfile.contains("-p exocortex-server --bin exocortex-node --features fastembed"),
        "Dockerfile must build exocortex-node with the production fastembed feature"
    );
    anyhow::ensure!(
        dockerfile.contains("RUN command -v pkg-config && pkg-config --exists openssl"),
        "Dockerfile FastEmbed builder must prove the ort-sys native-TLS build toolchain before compiling"
    );
    Ok(())
}

fn validate_protoc_installer(protoc_installer: &str) -> Result<()> {
    anyhow::ensure!(
        protoc_installer.contains("readonly PROTOC_VERSION=")
            && protoc_installer.contains(
                "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}",
            )
            && protoc_installer.contains(
                "curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error",
            )
            && protoc_installer.contains("actual_sha256")
            && protoc_installer.contains("actual_sha256\" != \"$expected_sha256"),
        "protoc installer must use a fixed upstream release and fail closed on checksum mismatch"
    );
    let checksums = protoc_installer
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("readonly expected_sha256="))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        checksums.len() == 4
            && checksums.iter().all(|checksum| {
                checksum.len() == 64 && checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
            }),
        "protoc installer must pin a full sha256 checksum for every supported host"
    );
    Ok(())
}

fn validate_compose_files(compose_files: &[&str]) -> Result<()> {
    for compose in compose_files {
        for port in compose
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with("- \"") && line.ends_with('"'))
        {
            anyhow::ensure!(
                port.starts_with("- \"127.0.0.1:"),
                "test service port must bind to loopback: {port}"
            );
        }
        for image in compose
            .lines()
            .map(str::trim)
            .filter_map(|line| line.strip_prefix("image:"))
            .map(str::trim)
        {
            let locally_built = image == "exocortex-node:local";
            let immutable_external = image.split_once("@sha256:").is_some_and(|(_, digest)| {
                digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
            });
            anyhow::ensure!(
                locally_built || immutable_external,
                "external Compose image must be pinned by full sha256 digest: {image}"
            );
        }
    }
    Ok(())
}

fn validate_chaos_compose(compose: &str) -> Result<()> {
    const PRINCIPAL_INSTALL: &str =
        "install -m 0600 -o 65532 -g 65532 /input/principal-policy.json /output/principal-policy.json";
    const SOURCE_INSTALL: &str =
        "install -m 0600 -o 65532 -g 65532 /input/source-policy.json /output/source-policy.json";
    anyhow::ensure!(
        compose.contains("  policy-init:\n")
            && compose.contains(PRINCIPAL_INSTALL)
            && compose.contains(SOURCE_INSTALL),
        "chaos compose must stage both credential policies owner-only through its pinned init service"
    );
    anyhow::ensure!(
        compose
            .matches("condition: service_completed_successfully")
            .count()
            == 3
            && compose
                .matches("policy-data:/run/exocortex-policies:ro")
                .count()
                == 3,
        "every chaos node must wait for and mount the owner-only staged policy volume"
    );
    anyhow::ensure!(
        !compose.contains("source-policy.empty.json:/etc/exocortex")
            && !compose.contains("principal-policy.dev.json:/etc/exocortex"),
        "chaos nodes must not bind-mount repository credential policies with host-controlled modes"
    );
    Ok(())
}

fn validate_chaos_script(script: &str) -> Result<()> {
    let probe = "--test fencing_live inflight_stale_dreams_write_is_fenced_after_takeover_live";
    anyhow::ensure!(
        script.contains("PRINCIPAL_POLICY=crates/exocortex-cluster/tests/principal-policy.dev.json")
            && script.contains("AUTH_TOKEN=$(jq -er")
            && script.contains("-H \"Authorization: Bearer $AUTH_TOKEN\"")
            && script.matches("cluster_health \"$port\"").count() == 2,
        "chaos leader polling must authenticate both protected health loops from the dev principal policy"
    );
    let probe_position = script.find(probe).ok_or_else(|| {
        anyhow::anyhow!("chaos harness must run the live in-flight Dreams fence probe")
    })?;
    let pass_position = script
        .find("PASS: authenticated takeover and no-zombie Dreams write fencing")
        .ok_or_else(|| {
            anyhow::anyhow!("chaos harness must report combined takeover/fencing success")
        })?;
    anyhow::ensure!(
        script.contains("CHAOS_OLD_OWNER=\"$leader\" CHAOS_NEW_OWNER=\"$new_leader\"")
            && script.contains("FALKOR_URL=falkor://127.0.0.1:16379")
            && script.contains("--features integration")
            && probe_position < pass_position,
        "chaos success must follow a live stale Dreams mutation and authoritative no-residue probe"
    );
    Ok(())
}

fn ontology_surfaces() -> Result<()> {
    gen_playbook(false)?;
    let pack = exocortex_pack_dev_v1::pack_def();
    let playbook = std::fs::read_to_string("crates/exocortex-client/src/playbook/v1_0_0.md")?;
    let authored: Vec<_> = pack
        .kinds
        .iter()
        .filter(|kind| kind.id.is_kernel() || kind.id.local_part() < 0x4000)
        .collect();
    for name in authored.iter().map(|kind| &kind.display_name) {
        anyhow::ensure!(
            playbook.contains(name.as_str()),
            "generated playbook omitted pack-owned ontology name `{name}`"
        );
    }
    println!(
        "ontology-surfaces ok: authoritative non-Rust catalogue covers {} kinds from the Rust pack",
        authored.len()
    );
    Ok(())
}

fn acceptance_coverage() -> Result<()> {
    gates::validate_acceptance_matrix(std::path::Path::new("."))?;
    println!("acceptance-coverage ok: every PRD §23 criterion has executable evidence or explicit plan tracking");
    Ok(())
}

fn cargo() -> String {
    std::env::var("CARGO")
        .map(|c| c.trim().to_string())
        .unwrap_or_else(|_| "cargo".into())
}

fn run(args: &[&str], env: &[(&str, &str)]) -> Result<()> {
    let mut cmd = std::process::Command::new(cargo());
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let status = cmd.status()?;
    anyhow::ensure!(status.success(), "gate failed: cargo {:?}", args);
    Ok(())
}

fn storage_test_listing(target: &str) -> Result<String> {
    let output = std::process::Command::new(cargo())
        .args([
            "test",
            "-p",
            "exocortex-storage",
            "--features",
            "integration",
            "--test",
            target,
            "--",
            "--list",
        ])
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "storage-conformance: failed to list live target {target}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .map_err(|error| anyhow::anyhow!("storage-conformance: non-UTF-8 test listing: {error}"))
}

/// §2.1: the storage suite against the double, plus the live Falkor
/// integration suite when FALKOR_URL is present.
fn storage_conformance() -> Result<()> {
    gates::validate_storage_targets(std::path::Path::new("."))?;
    for (target, canary) in gates::STORAGE_LIVE_CANARIES {
        let listing = storage_test_listing(target)?;
        gates::validate_storage_target_listing(target, canary, &listing)?;
    }
    println!("storage-conformance: live target canaries LISTED (backend suite not yet executed)");
    run(&["test", "-p", "exocortex-storage"], &[])?;
    println!("storage-conformance: in-memory suite PASS");
    if std::env::var("FALKOR_URL").is_ok_and(|url| !url.is_empty()) {
        println!("storage-conformance: live Falkor suite (FALKOR_URL set)");
        run(
            &[
                "test",
                "-p",
                "exocortex-storage",
                "--features",
                "integration",
                "--test",
                "integration",
                "--test",
                "fencing_live",
            ],
            &[],
        )?;
        println!("storage-conformance: live Falkor suite PASS");
    } else {
        println!(
            "storage-conformance: live Falkor suite UNEXECUTED — FALKOR_URL is unset or empty"
        );
    }
    println!("storage-conformance: available suites complete");
    Ok(())
}

/// W2: golden verdict table through BOTH validators.
fn write_path_parity() -> Result<()> {
    run(
        &[
            "test",
            "-p",
            "exocortex-ingest",
            "--test",
            "write_path_parity",
        ],
        &[],
    )?;
    println!("write-path-parity ok: offline and ingest validators agree row for row");
    Ok(())
}

/// §2.2: the listed enforcement functions must appear in production (non
/// test, non-definition) sources.
fn dead_enforcement() -> Result<()> {
    let violations =
        gates::dead_enforcement_violations(std::path::Path::new("."), gates::DEAD_CONTROLS)?;
    anyhow::ensure!(
        violations.is_empty(),
        "dead-enforcement FAILED:\n{}",
        violations.join("\n")
    );
    println!("dead-enforcement ok: every listed control has a live caller");
    Ok(())
}

/// §2.3: detailed endpoints require auth; readiness is minimal and public.
fn auth_coverage() -> Result<()> {
    run(
        &["test", "-p", "exocortex-server", "--test", "auth_coverage"],
        &[],
    )?;
    println!("auth-coverage ok: detailed endpoints require auth; readiness is minimal/public");
    Ok(())
}

/// §2.4: pack↔engine rule equivalence + MCP↔registry result equivalence.
fn artifact_equivalence() -> Result<()> {
    run(
        &[
            "test",
            "-p",
            "exocortex-reasoning",
            "--test",
            "rules",
            "pack_rule_ids_match_engine_outputs",
        ],
        &[],
    )?;
    run(
        &[
            "test",
            "-p",
            "exocortex-client",
            "--test",
            "stdio_smoke",
            "mcp_get_memory_shape_matches_registry",
        ],
        &[],
    )?;
    println!("artifact-equivalence ok: pack rules == engine rules; MCP result == registry result");
    Ok(())
}

/// `cargo xtask fingerprint` — compute and print the effective
/// `OntologyFingerprint` of the linked pack set. Must be byte-stable across
/// runs on the same commit (M1 acceptance).
fn fingerprint() -> Result<()> {
    // Force-link the pack crate so its inventory ctor (`.init_array`) runs in
    // this binary; an unreferenced dependency is not linked.
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let onto = exocortex_kernel::pack::load_registered_packs()?;
    let fp = onto.fingerprint.0;
    let mut hex = String::with_capacity(fp.len() * 2);
    for b in fp {
        use std::fmt::Write as _;
        let _ = write!(hex, "{b:02x}");
    }
    println!("{hex}");
    Ok(())
}

/// `cargo xtask gen-schemas` — emit MCP tool + OpenAPI catalogues from the
/// operation registry and verify the checked-in goldens match (schema-drift
/// CI gate, §21.2). Regenerate with --write.
fn gen_schemas(write: bool) -> Result<()> {
    let mut openapi = serde_json::json!({
        "openapi": "3.1.0",
        "info": { "title": "exocortex", "version": env!("CARGO_PKG_VERSION") },
        "paths": {},
    });
    let mut mcp_tools = serde_json::json!([]);
    let mut seen = std::collections::HashSet::new();
    for e in exocortex_ops::entries() {
        anyhow::ensure!(seen.insert(e.name), "duplicate op name {}", e.name);
        let input_schema = (e.input_schema)();
        anyhow::ensure!(
            input_schema
                .schema
                .metadata
                .as_ref()
                .and_then(|m| m.title.clone())
                .is_some(),
            "{}: input schema missing title",
            e.name
        );
        openapi["paths"][e.http_path] = serde_json::json!({
            format!("{:?}", (e.http_method)()).to_lowercase(): {
                "operationId": e.name,
                "requestBody": { "content": { "application/json": { "schema": input_schema } } },
                "responses": { "200": { "description": "OK" } },
            }
        });
        mcp_tools.as_array_mut().unwrap().push(serde_json::json!({
            "name": e.mcp_tool_name,
            "description": e.name,
            "inputSchema": (e.input_schema)(),
        }));
    }
    for (name, doc) in [("openapi.json", &openapi), ("mcp-tools.json", &mcp_tools)] {
        let path = format!("crates/exocortex-ops/tests/golden/{name}");
        let serialized = serde_json::to_string_pretty(doc)?;
        if write {
            std::fs::create_dir_all("crates/exocortex-ops/tests/golden")?;
            std::fs::write(
                &path,
                serialized
                    + "
",
            )?;
            println!("wrote {path}");
        } else {
            let golden = std::fs::read_to_string(&path).map_err(|_| {
                anyhow::anyhow!(
                    "golden {path} missing; run `cargo xtask gen-schemas --write` and commit it"
                )
            })?;
            anyhow::ensure!(
                golden.trim() == serialized.trim(),
                "schema drift in {name}; run `cargo xtask gen-schemas --write` and commit the diff"
            );
        }
    }
    println!("parity ok: {} operations", exocortex_ops::entries().len());
    Ok(())
}

/// `cargo xtask kernel-purity` — R-I1/R-I5/CR-19/CR-26 defence. Runs
/// `cargo tree -p exocortex-kernel -e no-dev` and fails if any banned crate
/// appears in the kernel's dependency graph. HTTP clients (`reqwest`) are
/// legitimate in server/client crates but must never reach the kernel.
fn kernel_purity() -> Result<()> {
    let pack_coupling = gates::kernel_pack_coupling_violations(std::path::Path::new("."))?;
    anyhow::ensure!(
        pack_coupling.is_empty(),
        "kernel-purity FAILED: {}",
        pack_coupling.join("; ")
    );
    let boundary = gates::kernel_boundary_violations(std::path::Path::new("."))?;
    anyhow::ensure!(
        boundary.is_empty(),
        "kernel-purity FAILED: {}",
        boundary.join("; ")
    );
    let cypher = gates::cypher_outside_storage_violations(std::path::Path::new("."))?;
    anyhow::ensure!(
        cypher.is_empty(),
        "kernel-purity FAILED: {}",
        cypher.join("; ")
    );
    for crate_name in [
        "exocortex-kernel",
        "exocortex-adapter-sdk",
        "exocortex-worker",
    ] {
        let out = std::process::Command::new(cargo())
            .args(["tree", "-p", crate_name, "--all-features", "-e", "no-dev"])
            .output()?;
        anyhow::ensure!(
            out.status.success(),
            "cargo tree for {crate_name} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let tree = String::from_utf8_lossy(&out.stdout);
        let violations = gates::dependency_tree_violations(crate_name, &tree);
        anyhow::ensure!(
            violations.is_empty(),
            "kernel-purity FAILED for {crate_name}: {}",
            violations.join("; ")
        );
    }
    verify_pack_link_contract()?;
    println!(
        "kernel-purity ok: kernel pure; SDK wire-only; worker kernel-free; pack omission fails linking"
    );
    Ok(())
}

/// §23 #25: the production pack requirement is a linker contract, not merely
/// a runtime inventory check. Build two tiny binaries against the current
/// sources: the packless one must fail to link and the pack-linked one must
/// succeed.
fn verify_pack_link_contract() -> Result<()> {
    let root = std::env::current_dir()?.canonicalize()?;
    let fixture = root.join("target/xtask-pack-link-contract");
    std::fs::create_dir_all(fixture.join("src"))?;
    let kernel = root.join("crates/exocortex-kernel");
    let pack = root.join("crates/exocortex-pack-dev-v1");
    let manifest = format!(
        "[workspace]\n\n[package]\nname = \"pack-link-contract\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[dependencies]\nexocortex-kernel = {{ path = {:?} }}\nexocortex-pack-dev-v1 = {{ path = {:?}, optional = true }}\n\n[features]\nwith-pack = [\"dep:exocortex-pack-dev-v1\"]\n\n[[bin]]\nname = \"pack-link-contract\"\npath = \"src/main.rs\"\n",
        kernel, pack
    );
    std::fs::write(fixture.join("Cargo.toml"), manifest)?;
    std::fs::copy(root.join("Cargo.lock"), fixture.join("Cargo.lock"))?;
    std::fs::write(
        fixture.join("src/main.rs"),
        "extern \"C\" { fn exocortex_required_ontology_pack_anchor(); }\nfn main() {\n    #[cfg(feature = \"with-pack\")]\n    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def());\n    // SAFETY: the optional pack supplies this inert linkage anchor.\n    unsafe { exocortex_required_ontology_pack_anchor() }\n}\n",
    )?;

    let target = fixture.join("target");
    let build = |with_pack: bool| -> Result<std::process::Output> {
        let mut command = std::process::Command::new(cargo());
        command
            .args(["build", "--manifest-path"])
            .arg(fixture.join("Cargo.toml"))
            .args(["--target-dir"])
            .arg(&target)
            .arg("--offline");
        if with_pack {
            command.args(["--features", "with-pack"]);
        }
        Ok(command.output()?)
    };
    let omitted = build(false)?;
    anyhow::ensure!(
        !omitted.status.success(),
        "pack-link contract FAILED: packless fixture linked successfully"
    );
    let omitted_stderr = String::from_utf8_lossy(&omitted.stderr);
    anyhow::ensure!(
        omitted_stderr.contains("exocortex_required_ontology_pack_anchor"),
        "packless fixture failed for the wrong reason: {omitted_stderr}"
    );
    let linked = build(true)?;
    anyhow::ensure!(
        linked.status.success(),
        "pack-link contract FAILED with dev-v1 linked: {}",
        String::from_utf8_lossy(&linked.stderr)
    );
    Ok(())
}

/// `cargo xtask bench` — R-Lat1 SLO gate. Runs both bench binaries
/// (`search`, `khop`) in release mode; each binary enforces its own budget
/// and exits non-zero on breach.
fn bench() -> Result<()> {
    let cargo = std::env::var("CARGO")?.trim().to_string();
    for (pkg, bench) in [
        ("exocortex-cache", "search"),
        ("exocortex-cache", "updates"),
        ("exocortex-reasoning", "khop"),
    ] {
        println!("==> cargo bench -p {pkg} --bench {bench}");
        let status = std::process::Command::new(&cargo)
            .args(["bench", "-p", pkg, "--bench", bench])
            .status()?;
        anyhow::ensure!(
            status.success(),
            "SLO gate FAILED for {bench} (R-Lat1); see the p50/p99 lines above"
        );
    }
    if std::env::var("FALKOR_URL").is_ok_and(|url| !url.is_empty()) {
        println!("==> cargo test -p exocortex-storage --features integration --test live_bench");
        let status = std::process::Command::new(&cargo)
            .args([
                "test",
                "-p",
                "exocortex-storage",
                "--features",
                "integration",
                "--test",
                "live_bench",
                "--release",
                "--",
                "--nocapture",
            ])
            .status()?;
        anyhow::ensure!(status.success(), "live Falkor SLO gate FAILED");
    } else {
        eprintln!(
            "UNEXECUTED: live Falkor SLO target requires FALKOR_URL; in-memory benchmarks passed"
        );
    }
    println!("bench ok: search, update/hydration, reasoning, and available live-backend SLO gates green (R-Lat1)");
    Ok(())
}

/// `cargo xtask no-llm` — CR-19/R-D6 grep gate. Walks `crates/` and
/// `xtask/` sources; fails on any LLM client crate identifier or API
/// endpoint string. Model names in docs/comments are not violations; the
/// gate matches dependency identifiers and wire endpoints. Scans `crates/`
/// only (this file's own banned list must not self-match).
fn no_llm() -> Result<()> {
    let violations = gates::no_llm_violations(std::path::Path::new("."))?;
    anyhow::ensure!(
        violations.is_empty(),
        "no-llm FAILED (CR-19):\n{}",
        violations.join("\n")
    );
    println!("no-llm ok: no LLM client crate or endpoint in executable workspace sources");
    Ok(())
}

fn mock_provider_audit() -> Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let stop = Arc::new(AtomicBool::new(false));
    let connections = Arc::new(AtomicUsize::new(0));
    let thread_stop = stop.clone();
    let thread_connections = connections.clone();
    let trap = std::thread::spawn(move || {
        while !thread_stop.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((_stream, _)) => {
                    thread_connections.fetch_add(1, Ordering::SeqCst);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::park_timeout(std::time::Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });

    let cargo = std::env::var("CARGO")?.trim().to_string();
    let mut command = std::process::Command::new(cargo);
    command.args([
        "test",
        "--workspace",
        "--features",
        "exocortex-adapter-sdk/testing",
        "--no-fail-fast",
    ]);
    for name in [
        ["OPENAI", "_BASE_URL"].concat(),
        ["ANTHROPIC", "_BASE_URL"].concat(),
        ["GOOGLE", "_AI_BASE_URL"].concat(),
        ["MISTRAL", "_BASE_URL"].concat(),
        ["COHERE", "_BASE_URL"].concat(),
    ] {
        command.env(name, &endpoint);
    }
    let status = command.status()?;
    stop.store(true, Ordering::SeqCst);
    trap.thread().unpark();
    trap.join()
        .map_err(|_| anyhow::anyhow!("mock provider trap thread panicked"))?;
    anyhow::ensure!(
        status.success(),
        "workspace suite failed during provider audit"
    );
    anyhow::ensure!(
        connections.load(Ordering::SeqCst) == 0,
        "mock-provider audit observed an outbound inference connection"
    );
    println!("mock-provider-audit ok: full workspace suite made zero provider connections");
    Ok(())
}

/// Wire vendors the protos for publishable tarballs (B8-era fix); the
/// root proto/ stays authoritative. This gate fails when they diverge.
fn proto_sync() -> anyhow::Result<()> {
    for name in ["ingest.proto", "cluster.proto", "sse.proto"] {
        let root = std::fs::read_to_string(format!("proto/{name}"))?;
        let vendored = std::fs::read_to_string(format!("crates/exocortex-wire/proto/{name}"))?;
        if root != vendored {
            anyhow::bail!("proto/{name} and crates/exocortex-wire/proto/{name} diverge — copy the authoritative root file");
        }
    }
    // D5: the wire tarball ships the AGPL text; the crate-local copy
    // must stay identical to the root LICENSE.
    let root = std::fs::read_to_string("LICENSE")?;
    let vendored = std::fs::read_to_string("crates/exocortex-wire/LICENSE")?;
    if root != vendored {
        anyhow::bail!("crates/exocortex-wire/LICENSE diverged from the root LICENSE");
    }
    println!("proto-sync ok: vendored wire protos + LICENSE match");
    Ok(())
}

/// PRD R1/R2: prove `exocortex-wire` is consumable standalone from the
/// PUBLISHED artifact. Packages the crate, extracts the tarball into a
/// temp workspace, drops the standalone fixture in beside it (pointing
/// at the extracted crate), builds, and asserts the fixture's dependency
/// graph contains exactly one `exocortex-*` crate.
fn wire_standalone() -> anyhow::Result<()> {
    use std::process::Command;
    let out = Command::new("cargo")
        .args([
            "package",
            "-p",
            "exocortex-wire",
            "--allow-dirty",
            "--no-verify",
        ])
        .output()?;
    if !out.status.success() {
        anyhow::bail!("cargo package -p exocortex-wire failed");
    }
    // G3: the real version from cargo metadata — a hardcoded "0.1.0"
    // breaks this gate at the first version bump.
    let version = wire_version()?;
    let crate_file = format!("target/package/exocortex-wire-{version}.crate");

    let tmp = tempfile_dir()?;
    let extracted = tmp.join("exocortex-wire");
    std::fs::create_dir_all(&extracted)?;
    let bytes = std::fs::read(&crate_file)?;
    unpack_gzip_tar(&bytes, &extracted)?;
    let inner = extracted.join(format!("exocortex-wire-{version}"));

    // Fixture crate beside the extracted wire, pointing at it by path.
    let fixture_src = std::path::Path::new("crates/exocortex-wire/tests/standalone");
    let fixture_dst = tmp.join("fixture");
    copy_dir(fixture_src, &fixture_dst)?;
    // Rewrite the fixture's path dep to the EXTRACTED tarball directory
    // (the extracted wire, not the repo copy — that is the R1 point).
    let manifest = fixture_dst.join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest)?;
    std::fs::write(
        &manifest,
        text.replace(
            r#"{ path = "../exocortex-wire" }"#,
            &format!(r#"{{ path = "../exocortex-wire/exocortex-wire-{version}" }}"#),
        ),
    )?;
    std::fs::write(
        tmp.join("Cargo.toml"),
        "[workspace]\nmembers = [\"fixture\"]\nresolver = \"2\"\n",
    )?;

    let build = Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(&tmp)
        .env("CARGO_TARGET_DIR", tmp.join("target"))
        .output()?;
    if !build.status.success() {
        anyhow::bail!(
            "standalone fixture build failed:
{}",
            String::from_utf8_lossy(&build.stderr)
        );
    }

    // R2: exactly one exocortex-* crate in the resolved graph.
    let tree = Command::new("cargo")
        .args(["tree", "-p", "wire-standalone-fixture", "-e", "no-dev"])
        .current_dir(&tmp)
        .env("CARGO_TARGET_DIR", tmp.join("target"))
        .output()?;
    let text = String::from_utf8_lossy(&tree.stdout);
    let exo_count = text.lines().filter(|l| l.contains("exocortex-")).count();
    if exo_count != 1 {
        anyhow::bail!(
            "standalone fixture must resolve exactly one exocortex-* crate, found {exo_count}:
{text}"
        );
    }
    let _ = inner;
    println!("wire-standalone ok: packaged tarball builds a wire-only consumer");
    Ok(())
}

fn tempfile_dir() -> anyhow::Result<std::path::PathBuf> {
    let dir = std::env::temp_dir().join(format!("exo-wire-standalone-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &to)?;
        } else {
            std::fs::copy(entry.path(), &to)?;
        }
    }
    Ok(())
}

/// Extract the packaged tarball (tar + flate2 crates; gate deps).
fn unpack_gzip_tar(gz: &[u8], dst: &std::path::Path) -> anyhow::Result<()> {
    let tarball = flate2::read::GzDecoder::new(gz);
    let mut archive = tar::Archive::new(tarball);
    archive.unpack(dst)?;
    Ok(())
}
/// The workspace version of exocortex-wire via cargo metadata.
fn wire_version() -> anyhow::Result<String> {
    let out = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()?;
    if !out.status.success() {
        anyhow::bail!("cargo metadata failed");
    }
    let meta: serde_json::Value = serde_json::from_slice(&out.stdout)?;
    meta["packages"]
        .as_array()
        .and_then(|ps| {
            ps.iter()
                .find(|p| p["name"] == "exocortex-wire")
                .and_then(|p| p["version"].as_str().map(String::from))
        })
        .ok_or_else(|| anyhow::anyhow!("exocortex-wire not found in metadata"))
}

/// Round-3 G2: calibrated batch-signing hygiene gates. The adapter-SDK
/// PRD's raw grep commands were miscalibrated (they matched benign
/// scaffolding and unrelated SSE-envelope HMACs). These check the
/// actual invariants:
///  1. No local batch-signing/checksum implementation outside
///     `exocortex-wire/src/signing.rs` (single-implementation rule).
///  2. No producer submits a blank checksum: every `IngestBatch`
///     construction site with `checksum: String::new()` must be
///     followed by prepare_batch/canonical_checksum before submit.
fn signing_hygiene() -> anyhow::Result<()> {
    let violations = gates::signing_hygiene_violations(std::path::Path::new("."))?;
    anyhow::ensure!(
        violations.is_empty(),
        "signing-hygiene FAILED:\n{}",
        violations.join("\n")
    );
    println!("signing-hygiene ok: all HMAC lives in exocortex-wire::signing; no unsigned blank-checksum submitters");
    Ok(())
}

fn metrics_hygiene() -> anyhow::Result<()> {
    let mut offenders = Vec::new();
    for entry in walk_rss(std::path::Path::new("crates"))? {
        let src = std::fs::read_to_string(&entry)?;
        for issue in metrics_hygiene_issues(&src) {
            offenders.push(format!("{}: {issue}", entry.display()));
        }
    }
    anyhow::ensure!(
        offenders.is_empty(),
        "metrics-hygiene FAILED:\n{}",
        offenders.join("\n")
    );
    println!("metrics-hygiene ok: authenticated surface; literal bounded labels only");
    Ok(())
}

fn metrics_hygiene_issues(src: &str) -> Vec<String> {
    const MACROS: &[&str] = &[
        "metrics::counter!(",
        "metrics::gauge!(",
        "metrics::histogram!(",
    ];
    const FORBIDDEN: &[&str] = &[
        "graph",
        "org",
        "org_id",
        "user",
        "user_id",
        "producer",
        "producer_id",
        "client_version",
        "playbook_version",
    ];
    let mut issues = Vec::new();
    for marker in MACROS {
        let mut rest = src;
        while let Some(start) = rest.find(marker) {
            rest = &rest[start + marker.len()..];
            let mut depth = 1usize;
            let mut end = rest.len();
            for (index, ch) in rest.char_indices() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = index;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let invocation = &rest[..end];
            for segment in invocation.split(',').filter(|part| part.contains("=>")) {
                let Some((key, value)) = segment.split_once("=>") else {
                    continue;
                };
                let key = key.trim().trim_matches('"');
                if FORBIDDEN.contains(&key) {
                    issues.push(format!("forbidden identity label `{key}`"));
                }
                let value = value.trim();
                if !value.starts_with('"') && value != "entry.name" {
                    issues.push(format!("label `{key}` has a non-literal value"));
                }
            }
            if end == rest.len() {
                break;
            }
            rest = &rest[end + 1..];
        }
    }
    issues
}

fn walk_rss(dir: &std::path::Path) -> anyhow::Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().map(|n| n == "target").unwrap_or(false) {
                continue;
            }
            out.extend(walk_rss(&path)?);
        } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
            out.push(path);
        }
    }
    Ok(out)
}

/// D1/P2 (agent-instructions PRD): regenerate the playbook's generated
/// sections from the code they describe — the kind catalogue from the
/// effective ontology (W6's computed_only flag honored), the reject-code
/// table from the `RejectCode` enum — and enforce the instruction
/// block's ≤300-word bound plus its kind claims against the pack.
/// Default mode is the CI drift gate (fails on diff); `--write`
/// regenerates in place. Facts can no longer drift from the code.
fn gen_playbook(write: bool) -> Result<()> {
    let _ = std::hint::black_box(exocortex_pack_dev_v1::pack_def().name.clone());
    let onto = exocortex_kernel::pack::load_registered_packs()?;
    let path = std::path::Path::new("crates/exocortex-client/src/playbook/v1_0_0.md");
    let src = std::fs::read_to_string(path)?;

    // ---- gen:kinds — bucket-grouped, computed-only flagged ----
    let mut kinds_section = String::from(
        "<!-- gen:kinds — do not edit by hand; regenerated from exocortex-pack-dev-v1 -->\n",
    );
    let bucket_name = |b: exocortex_kernel::RelBucket| -> &'static str {
        use exocortex_kernel::RelBucket::*;
        match b {
            Solution => "Solution",
            Causal => "Causal",
            Context => "Context",
            Learning => "Learning",
            Similarity => "Similarity",
            Workflow => "Workflow",
            Quality => "Quality",
            Integration => "Integration",
            Extension(_) => "Extension",
        }
    };
    let mut by_bucket: std::collections::BTreeMap<&str, Vec<String>> = Default::default();
    let mut computed_only = 0usize;
    let mut total = 0usize;
    // Authored kinds only: R-T4 auto-registers read-only inverse
    // companions (`SolvedBy`, `ReplacedBy`, …) at pack-local ids >=
    // 0x4000 — they are materialized on write, never authorable, and a
    // generator that listed them would teach producers kinds the
    // validator refuses (§3.1 r3's warning, caught by running it).
    let mut ordered: Vec<&exocortex_kernel::RelMeta> = onto
        .kinds_by_id
        .values()
        .filter(|m| m.id.is_kernel() || m.id.local_part() < 0x4000)
        .collect();
    ordered.sort_by_key(|m| m.display_name.to_string());
    for m in ordered {
        let bucket = bucket_name(m.bucket);
        let name = m.display_name.to_string();
        let marker = if m.computed_only {
            computed_only += 1;
            " †"
        } else {
            ""
        };
        by_bucket
            .entry(bucket)
            .or_default()
            .push(format!("`{name}`{marker}"));
        total += 1;
    }
    for (bucket, kinds) in &by_bucket {
        kinds_section.push_str(&format!(
            "- **{bucket} ({}):** {}\n",
            kinds.len(),
            kinds.join(", ")
        ));
    }
    let assertable = total - computed_only;
    kinds_section.push_str(&format!(
        "\n† Computed-only (R-T14): the consolidation cycle asserts it, producers may\nnot. {total} kinds are declared; **{assertable} are yours to use.** Asserting one\nis rejected with `ComputedKindRejected`.\n<!-- /gen:kinds -->"
    ));

    // ---- gen:rejects — one row per RejectCode variant ----
    use exocortex_wire::ingest::v1::RejectCode;
    let guidance = |c: RejectCode| -> String {
        exocortex_wire::corrections::guidance(c)
            .correction
            .to_string()
    };
    let all = [
        (
            RejectCode::InvalidTypeTriple,
            "Kind doesn't fit the (from, to) types",
        ),
        (
            RejectCode::UnknownKind,
            "Kind name typo or not in this pack",
        ),
        (
            RejectCode::UnknownMemoryType,
            "Memory type not in this pack",
        ),
        (
            RejectCode::Unknown,
            "Title empty/>200 chars, content empty, or an atomic-batch reject",
        ),
        (
            RejectCode::VisibilityWidening,
            "Visibility above the source ceiling (rare for session wrapups)",
        ),
        (
            RejectCode::ComputedKindRejected,
            "You asserted a computed-only kind (R-T14)",
        ),
        (
            RejectCode::DuplicateBatch,
            "Transport replayed the same batch id",
        ),
        (RejectCode::RateLimited, "Backend is shedding load"),
        (
            RejectCode::MissingExternalKey,
            "External-snapshot coordinates missing",
        ),
        (
            RejectCode::InvalidExternalKey,
            "External-snapshot coordinates malformed",
        ),
        (RejectCode::Unauthorized, "Credentials rejected (HMAC)"),
        (RejectCode::BadChecksum, "Batch checksum mismatch"),
        (
            RejectCode::IncompatibleOntology,
            "Ontology fingerprint mismatch",
        ),
        (
            RejectCode::UnknownSource,
            "Producer not registered / ceiling mismatch / wrong org",
        ),
    ];
    let mut rejects_section = String::from(
        "<!-- gen:rejects — do not edit by hand; regenerated from the RejectCode enum -->\n| Code | Meaning | Fix |\n|---|---|---|\n",
    );
    for (code, meaning) in all {
        rejects_section.push_str(&format!(
            "| `{code:?}` | {meaning} | {} |\n",
            guidance(code).replace('|', "/")
        ));
    }
    rejects_section.push_str("<!-- /gen:rejects -->");

    // ---- splice between the markers ----
    let splice = |src: &str, start_marker: &str, end_marker: &str, new_body: &str| -> String {
        let start = src.find(start_marker).expect("start marker present");
        let end = src.find(end_marker).expect("end marker present") + end_marker.len();
        format!("{}{}{}", &src[..start], new_body, &src[end..])
    };
    let regenerated = splice(
        &splice(
            &src,
            "<!-- gen:kinds",
            "<!-- /gen:kinds -->",
            &kinds_section,
        ),
        "<!-- gen:rejects",
        "<!-- /gen:rejects -->",
        &rejects_section,
    );

    // ---- block bound + kind claims (§11) ----
    let block = std::fs::read_to_string("crates/exocortex-client/src/playbook/block_v1_0_0.md")?;
    let words = block.split_whitespace().count();
    anyhow::ensure!(
        words <= 300,
        "instruction block is {words} words; the bound is 300"
    );
    anyhow::ensure!(
        onto.kind_id("RelatedTo").is_some(),
        "block teaches `RelatedTo` but the pack does not declare it"
    );
    anyhow::ensure!(
        onto.kinds_by_id
            .get(&onto.kind_id("SimilarTo").unwrap())
            .is_some_and(|m| m.computed_only),
        "block prohibits `SimilarTo`; the pack must mark it computed-only"
    );

    if write {
        std::fs::write(path, &regenerated)?;
        println!("gen-playbook: regenerated ({total} kinds, {assertable} assertable; {words}-word block)");
        Ok(())
    } else {
        anyhow::ensure!(
            regenerated == src,
            "gen-playbook: playbook drifted from the ontology/RejectCode — run `cargo xtask gen-playbook --write`"
        );
        println!("gen-playbook ok ({total} kinds, {assertable} assertable; {words}-word block)");
        Ok(())
    }
}

#[cfg(test)]
mod metrics_hygiene_tests {
    use super::metrics_hygiene_issues;

    #[test]
    fn rejects_identity_and_dynamic_metric_labels() {
        let bad = r#"metrics::counter!("requests", "producer_id" => producer.clone(), "outcome" => "ok");"#;
        let issues = metrics_hygiene_issues(bad);
        assert!(issues.iter().any(|issue| issue.contains("identity")));
        assert!(issues.iter().any(|issue| issue.contains("non-literal")));
        assert!(
            metrics_hygiene_issues(r#"metrics::counter!("requests", "outcome" => "ok");"#)
                .is_empty()
        );
    }
}

#[cfg(test)]
mod release_hardening_tests {
    use super::{
        validate_chaos_compose, validate_chaos_script, validate_fastembed_dependency_contract,
        validate_fastembed_release, validate_release_hardening,
    };

    const SHA: &str = "11d5960a326750d5838078e36cf38b85af677262";
    const PROTOC_INSTALLER: &str = r#"readonly PROTOC_VERSION=28.3
readonly RELEASE_ROOT="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}"
curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error "$url"
actual_sha256=x
if [[ "$actual_sha256" != "$expected_sha256" ]]; then exit 1; fi
readonly expected_sha256=0000000000000000000000000000000000000000000000000000000000000000
readonly expected_sha256=1111111111111111111111111111111111111111111111111111111111111111
readonly expected_sha256=2222222222222222222222222222222222222222222222222222222222222222
readonly expected_sha256=3333333333333333333333333333333333333333333333333333333333333333
"#;

    #[test]
    fn rejects_mutable_actions_build_inputs_root_runtime_and_public_test_ports() {
        let good_release = format!(
            "permissions:\n  contents: read\njobs:\n  build:\n    steps:\n      - uses: actions/checkout@{SHA}\n      - run: bash scripts/install-protoc.sh /tmp/protoc\n      - run: bash scripts/install-protoc.sh /tmp/protoc-2\n      - run: cargo build -p exocortex-server --features exocortex-server/fastembed\n  release:\n    permissions:\n      contents: write\n    steps:\n      - run: bash scripts/publish-release-assets.sh \"$TAG\" \"$GITHUB_REPOSITORY\" dist\n"
        );
        let good_ci = format!(
            "permissions:\n  contents: read\njobs:\n  gates:\n    steps:\n      - uses: actions/checkout@{SHA}\n      - run: bash scripts/install-protoc.sh /tmp/protoc\n"
        );
        let good_dockerfile = format!(
            concat!(
                "FROM scratch AS protoc-amd64\n",
                "ADD --checksum=sha256:{digest} https://example.invalid/protoc-28.3-linux-x86_64.zip /protoc.zip\n",
                "FROM scratch AS protoc-arm64\n",
                "ADD --checksum=sha256:{digest} https://example.invalid/protoc-28.3-linux-aarch_64.zip /protoc.zip\n",
                "FROM scratch AS model\n",
                "ADD --checksum=sha256:{digest} https://example.invalid/model-1 /model/1\n",
                "ADD --checksum=sha256:{digest} https://example.invalid/model-2 /model/2\n",
                "ADD --checksum=sha256:{digest} https://example.invalid/model-3 /model/3\n",
                "ADD --checksum=sha256:{digest} https://example.invalid/model-4 /model/4\n",
                "ADD --checksum=sha256:{digest} https://example.invalid/model-5 /model/5\n",
                "FROM gcr.io/distroless/cc-debian12:nonroot@sha256:{digest}\n",
                "RUN command -v pkg-config && pkg-config --exists openssl\n",
                "RUN cargo build --release -p exocortex-server --bin exocortex-node --features fastembed\n",
                "RUN HF_HOME=/opt/exocortex/models /repo/target/release/exocortex-node --verify-embedder\n",
                "COPY --from=build --chown=65532:65532 /opt/exocortex/models /opt/exocortex/models\n",
                "ENV HF_HOME=/opt/exocortex/models\n",
                "USER 65532:65532\n"
            ),
            digest = "a".repeat(64)
        );
        let good_compose = &format!(
            "image: redis@sha256:{}\nports:\n  - \"127.0.0.1:8080:8080\"\n",
            "b".repeat(64)
        );
        let workflows = [
            (".github/workflows/release.yml", good_release.as_str()),
            (".github/workflows/ci.yml", good_ci.as_str()),
        ];
        assert!(validate_release_hardening(
            &workflows,
            &good_dockerfile,
            PROTOC_INSTALLER,
            &[good_compose]
        )
        .is_ok());

        let mutable_ci = good_ci.replace(SHA, "v4");
        let workflows_with_mutable_ci = [
            (".github/workflows/release.yml", good_release.as_str()),
            (".github/workflows/ci.yml", mutable_ci.as_str()),
        ];
        assert!(validate_release_hardening(
            &workflows_with_mutable_ci,
            &good_dockerfile,
            PROTOC_INSTALLER,
            &[good_compose]
        )
        .is_err());
        for mutable_image in ["redis:latest", "redis:7-alpine"] {
            let compose = format!("image: {mutable_image}\nports:\n  - \"127.0.0.1:8080:8080\"\n");
            assert!(validate_release_hardening(
                &workflows,
                &good_dockerfile,
                PROTOC_INSTALLER,
                &[&compose]
            )
            .is_err());
        }
        let missing_release_feature =
            good_release.replace(" --features exocortex-server/fastembed", "");
        assert!(validate_release_hardening(
            &[
                (
                    ".github/workflows/release.yml",
                    missing_release_feature.as_str()
                ),
                (".github/workflows/ci.yml", good_ci.as_str()),
            ],
            &good_dockerfile,
            PROTOC_INSTALLER,
            &[good_compose]
        )
        .is_err());
        let missing_docker_feature = good_dockerfile.replace(" --features fastembed", "");
        assert!(validate_release_hardening(
            &workflows,
            &missing_docker_feature,
            PROTOC_INSTALLER,
            &[good_compose]
        )
        .is_err());
        let missing_native_tls_toolchain = good_dockerfile.replace(
            "RUN command -v pkg-config && pkg-config --exists openssl\n",
            "",
        );
        assert!(validate_release_hardening(
            &workflows,
            &missing_native_tls_toolchain,
            PROTOC_INSTALLER,
            &[good_compose]
        )
        .is_err());
        let no_permissions = good_ci.replacen("permissions:\n  contents: read\n", "", 1);
        let workflows_without_ci_permissions = [
            (".github/workflows/release.yml", good_release.as_str()),
            (".github/workflows/ci.yml", no_permissions.as_str()),
        ];
        assert!(validate_release_hardening(
            &workflows_without_ci_permissions,
            &good_dockerfile,
            PROTOC_INSTALLER,
            &[good_compose]
        )
        .is_err());
        assert!(validate_release_hardening(
            &workflows,
            "FROM debian:bookworm-slim\nUSER 65532:65532\n",
            PROTOC_INSTALLER,
            &[good_compose]
        )
        .is_err());
        for package_command in [
            "apt-get install -y ca-certificates",
            "brew install protobuf",
            "apk add ca-certificates",
        ] {
            let package_managed_dockerfile = format!("{good_dockerfile}RUN {package_command}\n");
            assert!(validate_release_hardening(
                &workflows,
                &package_managed_dockerfile,
                PROTOC_INSTALLER,
                &[good_compose]
            )
            .is_err());
        }
        assert!(validate_release_hardening(
            &workflows,
            &good_dockerfile,
            PROTOC_INSTALLER,
            &["ports:\n  - \"8080:8080\"\n"]
        )
        .is_err());
        for unsafe_release in [
            good_release.replace(
                "scripts/publish-release-assets.sh",
                "scripts/unsafe-release.sh",
            ),
            good_release.replace(" dist\n", " dist --clobber\n"),
            good_release.replace(" dist\n", " dist || true\n"),
        ] {
            let unsafe_workflows = [
                (".github/workflows/release.yml", unsafe_release.as_str()),
                (".github/workflows/ci.yml", good_ci.as_str()),
            ];
            assert!(validate_release_hardening(
                &unsafe_workflows,
                &good_dockerfile,
                PROTOC_INSTALLER,
                &[good_compose]
            )
            .is_err());
        }

        for package_command in [
            "sudo apt-get install -y protobuf-compiler",
            "brew install protobuf",
            "apk add protobuf",
        ] {
            let mutable_protoc_workflow = good_ci.replace(
                "bash scripts/install-protoc.sh /tmp/protoc",
                package_command,
            );
            let mutable_workflows = [
                (".github/workflows/release.yml", good_release.as_str()),
                (".github/workflows/ci.yml", mutable_protoc_workflow.as_str()),
            ];
            assert!(validate_release_hardening(
                &mutable_workflows,
                &good_dockerfile,
                PROTOC_INSTALLER,
                &[good_compose]
            )
            .is_err());
        }

        let unchecked_installer = PROTOC_INSTALLER.replace(
            "if [[ \"$actual_sha256\" != \"$expected_sha256\" ]]; then exit 1; fi",
            "true",
        );
        assert!(validate_release_hardening(
            &workflows,
            &good_dockerfile,
            &unchecked_installer,
            &[good_compose]
        )
        .is_err());
    }

    #[test]
    fn fastembed_release_contract_rejects_each_missing_surface() {
        let release = include_str!("../../.github/workflows/release.yml");
        let workflows = [(".github/workflows/release.yml", release)];
        let docker = include_str!("../../Dockerfile");
        let verify = include_str!("../../scripts/verify-release.sh");
        let fetcher = include_str!("../../scripts/fetch-embedding-model.sh");
        let installer = include_str!("../../scripts/release-install.sh");
        let embedding = include_str!("../../crates/exocortex-ingest/src/embedding.rs");
        let server_main = include_str!("../../crates/exocortex-server/src/main.rs");
        assert!(validate_fastembed_release(
            &workflows,
            docker,
            verify,
            fetcher,
            installer,
            embedding,
            server_main,
        )
        .is_ok());
        assert!(validate_fastembed_release(
            &[(".github/workflows/release.yml", "cargo build")],
            docker,
            verify,
            fetcher,
            installer,
            embedding,
            server_main,
        )
        .is_err());
        assert!(validate_fastembed_release(
            &workflows,
            "cargo build",
            verify,
            fetcher,
            installer,
            embedding,
            server_main,
        )
        .is_err());
        assert!(validate_fastembed_release(
            &workflows,
            docker,
            "cargo check",
            fetcher,
            installer,
            embedding,
            server_main,
        )
        .is_err());
        assert!(validate_fastembed_release(
            &workflows,
            &docker.replace(" --verify-embedder", ""),
            verify,
            fetcher,
            installer,
            embedding,
            server_main,
        )
        .is_err());
        assert!(validate_fastembed_release(
            &workflows,
            docker,
            verify,
            &fetcher.replace("ea104dacec62c0de699686887e3f920caeb4f3e3", "main"),
            installer,
            embedding,
            server_main,
        )
        .is_err());
        assert!(validate_fastembed_release(
            &workflows,
            docker,
            verify,
            fetcher,
            installer,
            &embedding.replace("try_new_from_user_defined", "try_new"),
            server_main,
        )
        .is_err());
        assert!(validate_fastembed_release(
            &workflows,
            docker,
            verify,
            fetcher,
            installer,
            embedding,
            &server_main.replace("max_error <= 1.0e-4", "true"),
        )
        .is_err());
        assert!(validate_fastembed_release(
            &workflows,
            docker,
            verify,
            fetcher,
            installer,
            &embedding.replace(
                "BGE_SMALL_MAX_LENGTH: usize = 512",
                "BGE_SMALL_MAX_LENGTH: usize = 384"
            ),
            server_main,
        )
        .is_err());
    }

    #[test]
    fn fastembed_dependency_contract_rejects_model_transport_and_msrv_drift() {
        let workspace = include_str!("../../Cargo.toml");
        let ingest = include_str!("../../crates/exocortex-ingest/Cargo.toml");
        assert!(validate_fastembed_dependency_contract(workspace, ingest).is_ok());
        assert!(validate_fastembed_dependency_contract(
            &workspace.replace(
                "features = [\"ort-download-binaries\"]",
                "features = [\"ort-download-binaries\", \"hf-hub-rustls-tls\"]"
            ),
            ingest
        )
        .is_err());
        assert!(validate_fastembed_dependency_contract(
            &workspace.replace("image = \"=0.25.5\"", "image = \"0.25.5\""),
            ingest
        )
        .is_err());
        assert!(validate_fastembed_dependency_contract(
            workspace,
            &ingest.replace("fastembed = [\"dep:fastembed\", \"dep:image\"]", "fastembed = [\"dep:fastembed\", \"dep:image\", \"dep:sha2\"]\nsha2 = { workspace = true, optional = true }")
        )
        .is_err());
        assert!(validate_fastembed_dependency_contract(
            &workspace.replace(", \"tls-native-roots\"", ""),
            ingest
        )
        .is_err());
    }

    #[test]
    fn chaos_compose_requires_owner_only_staged_policies() {
        let compose =
            include_str!("../../crates/exocortex-cluster/tests/docker-compose-cluster.yml");
        assert!(validate_chaos_compose(compose).is_ok());

        let direct_mount = compose.replace(
            "policy-data:/run/exocortex-policies:ro",
            "./principal-policy.dev.json:/etc/exocortex/principal-policy.json:ro",
        );
        assert!(validate_chaos_compose(&direct_mount).is_err());

        let unsafe_mode = compose.replace("install -m 0600", "install -m 0644");
        assert!(validate_chaos_compose(&unsafe_mode).is_err());
    }

    #[test]
    fn chaos_script_authenticates_protected_health_polling() {
        let script = include_str!("../../scripts/chaos-leader-kill.sh");
        assert!(validate_chaos_script(script).is_ok());
        assert!(validate_chaos_script(
            &script.replace("-H \"Authorization: Bearer $AUTH_TOKEN\"", "")
        )
        .is_err());
        assert!(
            validate_chaos_script(&script.replacen("cluster_health \"$port\"", "curl", 1)).is_err()
        );
    }

    #[test]
    fn chaos_script_requires_live_inflight_fence_probe_before_success() {
        let script = include_str!("../../scripts/chaos-leader-kill.sh");
        assert!(validate_chaos_script(script).is_ok());
        assert!(validate_chaos_script(&script.replace(
            "inflight_stale_dreams_write_is_fenced_after_takeover_live",
            "stale_lease_write_is_fenced_live"
        ))
        .is_err());
        let reordered = script.replace(
            "echo \"PASS: authenticated takeover and no-zombie Dreams write fencing\"",
            "true",
        );
        assert!(validate_chaos_script(&reordered).is_err());
    }
}
