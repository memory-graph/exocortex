//! D18: the git-history adapter binary. Reads `git log` from a local
//! checkout, maps it through [`exocortex_adapter_git::map_history`], and
//! submits through the signed Ingestion Protocol. Secrets come from the
//! environment (never argv): `EXOCORTEX_AUTH_TOKEN` (bearer) and
//! `EXOCORTEX_HMAC_KEY` (64 hex chars).

use clap::Parser;

/// Transcribe a git history into the exocortex graph.
#[derive(Debug, Parser)]
#[command(name = "exocortex-adapter-git", version)]
struct Args {
    /// Path to the git checkout to read.
    #[arg(long)]
    repo: std::path::PathBuf,
    /// Backend IngestService base URL.
    #[arg(long)]
    backend: String,
    /// Owning org.
    #[arg(long)]
    org: String,
    /// Producer identity for registration.
    #[arg(long, default_value = "git-adapter")]
    producer: String,
    /// Stable repo identity for external keys (remote URL or path —
    /// whatever the operator pins; it scopes commit/file identity).
    #[arg(long)]
    repo_id: Option<String>,
    /// Durable cursor file (stores the newest ingested sha).
    #[arg(long, default_value = "git-adapter.cursor")]
    cursor: std::path::PathBuf,
    /// Maximum rows per submit window (D21-a bound).
    #[arg(long, default_value = "256")]
    max_window: u64,
    /// Revision range instead of cursor..HEAD (one-shot mode).
    #[arg(long)]
    range: Option<String>,
}

fn git(repo: &std::path::Path, args: &[&str]) -> anyhow::Result<String> {
    let out = std::process::Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .map_err(|e| anyhow::anyhow!("git: {e}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    let auth_token = std::env::var("EXOCORTEX_AUTH_TOKEN")
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow::anyhow!("EXOCORTEX_AUTH_TOKEN is required"))?;
    let hmac_key = exocortex_wire::signing::decode_hex32(
        &std::env::var("EXOCORTEX_HMAC_KEY")
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow::anyhow!("EXOCORTEX_HMAC_KEY is required (64 hex chars)"))?,
    )
    .map_err(anyhow::Error::msg)?;

    // The revision range: <cursor>..HEAD (or --all on a fresh cursor, or
    // an explicit one-shot range).
    let cursor_sha = std::fs::read_to_string(&args.cursor)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let range = match (&args.range, &cursor_sha) {
        (Some(explicit), _) => explicit.clone(),
        (None, Some(sha)) => format!("{sha}..HEAD"),
        (None, None) => "--all".into(),
    };
    let log = git(
        &args.repo,
        &[
            "log",
            "--reverse",
            &format!("--format={}", exocortex_adapter_git::GIT_LOG_FORMAT),
            "--name-only",
            &range,
        ],
    )?;
    let (commits, skipped) = exocortex_adapter_git::parse_git_log(&log);
    if skipped > 0 {
        tracing::warn!(skipped, "malformed git log records skipped");
    }
    tracing::info!(commits = commits.len(), "parsed history");
    if commits.is_empty() {
        println!("nothing to ingest (range {range})");
        return Ok(());
    }

    let repo_id = args.repo_id.clone().unwrap_or_else(|| {
        args.repo
            .to_string_lossy()
            .trim_end_matches('/')
            .to_string()
    });

    let mut config = exocortex_adapter_sdk::AdapterConfig::new(
        &args.org,
        &format!("git://{repo_id}"),
        &args.producer,
        &args.backend,
    );
    config.source_flavor = "custom".into();
    config.producer_kind = exocortex_wire::ingest::v1::ProducerKind::Custom;
    config.auth_token = auth_token;
    config.hmac_key = hmac_key;
    config.cursor_path = args.cursor.with_extension("sdk-cursor");
    config.projection = Some(exocortex_adapter_git::projection(args.max_window));

    let mut session = exocortex_adapter_sdk::AdapterSession::connect(config).await?;
    // One window per bounded slice, oldest first (so a parent commit is
    // ingested before its child references it).
    for (index, chunk) in commits.chunks(args.max_window as usize).enumerate() {
        if chunk.len() as u64 > args.max_window {
            unreachable!("chunks() honors the bound");
        }
        let unit = exocortex_adapter_git::map_history(&repo_id, chunk, &format!("window-{index}"));
        let newest = chunk
            .last()
            .map(|commit| commit.sha.clone())
            .unwrap_or_default();
        let outcome = session.submit_window(vec![unit], &newest).await?;
        tracing::info!(
            accepted = outcome.accepted,
            duplicates = outcome.duplicates,
            rejected = outcome.permanent_rejections.len(),
            cursor = %newest,
            "window settled"
        );
        if !outcome.permanent_rejections.is_empty() {
            for rejection in &outcome.permanent_rejections {
                tracing::error!(key = %rejection.draft_key, code = %rejection.code, "{}", rejection.detail);
            }
        }
    }
    println!("ingested {} commits (range {range})", commits.len());
    Ok(())
}
