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
    #[arg(long, default_value = "256", value_parser = clap::value_parser!(u64).range(2..))]
    max_window: u64,
    /// Revision range instead of cursor..HEAD (one-shot mode).
    #[arg(long)]
    range: Option<String>,
    /// Maximum commits read per run: git log output is buffered whole,
    /// so an unbounded read of a huge history is an unbounded process
    /// (the oldest commits land first; the rest wait for the next run).
    #[arg(long, default_value = "20000")]
    max_commits: usize,
}

fn save_cursor_atomic(path: &std::path::Path, value: &str) -> std::io::Result<()> {
    // tmp+rename like the SDK's own save_cursor: a torn plain write
    // truncates the resume point and the next run replays from a
    // wrong-but-parseable instant.
    let tmp = path.with_extension("cursor.tmp");
    std::fs::write(&tmp, value)?;
    std::fs::rename(&tmp, path)
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
    let cursor_sha = match std::fs::read_to_string(&args.cursor) {
        Ok(content) => {
            let content = content.trim().to_string();
            if content.is_empty() {
                None
            } else {
                Some(content)
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => anyhow::bail!("reading cursor file {}: {error}", args.cursor.display()),
    };
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
    let (mut commits, skipped) = exocortex_adapter_git::parse_git_log(&log);
    if skipped > 0 {
        tracing::warn!(skipped, "malformed git log records skipped");
    }
    if commits.len() > args.max_commits {
        let remaining = commits.len() - args.max_commits;
        commits.truncate(args.max_commits);
        eprintln!(
            "history truncated at {} commits ({} newer remain; re-run to continue)",
            args.max_commits, remaining
        );
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

    let mut rejected_rows: usize = 0;
    let mut session = exocortex_adapter_sdk::AdapterSession::connect(config).await?;
    // Units are bounded by MEMORY rows (commits + distinct file rows)
    // AND by the protocol's per-batch edge ceiling: shared paths weld
    // commits into one component, and an over-ceiling component would
    // be permanently rejected with the cursor already past it (R12).
    // Oldest first, so a parent commit lands before its child
    // references it. A commit wider than the edge ceiling is sliced
    // across units; the cursor only advances past a commit once its
    // last slice has settled.
    let bounded = exocortex_adapter_git::map_history_bounded(
        &repo_id,
        &commits,
        "window",
        args.max_window as usize,
        exocortex_wire::limits::MAX_EDGES_PER_BATCH,
    );
    let mut frontier = cursor_sha.clone().unwrap_or_default();
    for bounded_unit in bounded {
        if !bounded_unit.completes_through.is_empty() {
            frontier = bounded_unit.completes_through;
        }
        let outcome = session
            .submit_window(vec![bounded_unit.unit], &frontier)
            .await?;
        rejected_rows += outcome.permanent_rejections.len();
        // The operator-facing cursor advances with every settled
        // window in SEQUENTIAL mode only: a one-shot --range may sit
        // mid-history, and writing its HEAD would strand the gap
        // before it (round-11 R11-6).
        if args.range.is_none() {
            save_cursor_atomic(&args.cursor, &frontier)?;
        }
        tracing::info!(
            accepted = outcome.accepted,
            duplicates = outcome.duplicates,
            rejected = outcome.permanent_rejections.len(),
            cursor = %frontier,
            "window settled"
        );
        if !outcome.permanent_rejections.is_empty() {
            for rejection in &outcome.permanent_rejections {
                tracing::error!(key = %rejection.draft_key, code = %rejection.code, "{}", rejection.detail);
            }
        }
    }
    println!("ingested {} commits (range {range})", commits.len());
    if rejected_rows > 0 {
        // Permanently rejected rows sit behind the advanced cursor and
        // never retry; a scheduler must see failure, not silent loss
        // (round-11, matching the sibling adapters).
        eprintln!("{rejected_rows} rows permanently rejected (see the log); they will not retry");
        std::process::exit(2);
    }
    Ok(())
}
