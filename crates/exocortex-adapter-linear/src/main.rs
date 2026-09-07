//! D19: the Linear adapter binary. Fetches issue windows from Linear's
//! GraphQL API (direct, env-only key — never MCP) and submits them
//! through the signed Ingestion Protocol. One-shot per invocation: a
//! scheduler (cron, systemd timer) drives cadence; the durable cursor
//! makes every run resume exactly where the last settled.
//!
//! Secrets come from the environment (never argv): `LINEAR_API_KEY`
//! (the source API), `EXOCORTEX_AUTH_TOKEN` (backend bearer) and
//! `EXOCORTEX_HMAC_KEY` (64 hex chars).

use clap::Parser;
use exocortex_api_client::{ApiClient, ApiError};

/// Transcribe a Linear workspace's issues into the exocortex graph.
#[derive(Debug, Parser)]
#[command(name = "exocortex-adapter-linear", version)]
struct Args {
    /// Backend IngestService base URL.
    #[arg(long)]
    backend: String,
    /// Owning org.
    #[arg(long)]
    org: String,
    /// Linear workspace identity (slug) — scopes external keys.
    #[arg(long)]
    workspace: String,
    /// Producer identity for registration.
    #[arg(long, default_value = "linear-adapter")]
    producer: String,
    /// Linear GraphQL endpoint (default: the public API).
    #[arg(long, default_value = "https://api.linear.app/graphql")]
    endpoint: String,
    /// Durable cursor file (stores the newest settled updatedAt).
    #[arg(long, default_value = "linear-adapter.cursor")]
    cursor: std::path::PathBuf,
    /// Maximum memory rows per submit window (D21-a bound). At least
    /// 2: an issue plus the project it introduces is the smallest
    /// legal window.
    #[arg(long, default_value = "100", value_parser = clap::value_parser!(u64).range(2..))]
    max_window: u64,
    /// Seed the cursor on a first run (RFC3339); later runs resume from
    /// the file.
    #[arg(long)]
    since: Option<String>,
    /// Visibility stamped on every row (private|project|team|org).
    /// Default org mirrors a single-tenant deployment whose boundary
    /// is the Linear workspace; tighten per source when the org is
    /// wider than the workspace's ACLs.
    #[arg(long, default_value = "org")]
    visibility: String,
    /// Page size for the source API.
    #[arg(long, default_value = "50")]
    page_size: u32,
}

/// Fetch one page, retrying rate limits on the server's own schedule
/// (bounded), surfacing everything else.
async fn fetch_page(
    client: &ApiClient,
    token: &str,
    after: Option<&str>,
    gte: Option<&str>,
    page_size: u32,
) -> Result<serde_json::Value, anyhow::Error> {
    let mut variables = serde_json::json!({ "first": page_size });
    if let Some(after) = after {
        variables["after"] = serde_json::Value::String(after.into());
    }
    match gte {
        Some(gte) => variables["gte"] = serde_json::Value::String(gte.into()),
        None => variables["gte"] = serde_json::Value::Null,
    }
    let mut attempts = 0u32;
    loop {
        match client
            .graphql(token, exocortex_adapter_linear::ISSUES_QUERY, &variables)
            .await
        {
            Ok(data) => return Ok(data),
            Err(ApiError::RateLimited { retry_after, .. }) => {
                attempts += 1;
                if attempts > 5 {
                    anyhow::bail!("source API rate limit persisted after {attempts} waits");
                }
                // The server's number when it stated one (capped); a
                // bounded fixed ladder otherwise — never a busy spin.
                let delay = retry_after
                    .map(|d| d.min(std::time::Duration::from_secs(120)))
                    .unwrap_or_else(|| std::time::Duration::from_secs(2u64.pow(attempts)));
                tracing::warn!(?delay, attempt = attempts, "rate limited; backing off");
                tokio::time::sleep(delay).await;
            }
            Err(other) => return Err(other.into()),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    let visibility = exocortex_adapter_linear::parse_visibility(&args.visibility)
        .ok_or_else(|| anyhow::anyhow!("--visibility must be private|project|team|org"))?;

    let api_key = std::env::var("LINEAR_API_KEY")
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow::anyhow!("LINEAR_API_KEY is required"))?;
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

    // A missing cursor is the normal first run; any OTHER read error
    // is fatal — silently degrading to a full re-fetch contradicts
    // the SDK's own corrupt-cursor rule.
    let resume = match std::fs::read_to_string(&args.cursor) {
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
    }
    .or_else(|| args.since.clone());

    let mut config = exocortex_adapter_sdk::AdapterConfig::new(
        &args.org,
        &format!("linear://{}", args.workspace),
        &args.producer,
        &args.backend,
    );
    config.source_flavor = "linear".into();
    config.producer_kind = exocortex_wire::ingest::v1::ProducerKind::SaasAdapter;
    config.auth_token = auth_token;
    config.hmac_key = hmac_key;
    config.cursor_path = args.cursor.with_extension("sdk-cursor");
    config.projection = Some(exocortex_adapter_linear::projection(args.max_window));

    let mut session = exocortex_adapter_sdk::AdapterSession::connect(config).await?;
    let client = ApiClient::new(&args.endpoint)?.with_user_agent(concat!(
        "exocortex-adapter-linear/",
        env!("CARGO_PKG_VERSION")
    ));

    // Drain the workspace in windows. The fetch bound is HALF the
    // declared per-run row bound: every issue may also introduce a
    // project row, so worst-case rows are twice the issues fetched.
    // Stopping under the bound keeps the run clean; if it were ever
    // exceeded anyway, the SDK's ProjectionBoundExceeded aborts with
    // the cursor on the last settled window (progress saved, re-run
    // resumes): a safe failure, not a silent one.
    let run_rows_bound = args.max_window.saturating_mul(100);
    let fetch_bound = run_rows_bound / 2;
    let mut after: Option<String> = None;
    let mut fetched: Vec<exocortex_adapter_linear::LinearIssue> = Vec::new();
    loop {
        let data = fetch_page(
            &client,
            &api_key,
            after.as_deref(),
            resume.as_deref(),
            args.page_size,
        )
        .await?;
        if data.get("issues").is_none() {
            anyhow::bail!(
                "unexpected response shape: no `issues` connection (a renamed field or a                  200-error body would otherwise ingest as silence)"
            );
        }
        let (issues, skipped, has_next, end_cursor) =
            exocortex_adapter_linear::parse_issues_page(&data);
        if skipped > 0 {
            tracing::warn!(skipped, "malformed issue nodes skipped");
        }
        fetched.extend(issues);
        if fetched.len() as u64 >= fetch_bound {
            tracing::warn!(
                fetched = fetched.len(),
                fetch_bound,
                "fetch bound reached (half the per-run projection bound); re-run to continue draining"
            );
            break;
        }
        if !has_next || end_cursor.is_empty() {
            break;
        }
        after = Some(end_cursor);
    }
    let total = fetched.len();
    let mut rejected_rows: usize = 0;
    for (index, chunk) in exocortex_adapter_linear::chunk_windows(fetched, args.max_window)
        .into_iter()
        .enumerate()
    {
        rejected_rows += submit_chunk(
            &mut session,
            &args.workspace,
            &chunk,
            index as u64,
            visibility,
        )
        .await?;
        // The operator-facing cursor advances with every settled
        // window (the SDK keeps its own file; this one is what the
        // next run resumes from).
        if let Some(cursor) = exocortex_adapter_linear::cursor_for(&chunk) {
            save_cursor_atomic(&args.cursor, &cursor)?;
        }
    }
    println!(
        "ingested {total} issues (resume at or after {:?})",
        resume.as_deref().unwrap_or("the beginning")
    );
    if rejected_rows > 0 {
        // Permanently rejected rows sit behind the advanced cursor and
        // never retry; a scheduler must see failure, not silent loss.
        eprintln!("{rejected_rows} rows permanently rejected (see the log); they will not retry");
        std::process::exit(2);
    }
    Ok(())
}

fn save_cursor_atomic(path: &std::path::Path, value: &str) -> std::io::Result<()> {
    // tmp+rename like the SDK's own save_cursor: a torn plain write
    // truncates the resume point and the next run replays from a
    // wrong-but-parseable instant.
    let tmp = path.with_extension("cursor.tmp");
    std::fs::write(&tmp, value)?;
    std::fs::rename(&tmp, path)
}
async fn submit_chunk(
    session: &mut exocortex_adapter_sdk::AdapterSession,
    workspace: &str,
    chunk: &[exocortex_adapter_linear::LinearIssue],
    index: u64,
    visibility: i32,
) -> anyhow::Result<usize> {
    let unit = exocortex_adapter_linear::map_issues(
        workspace,
        chunk,
        &format!("window-{index}"),
        visibility,
    );
    let cursor = exocortex_adapter_linear::cursor_for(chunk)
        .ok_or_else(|| anyhow::anyhow!("empty chunk has no cursor"))?;
    let outcome = session.submit_window(vec![unit], &cursor).await?;
    tracing::info!(
        accepted = outcome.accepted,
        duplicates = outcome.duplicates,
        rejected = outcome.permanent_rejections.len(),
        cursor = %cursor,
        "window settled"
    );
    for rejection in &outcome.permanent_rejections {
        tracing::error!(key = %rejection.draft_key, code = %rejection.code, "{}", rejection.detail);
    }
    Ok(outcome.permanent_rejections.len())
}
