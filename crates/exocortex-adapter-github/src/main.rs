//! D19: the GitHub adapter binary. Fetches issue and PR windows from
//! GitHub's GraphQL v4 API (direct, env-only PAT — never MCP) and
//! submits them through the signed Ingestion Protocol. One-shot per
//! invocation; the durable cursor resumes at the last settled window.
//!
//! Secrets come from the environment (never argv): `GITHUB_TOKEN`
//! (the source API), `EXOCORTEX_AUTH_TOKEN` (backend bearer) and
//! `EXOCORTEX_HMAC_KEY` (64 hex chars).

use clap::Parser;
use exocortex_adapter_github::{GhIssue, GhPull};
use exocortex_api_client::{ApiClient, ApiError};

/// Transcribe a GitHub repository's issues and PRs into the graph.
#[derive(Debug, Parser)]
#[command(name = "exocortex-adapter-github", version)]
struct Args {
    /// Backend IngestService base URL.
    #[arg(long)]
    backend: String,
    /// Owning org.
    #[arg(long)]
    org: String,
    /// Repository owner.
    #[arg(long)]
    owner: String,
    /// Repository name.
    #[arg(long)]
    repo: String,
    /// Producer identity for registration.
    #[arg(long, default_value = "github-adapter")]
    producer: String,
    /// GitHub GraphQL endpoint (default: the public API).
    #[arg(long, default_value = "https://api.github.com/graphql")]
    endpoint: String,
    /// Durable cursor file (stores the newest settled updatedAt).
    #[arg(long, default_value = "github-adapter.cursor")]
    cursor: std::path::PathBuf,
    /// Maximum memory rows per submit window (D21-a bound). At least
    /// 2: a PR plus a closing reference is the smallest legal window.
    #[arg(long, default_value = "100", value_parser = clap::value_parser!(u64).range(2..))]
    max_window: u64,
    /// Seed the cursor on a first run (RFC3339); later runs resume
    /// from the file.
    #[arg(long)]
    since: Option<String>,
    /// Visibility stamped on every row (private|project|team|org).
    /// Default org mirrors a single-tenant deployment whose boundary
    /// is the GitHub org; tighten per source when the org is wider
    /// than the repo's collaborator ACLs.
    #[arg(long, default_value = "org")]
    visibility: String,
    /// Page size for the source API.
    #[arg(long, default_value = "50")]
    page_size: u32,
}

/// Fetch one GraphQL page, retrying rate limits on the server's own
/// schedule (bounded), surfacing everything else.
async fn graphql_page(
    client: &ApiClient,
    token: &str,
    query: &str,
    variables: &mut serde_json::Value,
) -> Result<serde_json::Value, anyhow::Error> {
    let mut attempts = 0u32;
    loop {
        match client.graphql(token, query, variables).await {
            Ok(data) => return Ok(data),
            Err(ApiError::RateLimited { retry_after, .. }) => {
                attempts += 1;
                if attempts > 5 {
                    anyhow::bail!("source API rate limit persisted after {attempts} waits");
                }
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

    let visibility = exocortex_adapter_github::parse_visibility(&args.visibility)
        .ok_or_else(|| anyhow::anyhow!("--visibility must be private|project|team|org"))?;

    let api_token = std::env::var("GITHUB_TOKEN")
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow::anyhow!("GITHUB_TOKEN is required"))?;
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
    // is fatal (the SDK's own corrupt-cursor rule: never silently
    // re-ingest the world).
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
        &format!("github://{}/{}", args.owner, args.repo),
        &args.producer,
        &args.backend,
    );
    config.source_flavor = "github".into();
    config.producer_kind = exocortex_wire::ingest::v1::ProducerKind::SaasAdapter;
    config.auth_token = auth_token;
    config.hmac_key = hmac_key;
    config.cursor_path = args.cursor.with_extension("sdk-cursor");
    config.projection = Some(exocortex_adapter_github::projection(args.max_window));

    let mut session = exocortex_adapter_sdk::AdapterSession::connect(config).await?;
    let client = ApiClient::new(&args.endpoint)?.with_user_agent(concat!(
        "exocortex-adapter-github/",
        env!("CARGO_PKG_VERSION")
    ));

    // The fetch bound is HALF the declared per-run row bound (every
    // item may introduce closing-reference rows). Overrun is safe:
    // the SDK aborts with the cursor on the last settled window.
    let run_rows_bound = args.max_window.saturating_mul(100);
    let fetch_bound = run_rows_bound / 2;
    let mut rows_estimate: u64 = 0;

    // Issues: ascending under the inclusive since filter.
    let mut issues: Vec<GhIssue> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut variables = serde_json::json!({
            "owner": args.owner, "repo": args.repo, "first": args.page_size,
        });
        match (&resume, &after) {
            (Some(since), _) if !since.is_empty() => {
                variables["since"] = serde_json::Value::String(since.clone());
            }
            _ => {
                variables["since"] = serde_json::Value::Null;
            }
        }
        if let Some(after) = &after {
            variables["after"] = serde_json::Value::String(after.clone());
        }
        let data = graphql_page(
            &client,
            &api_token,
            exocortex_adapter_github::ISSUES_QUERY,
            &mut variables,
        )
        .await?;
        if data
            .get("repository")
            .and_then(|r| r.get("issues"))
            .is_none()
        {
            anyhow::bail!(
                "unexpected response shape: no repository.issues connection (a renamed field                  or a 200-error body would otherwise ingest as silence)"
            );
        }
        let (page, skipped, has_next, end_cursor) =
            exocortex_adapter_github::parse_issues_page(&data);
        if skipped > 0 {
            tracing::warn!(skipped, "malformed issue nodes skipped");
        }
        rows_estimate += page.len() as u64;
        issues.extend(page);
        if rows_estimate >= fetch_bound {
            tracing::warn!(
                rows_estimate,
                fetch_bound,
                "fetch bound reached (issues); re-run to continue"
            );
            break;
        }
        if !has_next || end_cursor.is_empty() {
            break;
        }
        after = Some(end_cursor);
    }

    // PRs: newest-first walk stopping at the cursor.
    let mut pulls: Vec<GhPull> = Vec::new();
    let mut after: Option<String> = None;
    'walk: loop {
        let mut variables = serde_json::json!({
            "owner": args.owner, "repo": args.repo, "first": args.page_size,
        });
        if let Some(after) = &after {
            variables["after"] = serde_json::Value::String(after.clone());
        }
        let data = graphql_page(
            &client,
            &api_token,
            exocortex_adapter_github::PULLS_QUERY,
            &mut variables,
        )
        .await?;
        if data
            .get("repository")
            .and_then(|r| r.get("pullRequests"))
            .is_none()
        {
            anyhow::bail!(
                "unexpected response shape: no repository.pullRequests connection (a renamed                  field or a 200-error body would otherwise ingest as silence)"
            );
        }
        let (page, skipped, has_next, end_cursor) =
            exocortex_adapter_github::parse_pulls_page(&data);
        if skipped > 0 {
            tracing::warn!(skipped, "malformed pull nodes skipped");
        }
        // The page is KEPT whole: pages mix rows newer than the cursor
        // with older ones, and dropping the page would drop the newer
        // rows too (round-10 R10-2). Older rows replay idempotently;
        // the walk stops after this page.
        let reached_backlog = page.iter().any(|pull| match resume.as_deref() {
            Some(cursor) if !cursor.is_empty() => pull.updated_at.as_str() < cursor,
            _ => false,
        });
        rows_estimate += page.iter().map(|p| 1 + p.closing.len() as u64).sum::<u64>();
        pulls.extend(page);
        if reached_backlog {
            break 'walk;
        }
        if rows_estimate >= fetch_bound {
            tracing::warn!(
                rows_estimate,
                fetch_bound,
                "fetch bound reached (pulls); re-run to continue"
            );
            break;
        }
        if !has_next || end_cursor.is_empty() {
            break;
        }
        after = Some(end_cursor);
    }
    // New pages arrive newest-first; windows emit oldest-first.
    pulls.reverse();

    // Closing references ride the window as their own rows so every
    // edge has both endpoints in-batch (deduped against window issues).
    let mut window_issues = issues;
    let known: std::collections::BTreeSet<u64> =
        window_issues.iter().map(|issue| issue.number).collect();
    for pull in &pulls {
        for issue in &pull.closing {
            if !known.contains(&issue.number) {
                window_issues.push(issue.clone());
            }
        }
    }

    let total = window_issues.len() + pulls.len();
    for (index, (issue_chunk, pull_chunk)) in chunk_windows(window_issues, pulls, args.max_window)?
        .into_iter()
        .enumerate()
    {
        let unit = exocortex_adapter_github::map_window(
            &args.owner,
            &args.repo,
            &issue_chunk,
            &pull_chunk,
            &format!("window-{index}"),
            visibility,
        );
        let cursor = exocortex_adapter_github::cursor_for(&issue_chunk, &pull_chunk)
            .ok_or_else(|| anyhow::anyhow!("empty window has no cursor"))?;
        let outcome = session.submit_window(vec![unit], &cursor).await?;
        // The operator-facing cursor advances with every settled
        // window (the SDK keeps its own file; this one is what the
        // next run resumes from).
        std::fs::write(&args.cursor, &cursor)?;
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
    }
    println!(
        "ingested {total} issues+pulls (resume at or after {:?})",
        resume.as_deref().unwrap_or("the beginning")
    );
    Ok(())
}

/// Chunk (issues, pulls) into windows whose MEMORY row count (items +
/// closing-reference rows) respects the bound. Items keep their order;
/// a chunk closes when adding the next item (and, for a PR, its
/// closing refs) would exceed the bound. Returns `(issue_chunk,
/// pull_chunk)` pairs, oldest-first.
fn chunk_windows(
    issues: Vec<GhIssue>,
    pulls: Vec<GhPull>,
    max_window: u64,
) -> anyhow::Result<Vec<(Vec<GhIssue>, Vec<GhPull>)>> {
    let mut out: Vec<(Vec<GhIssue>, Vec<GhPull>)> = Vec::new();
    let mut issue_iter = issues.into_iter().peekable();
    let mut pull_iter = pulls.into_iter().peekable();
    // Interleave by updatedAt so each window is a contiguous oldest-first
    // slice of the stream: whichever head is older goes next.
    loop {
        let mut chunk_issues: Vec<GhIssue> = Vec::new();
        let mut chunk_pulls: Vec<GhPull> = Vec::new();
        let mut rows: u64 = 0;
        loop {
            let next_cost = match (issue_iter.peek(), pull_iter.peek()) {
                (Some(issue), Some(pull)) => {
                    if issue.updated_at <= pull.updated_at {
                        Some(1)
                    } else {
                        Some(1 + pull.closing.len() as u64)
                    }
                }
                (Some(_), None) => Some(1),
                (None, Some(pull)) => Some(1 + pull.closing.len() as u64),
                (None, None) => None,
            };
            let Some(cost) = next_cost else { break };
            if rows + cost > max_window && !(chunk_issues.is_empty() && chunk_pulls.is_empty()) {
                break;
            }
            if chunk_issues.is_empty()
                && chunk_pulls.is_empty()
                && rows + cost > max_window
                && cost > 1
            {
                // A single item whose closing references alone exceed
                // the declared bound cannot be submitted without
                // violating it — name the fix rather than building an
                // unsplittable window the SDK must reject forever
                // (round-10 R10-3).
                anyhow::bail!(
                    "a pull request carries {} closing references, exceeding --max-window                      {max_window}; raise --max-window above its cost",
                    cost - 1
                );
            }
            let take_issue = match (issue_iter.peek(), pull_iter.peek()) {
                (Some(issue), Some(pull)) => issue.updated_at <= pull.updated_at,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => unreachable!("cost was Some"),
            };
            if take_issue {
                chunk_issues.push(issue_iter.next().unwrap());
            } else {
                chunk_pulls.push(pull_iter.next().unwrap());
            }
            rows += cost;
        }
        if chunk_issues.is_empty() && chunk_pulls.is_empty() {
            break;
        }
        out.push((chunk_issues, chunk_pulls));
    }
    Ok(out)
}
