//! D19 live leg: one issues page and one PRs page from the REAL
//! GitHub GraphQL v4 API, through the real transport, into the real
//! parsers and mapper. Gated exactly like the Postgres CDC live leg —
//! feature `integration` + `GITHUB_TOKEN` — and it SKIPS LOUDLY when
//! either is absent, so a green default run never claims live coverage
//! it did not execute.
//!
//! ```sh
//! GITHUB_TOKEN=github_pat_… \
//!   cargo test -p exocortex-adapter-github \
//!     --features integration --test github_live -- --nocapture
//! ```

#[cfg(feature = "integration")]
#[tokio::test]
async fn one_live_page_parses_and_maps() -> anyhow::Result<()> {
    let Some(token) = std::env::var("GITHUB_TOKEN").ok().filter(|v| !v.is_empty()) else {
        eprintln!("live GitHub suite UNEXECUTED (GITHUB_TOKEN unset)");
        return Ok(());
    };
    // A small public repo exercises the schema without depending on
    // the owner's private data; any repo works.
    let client = exocortex_api_client::ApiClient::new("https://api.github.com/graphql")?;
    let issues_data = client
        .graphql(
            &token,
            exocortex_adapter_github::ISSUES_QUERY,
            &serde_json::json!({ "owner": "memory-graph", "repo": "exocortex", "first": 5 }),
        )
        .await?;
    let (issues, skipped, _, _) = exocortex_adapter_github::parse_issues_page(&issues_data);
    assert_eq!(
        skipped, 0,
        "the live schema must parse cleanly or the mapping is stale"
    );
    eprintln!("live issues page: {} issues", issues.len());

    let pulls_data = client
        .graphql(
            &token,
            exocortex_adapter_github::PULLS_QUERY,
            &serde_json::json!({ "owner": "memory-graph", "repo": "exocortex", "first": 5 }),
        )
        .await?;
    let (pulls, skipped, _, _) = exocortex_adapter_github::parse_pulls_page(&pulls_data);
    assert_eq!(
        skipped, 0,
        "the live schema must parse cleanly or the mapping is stale"
    );
    eprintln!("live pulls page: {} pulls", pulls.len());
    if !issues.is_empty() || !pulls.is_empty() {
        let unit = exocortex_adapter_github::map_window(
            "memory-graph",
            "exocortex",
            &issues,
            &pulls,
            "live-page",
        );
        assert!(unit.snapshot.is_some());
    }
    Ok(())
}

#[cfg(not(feature = "integration"))]
#[test]
fn live_github_suite_requires_the_integration_feature() {
    // Loud, not silent: the default suite reports exactly what is not
    // running (the storage-conformance umbrella prints the same line
    // for its live legs).
    eprintln!(
        "live GitHub suite UNEXECUTED (exocortex-adapter-github built without the \
         `integration` feature)"
    );
}
