//! D19 live leg: one issues page from the REAL Linear GraphQL API,
//! through the real transport, into the real parser and mapper. Gated
//! exactly like the Postgres CDC live leg — feature `integration` +
//! `LINEAR_API_KEY` — and it SKIPS LOUDLY when either is absent, so a
//! green default run never claims live coverage it did not execute.
//!
//! Proves the whole first-party path: bearer auth over the api-client
//! (hyper + rustls), the window query's shape against the live schema,
//! pagination bookkeeping, and that real nodes parse into typed rows
//! (a mapping change at Linear's end fails here first, not in a
//! backfill).
//!
//! ```sh
//! LINEAR_API_KEY=lin_api_… \
//!   cargo test -p exocortex-adapter-linear \
//!     --features integration --test linear_live -- --nocapture
//! ```

#[cfg(feature = "integration")]
#[tokio::test]
async fn one_live_page_parses_and_maps() -> anyhow::Result<()> {
    let Some(key) = std::env::var("LINEAR_API_KEY")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        eprintln!("live Linear suite UNEXECUTED (LINEAR_API_KEY unset)");
        return Ok(());
    };
    let client = exocortex_api_client::ApiClient::new("https://api.linear.app/graphql")?;
    let data = client
        .graphql(
            &key,
            exocortex_adapter_linear::ISSUES_QUERY,
            &serde_json::json!({ "first": 5, "gte": null }),
        )
        .await?;
    let (issues, skipped, has_next, end_cursor) =
        exocortex_adapter_linear::parse_issues_page(&data);
    assert_eq!(
        skipped, 0,
        "the live schema must parse cleanly or the mapping is stale"
    );
    eprintln!(
        "live page: {} issues, has_next={has_next}, end_cursor={end_cursor:?}",
        issues.len()
    );
    if !issues.is_empty() {
        let unit = exocortex_adapter_linear::map_issues("live", &issues, "live-page");
        assert!(unit.snapshot.is_some());
        assert_eq!(unit.memories.len() >= issues.len(), true);
    }
    Ok(())
}

#[cfg(not(feature = "integration"))]
#[test]
fn live_linear_suite_requires_the_integration_feature() {
    // Loud, not silent: the default suite reports exactly what is not
    // running (the storage-conformance umbrella prints the same line
    // for its live legs).
    eprintln!(
        "live Linear suite UNEXECUTED (exocortex-adapter-linear built without the \
         `integration` feature)"
    );
}
