//! D19 (master plan, SaaS-API adapter family): the shared first-party
//! HTTPS client SaaS adapters speak through — Linear's GraphQL today,
//! GitHub's next, Jira/ServiceNow later.
//!
//! Deliberately minimal (the D20 first-party-replication-client
//! precedent): POST JSON with a bearer token over the workspace's
//! existing hyper 0.14 + hyper-rustls stack. No new dependency
//! (PUBLISHING.md rule 9). The two things a SaaS API gives that a
//! generic HTTP client would hide are surfaced as typed values, so
//! adapters back off deterministically instead of guessing:
//!
//! - rate-limit signals — [`RateState`] carries `Retry-After` and
//!   `x-ratelimit-remaining`/`-reset` out of every response;
//!   a 429 is [`ApiError::RateLimited`], never a string to re-parse;
//! - GraphQL error envelopes — a 200 whose body carries `{"errors":
//!   [...]}` is [`ApiError::Graphql`], with the messages joined.
//!
//! No retries live here: the adapter owns source-API policy (bounded
//! sleeps over these typed signals); the SDK's `RetryPolicy` stays for
//! the backend leg. Transport only — no retries, no caching, no
//! inference, no LLM.
//!
//! `https` endpoints use the platform trust store via rustls
//! (native-tokio); `http` is permitted for local mocks and proxies the
//! operator pins deliberately — a non-local plaintext endpoint draws a
//! loud warning, because the bearer token would travel unencrypted.
//! Response bodies are capped ([`BODY_CAP`]) so a hostile or broken
//! endpoint cannot exhaust the host; exceeding the cap is an error,
//! never a truncation.

use std::time::Duration;

/// The response-body cap. A GraphQL page at the adapters' page size is
/// tens of KiB; 16 MiB is two orders of headroom and still bounded.
pub const BODY_CAP: usize = 16 * 1024 * 1024;

/// A request timeout covering connect + headers + body, applied
/// internally to every request this client sends (kept here so every
/// adapter agrees on one number).
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Rate-limit signals pulled out of one response's headers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RateState {
    /// `Retry-After` in seconds, when the server sent the delay form.
    pub retry_after: Option<Duration>,
    /// `x-ratelimit-remaining` (GitHub REST/GraphQL), when present.
    pub remaining: Option<u64>,
    /// `x-ratelimit-reset` (epoch seconds, GitHub), when present.
    pub reset_epoch: Option<u64>,
}

impl RateState {
    /// Read the rate-limit headers off a response.
    pub fn from_headers(headers: &hyper::HeaderMap) -> Self {
        Self {
            retry_after: retry_after(headers),
            remaining: header_u64(headers, "x-ratelimit-remaining"),
            reset_epoch: header_u64(headers, "x-ratelimit-reset"),
        }
    }
}

fn header_u64(headers: &hyper::HeaderMap, name: &str) -> Option<u64> {
    headers.get(name)?.to_str().ok()?.trim().parse::<u64>().ok()
}

/// `Retry-After` in the delay-seconds form. The HTTP-date form is
/// deliberately not interpreted (no clock dependence); `None` means
/// "back off with the adapter's own policy".
fn retry_after(headers: &hyper::HeaderMap) -> Option<Duration> {
    header_u64(headers, "retry-after").map(Duration::from_secs)
}

/// Errors the API client produces. Transport-level only; adapter policy
/// decides what to retry.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Connection, TLS, or body I/O failure.
    #[error("transport: {0}")]
    Transport(String),
    /// Endpoint is not a valid `http(s)` URL.
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    /// Non-2xx status, with the leading bytes of the body for the log.
    #[error("http {status}: {summary}")]
    Status {
        /// HTTP status code.
        status: u16,
        /// Leading bytes of the response body, for operators.
        summary: String,
        /// Rate signals the same response carried.
        rate: RateState,
    },
    /// 429 (or a 403 carrying GitHub's exhausted-`remaining` signal).
    /// Back off by `retry_after` when present, else adapter policy.
    #[error("rate limited (retry after {retry_after:?}, remaining {remaining:?})")]
    RateLimited {
        /// The server-stated delay, when it stated one.
        retry_after: Option<Duration>,
        /// Remaining quota, when the API reports it.
        remaining: Option<u64>,
    },
    /// A GraphQL response whose `errors` array is non-empty.
    #[error("graphql: {0}")]
    Graphql(String),
    /// Response body exceeded [`BODY_CAP`].
    #[error("response body exceeded the {limit}-byte cap")]
    BodyTooLarge {
        /// The cap that was hit.
        limit: usize,
    },
    /// Response body was not valid JSON.
    #[error("response was not JSON: {0}")]
    NotJson(String),
}

/// A minimal bearer-authenticated JSON POST client for one endpoint.
/// The connection (and TLS session) is built ONCE and reused for every
/// request — a per-request client pays a TCP+TLS handshake and a
/// platform-trust-store load per page.
pub struct ApiClient {
    endpoint: hyper::Uri,
    user_agent: String,
    client: hyper::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body>,
}

/// A loopback or link-local host: plaintext `http` is the local-mock
/// case, not a credential hazard.
fn is_local_host(host: &str) -> bool {
    // Loopback literals only: an mDNS `.local` name can resolve to any
    // LAN peer, and the warning is exactly for that case.
    let host = host.strip_prefix('[').unwrap_or(host);
    let host = host.strip_suffix(']').unwrap_or(host);
    let host = host.rsplit_once(':').map(|(h, _)| h).unwrap_or(host);
    matches!(host, "localhost" | "127.0.0.1" | "::1" | "0.0.0.0")
}

impl ApiClient {
    /// Build a client for one API endpoint (`https://api.linear.app/graphql`,
    /// `https://api.github.com/graphql`, ...).
    pub fn new(endpoint: &str) -> Result<Self, ApiError> {
        let uri: hyper::Uri = endpoint
            .parse()
            .map_err(|e| ApiError::InvalidEndpoint(format!("{endpoint}: {e}")))?;
        match uri.scheme_str() {
            Some("https") => {}
            // Local mocks and pinned proxies only; never a live SaaS API.
            Some("http") => {
                let host = uri.host().unwrap_or_default();
                if !is_local_host(host) {
                    tracing::warn!(
                        endpoint,
                        "plaintext http endpoint: the bearer token travels unencrypted; local mocks and pinned proxies only"
                    );
                }
            }
            other => {
                return Err(ApiError::InvalidEndpoint(format!(
                    "{endpoint}: scheme must be http or https, got {other:?}"
                )));
            }
        }
        Ok(Self {
            endpoint: uri,
            user_agent: format!("exocortex-api-client/{}", env!("CARGO_PKG_VERSION")),
            client: hyper::Client::builder().build(build_connector()?),
        })
    }

    /// Override the User-Agent (adapters name themselves; GitHub refuses
    /// requests without one, with an empty 403).
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    /// POST `body` as JSON with `Authorization: Bearer <token>`.
    /// Returns `(status, rate signals, parsed JSON)`. A 429 maps to
    /// [`ApiError::RateLimited`]; other non-2xx to [`ApiError::Status`].
    pub async fn post_json(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<(u16, RateState, serde_json::Value), ApiError> {
        let payload = serde_json::to_vec(body)
            .map_err(|e| ApiError::Transport(format!("serialize request: {e}")))?;
        let request = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(self.endpoint.clone())
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .header(hyper::header::AUTHORIZATION, format!("Bearer {token}"))
            .header(hyper::header::USER_AGENT, &self.user_agent)
            .body(hyper::Body::from(payload))
            .map_err(|e| ApiError::Transport(format!("build request: {e}")))?;
        // ONE deadline over connect + headers + BODY: a timeout that
        // ends at the headers leaves a trickling-body endpoint free to
        // hang the adapter forever.
        let (status, rate, bytes) = tokio::time::timeout(REQUEST_TIMEOUT, async {
            let response = self
                .client
                .request(request)
                .await
                .map_err(|e| ApiError::Transport(e.to_string()))?;
            let status = response.status().as_u16();
            let rate = RateState::from_headers(response.headers());
            let bytes = read_capped(response).await?;
            Ok::<_, ApiError>((status, rate, bytes))
        })
        .await
        .map_err(|_| ApiError::Transport("request timed out".into()))??;
        if let Some(limited) = rate_limited(status, &rate) {
            return Err(limited);
        }
        if !(200..300).contains(&status) {
            return Err(ApiError::Status {
                status,
                summary: summarize(&bytes, token),
                rate,
            });
        }
        let json = serde_json::from_slice(&bytes)
            .map_err(|e| ApiError::NotJson(format!("{e}; body: {}", summarize(&bytes, token))))?;
        Ok((status, rate, json))
    }

    /// POST one GraphQL document and return its `data` value. A 200
    /// carrying a non-empty `errors` array is [`ApiError::Graphql`] —
    /// GraphQL transports application errors inside a success status.
    pub async fn graphql(
        &self,
        token: &str,
        query: &str,
        variables: &serde_json::Value,
    ) -> Result<serde_json::Value, ApiError> {
        let body = serde_json::json!({ "query": query, "variables": variables });
        let (_status, _rate, json) = self.post_json(token, &body).await?;
        if let Some(errors) = json.get("errors").and_then(|e| e.as_array()) {
            if !errors.is_empty() {
                let messages: Vec<String> = errors
                    .iter()
                    .map(|e| {
                        e.get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("(no message)")
                            .to_string()
                    })
                    .collect();
                return Err(ApiError::Graphql(messages.join("; ")));
            }
        }
        Ok(json.get("data").cloned().unwrap_or(serde_json::Value::Null))
    }
}

/// The rate-limit decision over one response's status + signals,
/// extracted so the mapping itself is testable without a network.
/// 429 is always rate limiting. A 403 counts when GitHub reports an
/// exhausted primary quota (`x-ratelimit-remaining: 0`) OR when the
/// server stated a delay (`Retry-After`) — the secondary/abuse
/// limiters return 403 with Retry-After but a non-zero primary
/// remaining, and aborting a run on a server that said "wait 30s"
/// wastes the run.
fn rate_limited(status: u16, rate: &RateState) -> Option<ApiError> {
    let quota_exhausted = status == 403 && rate.remaining == Some(0);
    let server_stated_delay = status == 403 && rate.retry_after.is_some();
    (status == 429 || quota_exhausted || server_stated_delay).then_some(ApiError::RateLimited {
        retry_after: rate.retry_after,
        remaining: rate.remaining,
    })
}

/// Leading bytes of a response body for operator-facing error strings,
/// with any echo of the request's own bearer token redacted (a hostile
/// or misbehaving endpoint that echoes the `Authorization` header back
/// must not land the credential in logs).
fn summarize(bytes: &[u8], secret: &str) -> String {
    let text = String::from_utf8_lossy(bytes);
    let summary: String = text.chars().take(500).collect();
    if secret.len() >= 4 {
        summary.replace(secret, "***")
    } else {
        summary
    }
}

/// Build the process-reusable TLS connector (both schemes: the pinned
/// endpoint's URI decides per request; the scheme is validated at
/// construction). The trust-store load can panic on platforms without
/// roots — an environment error the operator can fix, surfaced as a
/// typed error (the exocortex-client SSE precedent).
fn build_connector() -> Result<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>, ApiError>
{
    // `with_native_roots` loads the platform trust store eagerly and
    // PANICS on failure. On macOS the Keychain read flakes
    // intermittently (security-framework -36, observed live in the
    // D19 GitHub leg: one run loads, the next does not), so the
    // load is retried a bounded three times before surfacing — the
    // D29 supervisor lesson: name WHY, and do not die on a
    // transient the second attempt survives.
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match std::panic::catch_unwind(|| {
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                .enable_http1()
                .enable_http2()
                .build()
        }) {
            Ok(connector) => return Ok(connector),
            Err(payload) if attempt < 3 => {
                let reason = payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "(panic payload not a string)".into());
                tracing::warn!(attempt, %reason, "platform trust store load failed; retrying");
                std::thread::sleep(std::time::Duration::from_millis(250 * u64::from(attempt)));
            }
            Err(payload) => {
                let reason = payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "(panic payload not a string)".into());
                return Err(ApiError::Transport(format!(
                    "failed to load the platform TLS trust store after {attempt} attempts: {reason}"
                )));
            }
        }
    }
}

async fn read_capped(response: hyper::Response<hyper::Body>) -> Result<Vec<u8>, ApiError> {
    use hyper::body::HttpBody as _;
    let mut body = response.into_body();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|e| ApiError::Transport(e.to_string()))?;
        if buf.len() + chunk.len() > BODY_CAP {
            return Err(ApiError::BodyTooLarge { limit: BODY_CAP });
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> hyper::HeaderMap {
        let mut map = hyper::HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        map
    }

    #[test]
    fn rate_state_reads_delay_and_quota_headers() {
        let rate = RateState::from_headers(&headers(&[
            ("retry-after", "29"),
            ("x-ratelimit-remaining", "0"),
            ("x-ratelimit-reset", "1756900000"),
        ]));
        assert_eq!(rate.retry_after, Some(Duration::from_secs(29)));
        assert_eq!(rate.remaining, Some(0));
        assert_eq!(rate.reset_epoch, Some(1_756_900_000));
    }

    #[test]
    fn malformed_rate_headers_are_absent_never_guessed() {
        let rate = RateState::from_headers(&headers(&[
            ("retry-after", "Tue, 09 Sep 2026 12:00:00 GMT"), // date form: not interpreted
            ("x-ratelimit-remaining", "soon"),
        ]));
        assert_eq!(rate.retry_after, None, "date form is not guessed");
        assert_eq!(rate.remaining, None, "non-numeric is not guessed");
        assert_eq!(rate, RateState::default());
    }

    #[test]
    fn rate_limited_maps_429_exhausted_quota_and_secondary_limits() {
        for (status, rate, hits) in [
            (429u16, RateState::default(), true),
            (
                403,
                RateState {
                    remaining: Some(0),
                    ..Default::default()
                },
                true,
            ),
            (
                403,
                RateState {
                    retry_after: Some(Duration::from_secs(30)),
                    ..Default::default()
                },
                true,
            ),
            (403, RateState::default(), false),
            (200, RateState::default(), false),
        ] {
            match (rate_limited(status, &rate), hits) {
                (
                    Some(ApiError::RateLimited {
                        retry_after,
                        remaining,
                    }),
                    true,
                ) => {
                    assert_eq!(retry_after, rate.retry_after, "payload carries the delay");
                    assert_eq!(remaining, rate.remaining, "payload carries the quota");
                }
                (None, false) => {}
                (other, _) => {
                    panic!("status {status} with {rate:?}: expected hits={hits}, got {other:?}")
                }
            }
        }
    }

    #[test]
    fn endpoints_must_be_http_or_https() {
        assert!(ApiClient::new("https://api.linear.app/graphql").is_ok());
        assert!(ApiClient::new("http://127.0.0.1:9955/graphql").is_ok());
        for bad in ["ftp://x", "api.linear.app", ""] {
            assert!(ApiClient::new(bad).is_err(), "{bad} must be refused");
        }
    }

    #[test]
    fn summaries_redact_the_bearer_token() {
        // A hostile or misbehaving endpoint that echoes the
        // Authorization header back in its error body must not land
        // the credential in operator-facing error strings.
        let token = "ghp_secrettoken1234567890";
        let body = format!("Authorization: Bearer {token} and more text");
        let summary = summarize(body.as_bytes(), token);
        assert!(!summary.contains(token), "token redacted: {summary}");
        assert!(summary.contains("***"), "redaction marker present");
        assert!(summary.contains("and more text"), "context preserved");
    }
}
