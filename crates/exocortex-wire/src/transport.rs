//! Shared admission policy for backend transport endpoints.

use std::net::IpAddr;

/// Admit encrypted remote backends and the explicit local-development
/// plaintext exception. This must run before credentials are attached or
/// connection retries begin.
pub fn validate_backend_url(url: &str) -> Result<(), String> {
    let endpoint = tonic::transport::Endpoint::from_shared(url.to_owned())
        .map_err(|error| format!("invalid backend URL: {error}"))?;
    let uri = endpoint.uri();
    match uri.scheme_str() {
        Some("https") => Ok(()),
        Some("http") => {
            let host = uri
                .host()
                .ok_or_else(|| "plaintext backend URL has no host".to_string())?;
            let literal_host = host
                .strip_prefix('[')
                .and_then(|host| host.strip_suffix(']'))
                .unwrap_or(host);
            if host.eq_ignore_ascii_case("localhost")
                || literal_host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
            {
                Ok(())
            } else {
                Err(
                    "plaintext backend URL is allowed only for literal loopback or localhost"
                        .into(),
                )
            }
        }
        _ => Err("backend URL must use https (http is loopback-only)".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::validate_backend_url;

    #[test]
    fn encrypted_remote_and_plaintext_loopback_are_admitted() {
        for url in [
            "https://backend.example:50051",
            "http://localhost:50051",
            "http://127.0.0.1:50051",
            "http://[::1]:50051",
        ] {
            assert!(validate_backend_url(url).is_ok(), "{url}");
        }
    }

    #[test]
    fn plaintext_remote_and_lookalike_hosts_are_rejected() {
        for url in [
            "http://backend.example:50051",
            "http://10.0.0.1:50051",
            "http://localhost.example:50051",
        ] {
            assert!(validate_backend_url(url).is_err(), "{url}");
        }
    }
}
