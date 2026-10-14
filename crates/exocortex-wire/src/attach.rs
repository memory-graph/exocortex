//! D44-S2: the attach record a standalone owner publishes beside its
//! graph, and D55: the SAME record is the re-resolution point a
//! long-lived client consults when its owner is replaced. Server and
//! client parse ONE shape (this module); the record never leaves the
//! machine (loopback-only backend, 0600 file in a single-user data
//! home), so it is config, not protocol — but it is still ONE
//! implementation, and it must fail closed on every field (an attacher
//! must never guess at credentials).

use serde::{Deserialize, Serialize};

/// The attach record: the endpoint and secrets a second concurrent
/// session reuses in client mode instead of starting a second store —
/// and, after an owner replacement (D55), the endpoint and FRESH
/// credentials an orphaned client adopts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachInfo {
    /// The owner node's loopback HTTP/gRPC base URL.
    pub backend: String,
    /// The org the owner serves (R14-final): attachers refuse a
    /// different `--org` instead of hydrating a foreign partition.
    pub org: String,
    /// The owner's bearer (EXOCORTEX_AUTH_TOKEN).
    pub auth_token: String,
    /// The owner's producer key (EXOCORTEX_HMAC_KEY, 64 hex).
    pub hmac_key: String,
    /// The owner's SSE client key (64 hex).
    pub sse_key: String,
}

impl AttachInfo {
    /// Every field must be present and well-shaped; a malformed record
    /// fails closed. R14-B6: the backend is pinned to a literal
    /// loopback host with no userinfo, path, query, or fragment — a
    /// string prefix admits userinfo (`http://127.0.0.1:1@evil.example/`
    /// passes the prefix and dials evil), and a path or query can carry
    /// quote bytes that escape the single-quoted env file the wrapper
    /// sources. Validated structurally here (wire adds no HTTP
    /// dependency): scheme-literal, exactly one authority token,
    /// loopback host, explicit numeric port, nothing after it. The
    /// secrets ride the ONE hex validator (`signing::decode_hex32`).
    pub fn validate(&self) -> Result<(), String> {
        let rest = self.backend.strip_prefix("http://").ok_or_else(|| {
            format!(
                "attach record backend must be plain http, got {}",
                self.backend
            )
        })?;
        // Exactly one path segment: a second '/', or any query/fragment
        // delimiter, means something follows the authority — refuse.
        // (The owner always renders a bare `host:port`.)
        if rest.contains('/') || rest.contains('?') || rest.contains('#') {
            return Err(format!(
                "attach record backend must carry no path, query, or fragment, got {}",
                self.backend
            ));
        }
        // R14-B6: userinfo smuggling rides '@'.
        if rest.contains('@') {
            return Err(format!(
                "attach record backend carries userinfo: {}",
                self.backend
            ));
        }
        let (host, port) = rest.rsplit_once(':').ok_or_else(|| {
            format!(
                "attach record backend must carry an explicit port, got {}",
                self.backend
            )
        })?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if !matches!(host, "127.0.0.1" | "::1" | "localhost") {
            return Err(format!(
                "attach record backend must be loopback, got {host}"
            ));
        }
        if port.parse::<u16>().is_err() || port.is_empty() {
            return Err(format!(
                "attach record backend must carry a numeric port, got {}",
                self.backend
            ));
        }
        for (name, value) in [
            ("auth_token", &self.auth_token),
            ("hmac_key", &self.hmac_key),
            ("sse_key", &self.sse_key),
        ] {
            crate::signing::decode_hex32(value)
                .map_err(|e| format!("attach record {name}: {e}"))?;
        }
        Ok(())
    }

    /// Render the runtime-env lines a wrapper sources: the backend + SSE
    /// key every session gets, the owner's credentials (an attacher must
    /// present the owner's identity — standalone serves ONE principal),
    /// the attach marker the wrapper keys its per-session WAL slot on,
    /// and (D55) the data home a long-lived client re-reads the attach
    /// record from when its owner is replaced. Hex values cannot carry
    /// shell-significant bytes; the data home is single-quoted and the
    /// owner's own path — still, refuse a quote in it rather than emit a
    /// file that would break the sourcing shell.
    pub fn runtime_env_lines(&self, data_home: &std::path::Path) -> String {
        let data_home = data_home.to_string_lossy();
        let data_home = if data_home.contains('\'') {
            String::new()
        } else {
            data_home.into_owned()
        };
        format!(
            "EXOCORTEX_BACKEND='{}'\nEXOCORTEX_SSE_KEY='{}'\nEXOCORTEX_AUTH_TOKEN='{}'\nEXOCORTEX_HMAC_KEY='{}'\nEXOCORTEX_ATTACHED='1'\nEXOCORTEX_STANDALONE_DATA_HOME='{}'\n",
            self.backend, self.sse_key, self.auth_token, self.hmac_key, data_home
        )
    }

    /// Read + validate the attach record from `<data_dir>/attach.json`.
    pub fn read(data_dir: &std::path::Path) -> Result<Self, String> {
        let path = data_dir.join("attach.json");
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| format!("no attach record at {}: {e}", path.display()))?;
        let info: AttachInfo =
            serde_json::from_str(&raw).map_err(|e| format!("malformed attach record: {e}"))?;
        info.validate()?;
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn well_formed() -> AttachInfo {
        AttachInfo {
            backend: "http://127.0.0.1:54326".into(),
            org: "personal".into(),
            auth_token: "ab".repeat(32),
            hmac_key: "cd".repeat(32),
            sse_key: "ef".repeat(32),
        }
    }

    #[test]
    fn attach_record_round_trips_and_fails_closed_on_malformation() {
        let info = well_formed();
        info.validate().expect("well-formed record validates");
        let parsed: AttachInfo =
            serde_json::from_str(&serde_json::to_string(&info).unwrap()).unwrap();
        parsed.validate().expect("round trip validates");
        // R14-B6: userinfo behind a passing prefix must be refused.
        for backend in [
            "http://127.0.0.1:1@evil.example/",
            "https://127.0.0.1:54326",
            "http://10.0.0.5:54326",
            "http://127.0.0.1",
            "http://127.0.0.1:54326/credentials",
            "http://127.0.0.1:54326?x=1",
            "http://127.0.0.1:notaport",
            "",
        ] {
            let mut bad = well_formed();
            bad.backend = backend.into();
            assert!(bad.validate().is_err(), "{backend} must be refused");
        }
        for field in ["auth_token", "hmac_key", "sse_key"] {
            let mut bad = well_formed();
            match field {
                "auth_token" => bad.auth_token = "zz".into(),
                "hmac_key" => bad.hmac_key = "short".into(),
                _ => bad.sse_key = "ab".repeat(31),
            }
            assert!(bad.validate().is_err(), "{field} must fail closed");
        }
    }

    #[test]
    fn runtime_env_lines_carry_the_data_home_and_never_a_quote() {
        let info = well_formed();
        let lines = info.runtime_env_lines(std::path::Path::new("/tmp/data home"));
        assert!(lines.contains("EXOCORTEX_STANDALONE_DATA_HOME='/tmp/data home'"));
        assert!(lines.contains("EXOCORTEX_ATTACHED='1'"));
        let hostile = info.runtime_env_lines(std::path::Path::new("/tmp/x'w"));
        assert!(hostile.contains("EXOCORTEX_STANDALONE_DATA_HOME=''"));
    }

    #[test]
    fn read_fails_closed_when_absent_or_malformed() {
        let dir = std::env::temp_dir().join(format!("attach-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        assert!(AttachInfo::read(&dir).is_err());
        std::fs::write(dir.join("attach.json"), "{ not json").unwrap();
        assert!(AttachInfo::read(&dir).is_err());
        let raw = serde_json::to_string(&well_formed()).unwrap();
        std::fs::write(dir.join("attach.json"), raw).unwrap();
        assert!(AttachInfo::read(&dir).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }
}
