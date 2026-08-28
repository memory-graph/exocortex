//! Administrator-owned bearer credential to request-principal mapping.

use std::collections::HashSet;
use std::path::Path;

use exocortex_kernel::Visibility;
use exocortex_storage::VisibilityContext;
use serde::Deserialize;

/// Minimum entropy-bearing credential width accepted at any server boundary.
pub const MIN_BEARER_TOKEN_BYTES: usize = 32;

/// One administrator provisioned credential and its complete read/write scope.
#[derive(Deserialize)]
struct PrincipalPolicyRow {
    bearer_token: String,
    org_id: String,
    user_id: String,
    #[serde(default)]
    project_ids: Vec<String>,
    #[serde(default)]
    team_ids: Vec<String>,
    max_visibility: u8,
    #[serde(default)]
    audit_admin: bool,
}

/// One authenticated bearer principal and its operation permissions.
#[derive(Clone)]
pub struct AuthenticatedPrincipal {
    /// Exact tenant, membership, identity, and visibility scope.
    pub visibility: VisibilityContext,
    /// Explicit permission to read the organization-wide audit ledger.
    pub audit_admin: bool,
}

/// Immutable credential registry installed at process startup.
#[derive(Clone)]
pub struct PrincipalRegistry {
    entries: Vec<(Vec<u8>, AuthenticatedPrincipal)>,
}

impl PrincipalRegistry {
    /// Load and validate a JSON policy file. Missing, empty, duplicated, or
    /// malformed credentials fail startup rather than widening access.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read principal policy {}: {e}", path.display()))?;
        let rows: Vec<PrincipalPolicyRow> = serde_json::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("parse principal policy {}: {e}", path.display()))?;
        Self::from_rows(rows)
    }

    /// Construct the single-principal registry used by embedded tests. The
    /// production node uses [`Self::load`] exclusively.
    pub fn single(token: String, principal: VisibilityContext) -> anyhow::Result<Self> {
        Self::single_with_audit_admin(token, principal, true)
    }

    /// Construct one explicitly permissioned principal for embedded tests.
    pub fn single_with_audit_admin(
        token: String,
        principal: VisibilityContext,
        audit_admin: bool,
    ) -> anyhow::Result<Self> {
        validate_bearer_token(&token)?;
        Ok(Self {
            entries: vec![(
                token.into_bytes(),
                AuthenticatedPrincipal {
                    visibility: principal,
                    audit_admin,
                },
            )],
        })
    }

    fn from_rows(rows: Vec<PrincipalPolicyRow>) -> anyhow::Result<Self> {
        anyhow::ensure!(!rows.is_empty(), "principal policy must not be empty");
        let mut tokens = HashSet::new();
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            anyhow::ensure!(
                !row.org_id.is_empty() && !row.user_id.is_empty(),
                "principal org_id and user_id must be non-empty"
            );
            validate_bearer_token(&row.bearer_token)?;
            anyhow::ensure!(
                row.project_ids.iter().all(|id| !id.is_empty())
                    && row.team_ids.iter().all(|id| !id.is_empty()),
                "principal project/team ids must be non-empty"
            );
            anyhow::ensure!(
                tokens.insert(row.bearer_token.clone()),
                "duplicate bearer credential in principal policy"
            );
            let max_visibility = match row.max_visibility {
                0 => Visibility::Private,
                1 => Visibility::Project,
                2 => Visibility::Team,
                3 => Visibility::Org,
                4 => Visibility::Public,
                other => anyhow::bail!("principal max_visibility {other} is outside 0..=4"),
            };
            entries.push((
                row.bearer_token.into_bytes(),
                AuthenticatedPrincipal {
                    visibility: VisibilityContext {
                        user_id: row.user_id.into(),
                        org_id: row.org_id.into(),
                        project_ids: row.project_ids.into_iter().map(Into::into).collect(),
                        team_ids: row.team_ids.into_iter().map(Into::into).collect(),
                        max_visibility,
                    },
                    audit_admin: row.audit_admin,
                },
            ));
        }
        Ok(Self { entries })
    }

    /// Authenticate a bearer token with a length-hiding full-registry scan.
    pub fn authenticate(&self, token: &[u8]) -> Option<AuthenticatedPrincipal> {
        self.entries
            .iter()
            .find(|(candidate, _)| constant_time_eq(token, candidate))
            .map(|(_, principal)| principal.clone())
    }

    /// Fail startup when a single-org node is provisioned with credentials
    /// for another tenant. Storage graph selection and authorization policy
    /// must name the same organization.
    pub fn ensure_org(&self, expected: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.entries
                .iter()
                .all(|(_, principal)| principal.visibility.org_id.as_str() == expected),
            "principal policy contains an org other than node org {expected}"
        );
        Ok(())
    }
}

fn validate_bearer_token(token: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        token.len() >= MIN_BEARER_TOKEN_BYTES,
        "bearer token must contain at least {MIN_BEARER_TOKEN_BYTES} bytes"
    );
    Ok(())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let max_len = a.len().max(b.len());
    let mut diff = a.len() ^ b.len();
    for index in 0..max_len {
        diff |= usize::from(a.get(index).copied().unwrap_or(0))
            ^ usize::from(b.get(index).copied().unwrap_or(0));
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_BEARER: &str = "test-bearer-token-32-bytes-long!";

    #[test]
    fn policy_maps_credentials_to_exact_scopes_and_rejects_bad_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("principals.json");
        std::fs::write(
            &path,
            format!(r#"[{{"bearer_token":"{TEST_BEARER}","org_id":"o","user_id":"u","project_ids":["p"],"team_ids":["t"],"max_visibility":2,"audit_admin":true}}]"#),
        )
        .unwrap();
        let registry = PrincipalRegistry::load(&path).unwrap();
        let principal = registry.authenticate(TEST_BEARER.as_bytes()).unwrap();
        assert_eq!(principal.visibility.org_id.as_str(), "o");
        assert_eq!(principal.visibility.user_id.as_str(), "u");
        assert_eq!(principal.visibility.project_ids[0].as_str(), "p");
        assert_eq!(principal.visibility.team_ids[0].as_str(), "t");
        assert_eq!(principal.visibility.max_visibility, Visibility::Team);
        assert!(principal.audit_admin);
        assert!(registry.authenticate(b"wrong").is_none());
        assert!(registry.ensure_org("o").is_ok());
        assert!(registry.ensure_org("another-org").is_err());

        std::fs::write(&path, "[]").unwrap();
        assert!(PrincipalRegistry::load(&path).is_err());
        std::fs::write(
            &path,
            r#"[{"bearer_token":"","org_id":"o","user_id":"u","max_visibility":3}]"#,
        )
        .unwrap();
        assert!(PrincipalRegistry::load(&path).is_err());
    }

    #[test]
    fn production_and_embedded_registries_reject_short_bearers() {
        let visibility = VisibilityContext {
            user_id: "user".into(),
            org_id: "org".into(),
            project_ids: vec![].into(),
            team_ids: vec![].into(),
            max_visibility: Visibility::Org,
        };
        assert!(PrincipalRegistry::single("short".into(), visibility).is_err());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("principals.json");
        std::fs::write(
            &path,
            r#"[{"bearer_token":"short","org_id":"org","user_id":"user","max_visibility":3}]"#,
        )
        .unwrap();
        let error = PrincipalRegistry::load(&path).err().unwrap().to_string();
        assert!(error.contains("at least 32 bytes"));
    }
}
