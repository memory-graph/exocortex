//! Administrator-owned bearer credential to request-principal mapping.

use std::collections::HashSet;
use std::path::Path;

use exocortex_kernel::Visibility;
use exocortex_storage::VisibilityContext;
use serde::Deserialize;

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
}

/// Immutable credential registry installed at process startup.
#[derive(Clone)]
pub struct PrincipalRegistry {
    entries: Vec<(Vec<u8>, VisibilityContext)>,
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
        anyhow::ensure!(!token.is_empty(), "bearer token must be non-empty");
        Ok(Self {
            entries: vec![(token.into_bytes(), principal)],
        })
    }

    fn from_rows(rows: Vec<PrincipalPolicyRow>) -> anyhow::Result<Self> {
        anyhow::ensure!(!rows.is_empty(), "principal policy must not be empty");
        let mut tokens = HashSet::new();
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            anyhow::ensure!(
                !row.bearer_token.is_empty() && !row.org_id.is_empty() && !row.user_id.is_empty(),
                "principal token, org_id, and user_id must be non-empty"
            );
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
                VisibilityContext {
                    user_id: row.user_id.into(),
                    org_id: row.org_id.into(),
                    project_ids: row.project_ids.into_iter().map(Into::into).collect(),
                    team_ids: row.team_ids.into_iter().map(Into::into).collect(),
                    max_visibility,
                },
            ));
        }
        Ok(Self { entries })
    }

    /// Authenticate a bearer token with a length-hiding full-registry scan.
    pub fn authenticate(&self, token: &[u8]) -> Option<VisibilityContext> {
        self.entries
            .iter()
            .find(|(candidate, _)| constant_time_eq(token, candidate))
            .map(|(_, principal)| principal.clone())
    }
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

    #[test]
    fn policy_maps_credentials_to_exact_scopes_and_rejects_bad_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("principals.json");
        std::fs::write(
            &path,
            r#"[{"bearer_token":"secret","org_id":"o","user_id":"u","project_ids":["p"],"team_ids":["t"],"max_visibility":2}]"#,
        )
        .unwrap();
        let registry = PrincipalRegistry::load(&path).unwrap();
        let principal = registry.authenticate(b"secret").unwrap();
        assert_eq!(principal.org_id.as_str(), "o");
        assert_eq!(principal.user_id.as_str(), "u");
        assert_eq!(principal.project_ids[0].as_str(), "p");
        assert_eq!(principal.team_ids[0].as_str(), "t");
        assert_eq!(principal.max_visibility, Visibility::Team);
        assert!(registry.authenticate(b"wrong").is_none());

        std::fs::write(&path, "[]").unwrap();
        assert!(PrincipalRegistry::load(&path).is_err());
        std::fs::write(
            &path,
            r#"[{"bearer_token":"","org_id":"o","user_id":"u","max_visibility":3}]"#,
        )
        .unwrap();
        assert!(PrincipalRegistry::load(&path).is_err());
    }
}
