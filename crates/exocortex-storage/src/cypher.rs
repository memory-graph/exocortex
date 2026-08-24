// crates/exocortex-storage/src/cypher.rs
//! Every Cypher template the FalkorDB adapter can execute. Registered at
//! compile time; the trait method `query_cypher` refuses templates not listed
//! here. This keeps Cypher confined to one file (CR-10).

use once_cell::sync::Lazy;
use std::collections::HashMap;

/// A registered, parameterized Cypher template.
pub struct Template {
    /// Stable template id used by `CypherQuery::template_id`.
    pub id: &'static str,
    /// Whether the template only reads.
    pub read_only: bool,
    /// The Cypher text. Parameters use `$name` placeholders (R-S2).
    pub cypher: &'static str,
    /// Every param that must be present on the query.
    pub required_params: &'static [&'static str],
}

/// The compile-time template catalogue. Additions are allowed (soft-delete,
/// state-at, and audit templates joined at M2/M7); removals are not.
pub static TEMPLATES: Lazy<HashMap<&'static str, Template>> = Lazy::new(|| {
    let mut m = HashMap::new();
    macro_rules! reg {
        ($t:expr) => {
            m.insert($t.id, $t);
        };
    }

    reg!(Template {
        id: "upsert_memory",
        read_only: false,
        required_params: &[
            "id",
            "memory_type_label",
            "props_json",
            "visibility",
            "valid_from",
            "valid_until",
            "invalidated_by",
            "recorded_at",
            "lsn"
        ],
        cypher: r#"
            MERGE (m:Memory {id: $id})
            SET m.memory_type_label = $memory_type_label,
                m.visibility        = $visibility,
                m.valid_from        = $valid_from,
                m.valid_until       = $valid_until,
                m.recorded_at       = $recorded_at,
                m.invalidated_by    = $invalidated_by,
                m.props_json        = $props_json,
                m.lsn               = $lsn
            RETURN id(m) AS node_id, m.lsn AS lsn
        "#,
    });

    reg!(Template {
        id: "upsert_relationship",
        read_only: false,
        // Note: no MERGE on the relationship (R-S2). We DELETE-then-CREATE so
        // each write is a new relationship row in FalkorDB, giving us stable
        // bi-temporal history.
        //
        // M2 amendment (recorded): the §6.4 text used a generic `:RELATES`
        // type plus a `kind_label` property. R-T2 (§2.6.1) makes the kind's
        // display name the stable Cypher label, and the pinned FalkorDB
        // server cannot evaluate predicates over var-length edge lists
        // ("mismatch: expected List or Null but was Edge"), so kind filtering
        // rides the relationship TYPE via the `__KIND_TYPE__` placeholder
        // (substituted from the validated ontology allowlist, §6.5).
        required_params: &[
            "rel_id",
            "from",
            "to",
            "kind_label",
            "props_json",
            "visibility",
            "valid_from",
            "valid_until",
            "invalidated_by",
            "recorded_at",
            "lsn"
        ],
        cypher: r#"
            MATCH (a:Memory {id: $from}), (b:Memory {id: $to})
            OPTIONAL MATCH (a)-[old]->(b) WHERE old.id = $rel_id
            DELETE old
            WITH a, b
            CREATE (a)-[r:__KIND_TYPE__ {id: $rel_id,
                                   kind_label: $kind_label,
                                   visibility: $visibility,
                                   valid_from: $valid_from,
                                   valid_until: $valid_until,
                                   recorded_at: $recorded_at,
                                   invalidated_by: $invalidated_by,
                                   props_json: $props_json,
                                   lsn: $lsn}]->(b)
            RETURN id(r) AS edge_id
        "#,
    });

    reg!(Template {
        id: "get_memory_by_id",
        read_only: true,
        required_params: &["id", "max_visibility"],
        cypher: r#"
            MATCH (m:Memory {id: $id})
            WHERE m.visibility <= $max_visibility
            RETURN m LIMIT 1
        "#,
    });

    reg!(Template {
        id: "traverse_bounded",
        read_only: true,
        required_params: &[
            "from",
            "kind_labels",
            "max_depth",
            "max_nodes",
            "max_visibility"
        ],
        // M2 amendment (recorded): the pinned FalkorDB server cannot evaluate
        // `ALL(r IN rels ...)` predicates over var-length edge lists, so kind
        // filtering rides relationship TYPES via the `__KIND_TYPES__`
        // placeholder (substituted from the validated ontology allowlist;
        // empty kind list = any type). Edge-level visibility is enforced by
        // the cache-layer traversal (§8.4), the interactive read path.
        cypher: r#"
            MATCH (a:Memory {id: $from})
            CALL {
              WITH a
              MATCH (a)-[rels__KIND_TYPES__*1..$max_depth]->(b:Memory)
              WHERE b.visibility <= $max_visibility
              RETURN DISTINCT b LIMIT $max_nodes
            }
            RETURN b
        "#,
    });

    reg!(Template {
        id: "valid_at",
        read_only: true,
        required_params: &["id", "at", "max_visibility"],
        cypher: r#"
            MATCH (m:Memory {id: $id})
            WHERE m.valid_from <= $at
              AND (m.valid_until IS NULL OR m.valid_until > $at)
              AND m.visibility <= $max_visibility
            RETURN m ORDER BY m.recorded_at DESC LIMIT 1
        "#,
    });

    reg!(Template {
        id: "find_by_entity",
        read_only: true,
        required_params: &["entity_id", "limit", "max_visibility"],
        cypher: r#"
            MATCH (m:Memory)-[:MENTIONS]->(e:Entity {id: $entity_id})
            WHERE m.visibility <= $max_visibility
            RETURN m ORDER BY m.recorded_at DESC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "stream_memories",
        read_only: true,
        required_params: &["after_lsn", "limit"],
        cypher: r#"
            MATCH (m:Memory) WHERE m.lsn > $after_lsn
            RETURN m ORDER BY m.lsn ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "stream_relationships",
        read_only: true,
        required_params: &["after_lsn", "limit"],
        // Untyped match: edges carry per-kind types (R-T2); the lsn
        // predicate excludes entity/MENTIONS edges, which have no LSN.
        cypher: r#"
            MATCH ()-[r]->() WHERE r.lsn > $after_lsn
            RETURN r ORDER BY r.lsn ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "read_fingerprint",
        read_only: true,
        required_params: &[],
        cypher: r#"
            MATCH (m:_ExocortexMeta {key: 'ontology_fingerprint'})
            RETURN m.value AS fp LIMIT 1
        "#,
    });

    reg!(Template {
        id: "write_fingerprint",
        read_only: false,
        required_params: &["fp"],
        cypher: r#"
            MERGE (m:_ExocortexMeta {key: 'ontology_fingerprint'})
            SET m.value = $fp
        "#,
    });

    // ---- M2 additions: soft deletes + snapshot counts (§6.5 todo! sites) ----

    reg!(Template {
        id: "soft_delete_memory",
        read_only: false,
        required_params: &["id", "now", "lsn"],
        cypher: r#"
            MATCH (m:Memory {id: $id})
            WHERE m.valid_until IS NULL
            SET m.valid_until = $now, m.lsn = $lsn
            RETURN id(m) AS node_id
        "#,
    });

    reg!(Template {
        id: "soft_delete_relationship",
        read_only: false,
        required_params: &["rel_id", "now", "lsn"],
        cypher: r#"
            MATCH ()-[r:RELATES {id: $rel_id}]->()
            WHERE r.valid_until IS NULL
            SET r.valid_until = $now, r.lsn = $lsn
            RETURN id(r) AS edge_id
        "#,
    });

    reg!(Template {
        id: "count_state_at",
        read_only: true,
        required_params: &["at", "max_visibility"],
        cypher: r#"
            MATCH (m:Memory)
            WHERE m.valid_from <= $at
              AND (m.valid_until IS NULL OR m.valid_until > $at)
              AND m.visibility <= $max_visibility
            RETURN count(m) AS memories
        "#,
    });

    reg!(Template {
        id: "count_state_at_rels",
        read_only: true,
        required_params: &["at", "max_visibility"],
        cypher: r#"
            MATCH ()-[r]->()
            WHERE r.valid_from <= $at
              AND (r.valid_until IS NULL OR r.valid_until > $at)
              AND r.visibility <= $max_visibility
              AND r.lsn IS NOT NULL
            RETURN count(r) AS relationships
        "#,
    });

    reg!(Template {
        id: "audit_append",
        read_only: false,
        required_params: &[
            "action",
            "actor",
            "org_id",
            "input_digest",
            "output_ids",
            "fingerprint",
            "lease_epoch",
            "recorded_at",
            "lsn"
        ],
        cypher: r#"
            CREATE (a:_AuditRecord {
                action: $action, actor: $actor, org_id: $org_id,
                input_digest: $input_digest, output_ids: $output_ids,
                fingerprint: $fingerprint, lease_epoch: $lease_epoch,
                recorded_at: $recorded_at, lsn: $lsn})
            RETURN id(a) AS node_id
        "#,
    });

    reg!(Template {
        id: "audit_range",
        read_only: true,
        required_params: &["since_lsn", "limit"],
        cypher: r#"
            MATCH (a:_AuditRecord) WHERE a.lsn > $since_lsn
            RETURN a ORDER BY a.lsn ASC LIMIT $limit
        "#,
    });

    m
});

/// Validates a `CypherQuery` before it hits the driver:
///   - `template_id` MUST be registered here
///   - every `required_param` MUST be present
///   - if `read_only` on the template is true, `q.read_only` must also be true
pub fn validate(q: &crate::CypherQuery) -> Result<&'static Template, crate::StorageError> {
    let t = TEMPLATES.get(q.template_id).ok_or_else(|| {
        crate::StorageError::Backend(format!("unregistered cypher template: {}", q.template_id))
    })?;
    for p in t.required_params {
        if q.params.get(p).is_none() {
            return Err(crate::StorageError::Backend(format!(
                "template `{}` missing param `{p}`",
                t.id
            )));
        }
    }
    if t.read_only && !q.read_only {
        return Err(crate::StorageError::Backend(format!(
            "template `{}` is read-only",
            t.id
        )));
    }
    Ok(t)
}
