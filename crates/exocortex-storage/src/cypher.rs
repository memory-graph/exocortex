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
            assert!(
                m.insert($t.id, $t).is_none(),
                "duplicate Cypher template id"
            );
        };
    }

    reg!(Template {
        id: "upsert_memory",
        read_only: false,
        required_params: &[
            "id",
            "memory_type_label",
            "memory_type_id",
            "props_json",
            "tags",
            "entity_ids",
            "attribute_keys",
            "tenant_id",
            "user_id",
            "project_id",
            "team_id",
            "visibility",
            "valid_from",
            "valid_until",
            "invalidated_by",
            "recorded_at",
            "lsn"
        ],
        cypher: r#"
            MERGE (m:Memory {id: $id})
            WITH m
            OPTIONAL MATCH (prior:_MemoryAssertion {id: $id})
            WHERE prior.lsn = m.lsn
            WITH m, count(prior) = 0 AND m.lsn IS NOT NULL AS preserve_outgoing
            FOREACH (_ IN CASE WHEN preserve_outgoing THEN [1] ELSE [] END |
                CREATE (legacy:_MemoryAssertion {id: m.id,
                    memory_type_label: m.memory_type_label, memory_type_id: m.memory_type_id,
                    visibility: m.visibility, valid_from: m.valid_from,
                    valid_until: m.valid_until, recorded_at: m.recorded_at,
                    invalidated_by: m.invalidated_by, props_json: m.props_json,
                    tags: m.tags, entity_ids: m.entity_ids, tenant_id: m.tenant_id,
                    user_id: m.user_id, project_id: m.project_id, team_id: m.team_id,
                    lsn: m.lsn}))
            SET m.memory_type_label = $memory_type_label,
                m.memory_type_id    = $memory_type_id,
                m.visibility        = $visibility,
                m.valid_from        = $valid_from,
                m.valid_until       = $valid_until,
                m.recorded_at       = $recorded_at,
                m.invalidated_by    = $invalidated_by,
                m.props_json        = $props_json,
                m.tags              = $tags,
                m.entity_ids        = $entity_ids,
                m.tenant_id         = $tenant_id,
                m.user_id           = $user_id,
                m.project_id        = $project_id,
                m.team_id           = $team_id,
                m.lsn               = $lsn
            CREATE (h:_MemoryAssertion {id: $id, memory_type_label: $memory_type_label,
                memory_type_id: $memory_type_id, visibility: $visibility,
                valid_from: $valid_from, valid_until: $valid_until,
                recorded_at: $recorded_at, invalidated_by: $invalidated_by,
                props_json: $props_json, tags: $tags, entity_ids: $entity_ids,
                tenant_id: $tenant_id, user_id: $user_id, project_id: $project_id,
                team_id: $team_id, lsn: $lsn})
            WITH m
            OPTIONAL MATCH (:_MemoryAttribute)-[old:_INDEXES_MEMORY]->(m)
            DELETE old
            WITH m
            FOREACH (key IN $attribute_keys |
                MERGE (attribute:_MemoryAttribute {key: key})
                MERGE (attribute)-[:_INDEXES_MEMORY]->(m))
            RETURN id(m) AS node_id, m.lsn AS lsn
        "#,
    });

    // Batch mutations are composed into one GRAPH.QUERY by the Falkor
    // adapter. They intentionally omit RETURN so a WITH boundary can join
    // every row mutation into the query engine's single atomic unit.
    reg!(Template {
        id: "batch_promote_visibility_guard",
        read_only: false,
        required_params: &["id", "visibility"],
        cypher: r#"
            MATCH (m:Memory {id: $id})
            WHERE m.visibility <= $visibility
        "#,
    });

    reg!(Template {
        id: "batch_upsert_memory",
        read_only: false,
        required_params: &[
            "id",
            "memory_type_label",
            "memory_type_id",
            "props_json",
            "tags",
            "entity_ids",
            "attribute_keys",
            "tenant_id",
            "user_id",
            "project_id",
            "team_id",
            "visibility",
            "valid_from",
            "valid_until",
            "invalidated_by",
            "recorded_at",
            "lsn"
        ],
        cypher: r#"
            MERGE (m:Memory {id: $id})
            WITH m
            OPTIONAL MATCH (prior:_MemoryAssertion {id: $id})
            WHERE prior.lsn = m.lsn
            WITH m, count(prior) = 0 AND m.lsn IS NOT NULL AS preserve_outgoing
            FOREACH (_ IN CASE WHEN preserve_outgoing THEN [1] ELSE [] END |
                CREATE (legacy:_MemoryAssertion {id: m.id,
                    memory_type_label: m.memory_type_label, memory_type_id: m.memory_type_id,
                    visibility: m.visibility, valid_from: m.valid_from,
                    valid_until: m.valid_until, recorded_at: m.recorded_at,
                    invalidated_by: m.invalidated_by, props_json: m.props_json,
                    tags: m.tags, entity_ids: m.entity_ids, tenant_id: m.tenant_id,
                    user_id: m.user_id, project_id: m.project_id, team_id: m.team_id,
                    lsn: m.lsn}))
            SET m.memory_type_label = $memory_type_label,
                m.memory_type_id    = $memory_type_id,
                m.visibility        = $visibility,
                m.valid_from        = $valid_from,
                m.valid_until       = $valid_until,
                m.recorded_at       = $recorded_at,
                m.invalidated_by    = $invalidated_by,
                m.props_json        = $props_json,
                m.tags              = $tags,
                m.entity_ids        = $entity_ids,
                m.tenant_id         = $tenant_id,
                m.user_id           = $user_id,
                m.project_id        = $project_id,
                m.team_id           = $team_id,
                m.lsn               = $lsn
            CREATE (h:_MemoryAssertion {id: $id, memory_type_label: $memory_type_label,
                memory_type_id: $memory_type_id, visibility: $visibility,
                valid_from: $valid_from, valid_until: $valid_until,
                recorded_at: $recorded_at, invalidated_by: $invalidated_by,
                props_json: $props_json, tags: $tags, entity_ids: $entity_ids,
                tenant_id: $tenant_id, user_id: $user_id, project_id: $project_id,
                team_id: $team_id, lsn: $lsn})
            WITH m
            OPTIONAL MATCH (:_MemoryAttribute)-[old:_INDEXES_MEMORY]->(m)
            DELETE old
            WITH m
            FOREACH (key IN $attribute_keys |
                MERGE (attribute:_MemoryAttribute {key: key})
                MERGE (attribute)-[:_INDEXES_MEMORY]->(m))
        "#,
    });

    reg!(Template {
        id: "refresh_memory_attribute_index",
        read_only: false,
        required_params: &["id"],
        cypher: r#"
            OPTIONAL MATCH (m:Memory {id: $id})
            OPTIONAL MATCH (:_MemoryAttribute)-[old:_INDEXES_MEMORY]->(m)
            DELETE old
            WITH m
            WITH m,
                [tag IN coalesce(m.tags, []) | 't:' + tag]
                + [entity IN coalesce(m.entity_ids, []) | 'e:' + entity] AS attribute_keys
            FOREACH (key IN attribute_keys |
                MERGE (attribute:_MemoryAttribute {key: key})
                MERGE (attribute)-[:_INDEXES_MEMORY]->(m))
        "#,
    });

    reg!(Template {
        id: "batch_cycle_journal_fragment",
        read_only: false,
        required_params: &[
            "lease_key",
            "cycle_id",
            "lease_epoch",
            "fragment_id",
            "restore_json"
        ],
        cypher: r#"
            OPTIONAL MATCH (existing:_CycleJournal {lease_key: $lease_key})
            WITH existing
            WHERE existing IS NULL OR existing.state = 'Completed'
                  OR existing.cycle_id = $cycle_id
            MERGE (journal:_CycleJournal {lease_key: $lease_key})
            SET journal.cycle_id = $cycle_id,
                journal.lease_epoch = $lease_epoch,
                journal.state = 'Active'
            MERGE (fragment:_CycleJournalFragment {
                lease_key: $lease_key, cycle_id: $cycle_id,
                fragment_id: $fragment_id
            })
            SET fragment.restore_json = $restore_json
        "#,
    });

    reg!(Template {
        id: "active_cycle_journal",
        read_only: true,
        required_params: &["lease_key"],
        cypher: r#"
            MATCH (journal:_CycleJournal {lease_key: $lease_key, state: 'Active'})
            MATCH (fragment:_CycleJournalFragment {
                lease_key: $lease_key, cycle_id: journal.cycle_id
            })
            RETURN journal.cycle_id, journal.lease_epoch, fragment.restore_json
            ORDER BY fragment.fragment_id ASC
        "#,
    });

    reg!(Template {
        id: "cycle_journal_complete_fenced",
        read_only: false,
        required_params: &["lease_key", "cycle_id", "token", "epoch", "now_ms"],
        cypher: r#"
            MATCH (lease:_ExocortexLease {lease_key: $lease_key, token: $token})
            WHERE lease.epoch = $epoch AND lease.expires_at_ms > $now_ms
            MATCH (journal:_CycleJournal {
                lease_key: $lease_key, cycle_id: $cycle_id, state: 'Active'
            })
            SET journal.state = 'Completed', journal.completed_by_epoch = $epoch
            RETURN journal.cycle_id
        "#,
    });

    // Fenced rollback physically removes only rows proven absent from the
    // cycle preimage. OPTIONAL MATCH makes an ambiguous failed write safe to
    // compensate even when the row never reached storage.
    reg!(Template {
        id: "batch_purge_memory",
        read_only: false,
        required_params: &["id"],
        cypher: r#"
            OPTIONAL MATCH (m:Memory {id: $id})
            OPTIONAL MATCH (h:_MemoryAssertion {id: $id})
            DELETE m, h
        "#,
    });

    reg!(Template {
        id: "batch_purge_relationship",
        read_only: false,
        required_params: &["rel_id"],
        cypher: r#"
            OPTIONAL MATCH ()-[r]->()
            WHERE r.id = $rel_id
            OPTIONAL MATCH (h:_RelationshipAssertion {id: $rel_id})
            DELETE r, h
        "#,
    });

    reg!(Template {
        id: "batch_purge_memory_assertions_after",
        read_only: false,
        required_params: &["id", "preimage_lsn"],
        cypher: r#"
            OPTIONAL MATCH (h:_MemoryAssertion {id: $id})
            WHERE h.lsn > $preimage_lsn
            WITH collect(h) AS doomed
            FOREACH (row IN doomed | DELETE row)
        "#,
    });

    reg!(Template {
        id: "batch_purge_relationship_assertions_after",
        read_only: false,
        required_params: &["rel_id", "preimage_lsn"],
        cypher: r#"
            OPTIONAL MATCH (h:_RelationshipAssertion {id: $rel_id})
            WHERE h.lsn > $preimage_lsn
            WITH collect(h) AS doomed
            FOREACH (row IN doomed | DELETE row)
        "#,
    });

    reg!(Template {
        id: "batch_purge_memory_if_current",
        read_only: false,
        required_params: &["id", "owned_lsns"],
        cypher: r#"
            OPTIONAL MATCH (m:Memory {id: $id})
            OPTIONAL MATCH (h:_MemoryAssertion {id: $id})
            WITH m, h ORDER BY h.lsn DESC
            WITH m, m.lsn IN $owned_lsns AS current_owned, collect(h) AS history
            WITH m, current_owned,
                head([row IN history WHERE NOT row.lsn IN $owned_lsns]) AS survivor,
                [row IN history WHERE row.lsn IN $owned_lsns] AS doomed
            FOREACH (row IN doomed | DELETE row)
            FOREACH (_ IN CASE WHEN current_owned AND survivor IS NOT NULL THEN [1] ELSE [] END |
                SET m.memory_type_label = survivor.memory_type_label,
                    m.memory_type_id = survivor.memory_type_id,
                    m.visibility = survivor.visibility, m.valid_from = survivor.valid_from,
                    m.valid_until = survivor.valid_until, m.recorded_at = survivor.recorded_at,
                    m.invalidated_by = survivor.invalidated_by, m.props_json = survivor.props_json,
                    m.tags = survivor.tags, m.entity_ids = survivor.entity_ids,
                    m.tenant_id = survivor.tenant_id, m.user_id = survivor.user_id,
                    m.project_id = survivor.project_id, m.team_id = survivor.team_id,
                    m.lsn = survivor.lsn)
            FOREACH (_ IN CASE WHEN current_owned AND survivor IS NULL THEN [1] ELSE [] END | DELETE m)
        "#,
    });

    reg!(Template {
        id: "batch_purge_relationship_if_current",
        read_only: false,
        required_params: &["rel_id", "kind_label", "owned_lsns"],
        cypher: r#"
            OPTIONAL MATCH ()-[r]->() WHERE r.id = $rel_id
            OPTIONAL MATCH (h:_RelationshipAssertion {id: $rel_id})
            WITH r, h ORDER BY h.lsn DESC
            WITH r, startNode(r) AS a, endNode(r) AS b,
                r.lsn IN $owned_lsns AS current_owned, collect(h) AS history
            WITH r, a, b, current_owned,
                head([row IN history WHERE NOT row.lsn IN $owned_lsns]) AS survivor,
                [row IN history WHERE row.lsn IN $owned_lsns] AS doomed
            FOREACH (row IN doomed | DELETE row)
            FOREACH (_ IN CASE WHEN current_owned THEN [1] ELSE [] END | DELETE r)
            FOREACH (_ IN CASE WHEN current_owned AND survivor IS NOT NULL THEN [1] ELSE [] END |
                CREATE (a)-[:__KIND_TYPE__ {id: $rel_id, kind_label: survivor.kind_label,
                    visibility: survivor.visibility, valid_from: survivor.valid_from,
                    valid_until: survivor.valid_until, recorded_at: survivor.recorded_at,
                    invalidated_by: survivor.invalidated_by, props_json: survivor.props_json,
                    lsn: survivor.lsn}]->(b))
        "#,
    });

    reg!(Template {
        id: "batch_restore_memory_if_current",
        read_only: false,
        required_params: &[
            "id",
            "owned_lsns",
            "preimage_lsn",
            "memory_type_label",
            "memory_type_id",
            "props_json",
            "tags",
            "entity_ids",
            "tenant_id",
            "user_id",
            "project_id",
            "team_id",
            "visibility",
            "valid_from",
            "valid_until",
            "invalidated_by",
            "recorded_at",
            "lsn"
        ],
        cypher: r#"
            OPTIONAL MATCH (m:Memory {id: $id})
            OPTIONAL MATCH (h:_MemoryAssertion {id: $id})
            WITH m, h ORDER BY h.lsn DESC
            WITH m, m.lsn IN $owned_lsns AS current_owned, collect(h) AS history
            WITH m, current_owned,
                head([row IN history WHERE NOT row.lsn IN $owned_lsns]) AS survivor,
                [row IN history WHERE row.lsn IN $owned_lsns] AS doomed
            FOREACH (row IN doomed | DELETE row)
            FOREACH (_ IN CASE WHEN current_owned THEN [1] ELSE [] END |
                SET m.memory_type_label = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.memory_type_label ELSE $memory_type_label END,
                    m.memory_type_id = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.memory_type_id ELSE $memory_type_id END,
                    m.visibility = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.visibility ELSE $visibility END,
                    m.valid_from = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.valid_from ELSE $valid_from END,
                    m.valid_until = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.valid_until ELSE $valid_until END,
                    m.recorded_at = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.recorded_at ELSE $recorded_at END,
                    m.invalidated_by = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.invalidated_by ELSE $invalidated_by END,
                    m.props_json = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.props_json ELSE $props_json END,
                    m.tags = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.tags ELSE $tags END,
                    m.entity_ids = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.entity_ids ELSE $entity_ids END,
                    m.tenant_id = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.tenant_id ELSE $tenant_id END,
                    m.user_id = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.user_id ELSE $user_id END,
                    m.project_id = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.project_id ELSE $project_id END,
                    m.team_id = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.team_id ELSE $team_id END,
                    m.lsn = CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.lsn ELSE $preimage_lsn END)
        "#,
    });

    reg!(Template {
        id: "batch_restore_relationship_if_current",
        read_only: false,
        required_params: &[
            "rel_id",
            "owned_lsns",
            "preimage_lsn",
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
            OPTIONAL MATCH ()-[old]->() WHERE old.id = $rel_id
            OPTIONAL MATCH (h:_RelationshipAssertion {id: $rel_id})
            WITH a, b, old, h ORDER BY h.lsn DESC
            WITH a, b, old, old.lsn IN $owned_lsns AS current_owned, collect(h) AS history
            WITH a, b, old, current_owned,
                head([row IN history WHERE NOT row.lsn IN $owned_lsns]) AS survivor,
                [row IN history WHERE row.lsn IN $owned_lsns] AS doomed
            FOREACH (row IN doomed | DELETE row)
            FOREACH (_ IN CASE WHEN current_owned THEN [1] ELSE [] END |
                DELETE old
                CREATE (a)-[:__KIND_TYPE__ {id: $rel_id,
                    kind_label: CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.kind_label ELSE $kind_label END,
                    visibility: CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.visibility ELSE $visibility END,
                    valid_from: CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.valid_from ELSE $valid_from END,
                    valid_until: CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.valid_until ELSE $valid_until END,
                    recorded_at: CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.recorded_at ELSE $recorded_at END,
                    invalidated_by: CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.invalidated_by ELSE $invalidated_by END,
                    props_json: CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.props_json ELSE $props_json END,
                    lsn: CASE WHEN survivor.lsn > $preimage_lsn THEN survivor.lsn ELSE $preimage_lsn END}]->(b))
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
            OPTIONAL MATCH (prior:_RelationshipAssertion {id: $rel_id})
            WHERE prior.lsn = old.lsn
            WITH a, b, old, count(prior) = 0 AND old IS NOT NULL AS preserve_outgoing
            FOREACH (_ IN CASE WHEN preserve_outgoing THEN [1] ELSE [] END |
                CREATE (legacy:_RelationshipAssertion {id: old.id,
                    from: a.id, to: b.id, kind_label: old.kind_label,
                    visibility: old.visibility, valid_from: old.valid_from,
                    valid_until: old.valid_until, recorded_at: old.recorded_at,
                    invalidated_by: old.invalidated_by, props_json: old.props_json,
                    lsn: old.lsn}))
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
            CREATE (h:_RelationshipAssertion {id: $rel_id, from: $from, to: $to,
                kind_label: $kind_label,
                visibility: $visibility, valid_from: $valid_from,
                valid_until: $valid_until, recorded_at: $recorded_at,
                invalidated_by: $invalidated_by, props_json: $props_json, lsn: $lsn})
            RETURN id(r) AS edge_id
        "#,
    });

    reg!(Template {
        id: "batch_upsert_relationship",
        read_only: false,
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
            OPTIONAL MATCH (prior:_RelationshipAssertion {id: $rel_id})
            WHERE prior.lsn = old.lsn
            WITH a, b, old, count(prior) = 0 AND old IS NOT NULL AS preserve_outgoing
            FOREACH (_ IN CASE WHEN preserve_outgoing THEN [1] ELSE [] END |
                CREATE (legacy:_RelationshipAssertion {id: old.id,
                    from: a.id, to: b.id, kind_label: old.kind_label,
                    visibility: old.visibility, valid_from: old.valid_from,
                    valid_until: old.valid_until, recorded_at: old.recorded_at,
                    invalidated_by: old.invalidated_by, props_json: old.props_json,
                    lsn: old.lsn}))
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
            CREATE (h:_RelationshipAssertion {id: $rel_id, from: $from, to: $to,
                kind_label: $kind_label,
                visibility: $visibility, valid_from: $valid_from,
                valid_until: $valid_until, recorded_at: $recorded_at,
                invalidated_by: $invalidated_by, props_json: $props_json, lsn: $lsn})
        "#,
    });

    // Graph-resident leases are the authoritative Falkor fencing state.
    // Keeping the guard and owner mutation in one GRAPH.QUERY makes the
    // query engine's atomic transaction the R-C3 linearization point.
    reg!(Template {
        id: "governed_import_guard",
        read_only: false,
        required_params: &["import_key", "publication_json", "claim_token", "lease_ms"],
        cypher: r#"
            MERGE (i:_GovernedImport {key: $import_key})
            ON CREATE SET i.applied = false, i.publication_pending = false
            WITH i WHERE i.applied = false
            SET i.applied = true, i.publication_pending = true,
                i.publication_json = $publication_json,
                i.publication_claim_token = $claim_token,
                i.publication_claim_until_ms = timestamp() + $lease_ms
        "#,
    });

    reg!(Template {
        id: "idempotent_batch_publication_claim",
        read_only: false,
        required_params: &["operation_key", "claim_token", "lease_ms"],
        cypher: r#"
            MATCH (i:_GovernedImport {key: $operation_key})
            WHERE i.applied = true AND i.publication_pending = true
              AND (i.publication_claim_until_ms IS NULL OR i.publication_claim_until_ms <= timestamp())
            SET i.publication_claim_token = $claim_token,
                i.publication_claim_until_ms = timestamp() + $lease_ms
            RETURN i.publication_json
        "#,
    });

    reg!(Template {
        id: "idempotent_batch_publication_complete",
        read_only: false,
        required_params: &["operation_key", "claim_token"],
        cypher: r#"
            MATCH (i:_GovernedImport {key: $operation_key})
            WHERE i.applied = true AND i.publication_claim_token = $claim_token
              AND i.publication_claim_until_ms > timestamp()
            SET i.publication_pending = false
            REMOVE i.publication_claim_token, i.publication_claim_until_ms,
                   i.publication_json
            RETURN i.key
        "#,
    });

    reg!(Template {
        id: "idempotent_batch_publication_is_pending",
        read_only: true,
        required_params: &["operation_key"],
        cypher: r#"
            MATCH (i:_GovernedImport {key: $operation_key})
            WHERE i.applied = true AND i.publication_pending = true
            RETURN i.key
        "#,
    });

    reg!(Template {
        id: "idempotent_batch_publication_release",
        read_only: false,
        required_params: &["operation_key", "claim_token"],
        cypher: r#"
            MATCH (i:_GovernedImport {key: $operation_key})
            WHERE i.applied = true AND i.publication_pending = true
              AND i.publication_claim_token = $claim_token
            REMOVE i.publication_claim_token, i.publication_claim_until_ms
            RETURN i.key
        "#,
    });

    reg!(Template {
        id: "idempotent_batch_publication_renew",
        read_only: false,
        required_params: &["operation_key", "claim_token", "lease_ms"],
        cypher: r#"
            MATCH (i:_GovernedImport {key: $operation_key})
            WHERE i.applied = true AND i.publication_pending = true
              AND i.publication_claim_token = $claim_token
              AND i.publication_claim_until_ms > timestamp()
            SET i.publication_claim_until_ms = timestamp() + $lease_ms
            RETURN i.key
        "#,
    });

    reg!(Template {
        id: "lease_acquire",
        read_only: false,
        required_params: &["lease_key", "token", "now_ms", "expires_at_ms"],
        cypher: r#"
            MERGE (l:_ExocortexLease {lease_key: $lease_key})
            ON CREATE SET l.epoch = 0, l.token = '', l.expires_at_ms = 0
            WITH l
            WHERE l.expires_at_ms <= $now_ms
            SET l.epoch = l.epoch + 1,
                l.token = $token,
                l.expires_at_ms = $expires_at_ms
            RETURN l.epoch AS epoch
        "#,
    });

    reg!(Template {
        id: "lease_renew",
        read_only: false,
        required_params: &["lease_key", "token", "epoch", "now_ms", "expires_at_ms"],
        cypher: r#"
            MATCH (l:_ExocortexLease {lease_key: $lease_key})
            WHERE l.token = $token
              AND l.epoch = $epoch
              AND l.expires_at_ms > $now_ms
            SET l.expires_at_ms = $expires_at_ms
            RETURN l.epoch AS epoch
        "#,
    });

    reg!(Template {
        id: "lease_release",
        read_only: false,
        required_params: &["lease_key", "token", "epoch"],
        cypher: r#"
            MATCH (l:_ExocortexLease {lease_key: $lease_key})
            WHERE l.token = $token AND l.epoch = $epoch
            SET l.token = '', l.expires_at_ms = 0
            RETURN l.epoch AS epoch
        "#,
    });

    reg!(Template {
        id: "lease_fence_guard",
        read_only: false,
        required_params: &["lease_key", "token", "epoch", "now_ms"],
        cypher: r#"
            MATCH (l:_ExocortexLease {lease_key: $lease_key})
            WHERE l.token = $token
              AND l.epoch = $epoch
              AND l.expires_at_ms > $now_ms
        "#,
    });

    // Run before any batch mutation. Endpoints supplied by the batch are
    // excluded by the adapter; every remaining id must already exist or the
    // pipeline produces no row and therefore performs no writes.
    reg!(Template {
        id: "batch_endpoint_guard",
        read_only: false,
        required_params: &["external_ids", "external_count"],
        cypher: r#"
            OPTIONAL MATCH (endpoint:Memory)
            WHERE endpoint.id IN $external_ids
            WITH __batch_step, count(endpoint) AS found
            WHERE found = $external_count
        "#,
    });

    reg!(Template {
        id: "ingest_claim_guard",
        read_only: false,
        required_params: &["org_id", "producer_id", "batch_id", "claim_token"],
        cypher: r#"
            MERGE (d:_IngestBatch {
                org_id: $org_id, producer_id: $producer_id, batch_id: $batch_id})
            ON CREATE SET d.claim_token = $claim_token, d.state = 'claiming'
            WITH d
            WHERE d.claim_token = $claim_token AND d.state = 'claiming'
        "#,
    });

    reg!(Template {
        id: "ingest_endpoint_guard",
        read_only: false,
        required_params: &["external_ids", "external_count"],
        cypher: r#"
            OPTIONAL MATCH (endpoint:Memory)
            WHERE endpoint.id IN $external_ids
            WITH __atomic_step, count(endpoint) AS found
            WHERE found = $external_count
        "#,
    });

    reg!(Template {
        id: "ingest_settle",
        read_only: false,
        required_params: &[
            "org_id",
            "producer_id",
            "batch_id",
            "claim_token",
            "accepted",
            "rejected",
            "assigned_lsn",
            "effect_id",
            "effect_json"
        ],
        cypher: r#"
            MATCH (d:_IngestBatch {
                org_id: $org_id, producer_id: $producer_id, batch_id: $batch_id})
            WHERE d.claim_token = $claim_token AND d.state = 'claiming'
            SET d.state = 'settled', d.accepted = $accepted,
                d.rejected = $rejected, d.assigned_lsn = $assigned_lsn,
                d.effect_id = $effect_id, d.effect_json = $effect_json,
                d.effect_acknowledged = CASE WHEN $effect_json IS NULL THEN true ELSE false END
            REMOVE d.claim_token
        "#,
    });

    reg!(Template {
        id: "ingest_effects_pending",
        read_only: true,
        required_params: &["limit"],
        cypher: r#"
            MATCH (d:_IngestBatch)
            WHERE d.state = 'settled' AND d.effect_json IS NOT NULL
              AND d.effect_acknowledged = false
            RETURN d.effect_json ORDER BY d.effect_id ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "ingest_effect_claim",
        read_only: false,
        required_params: &["claim_token", "lease_ms"],
        cypher: r#"
            MATCH (d:_IngestBatch)
            WHERE d.state = 'settled' AND d.effect_json IS NOT NULL
              AND d.effect_acknowledged = false
              AND (d.effect_claim_until_ms IS NULL OR d.effect_claim_until_ms <= timestamp())
            WITH d ORDER BY d.effect_id ASC LIMIT 1
            SET d.effect_claim_token = $claim_token,
                d.effect_claim_until_ms = timestamp() + $lease_ms
            RETURN d.effect_json
        "#,
    });

    reg!(Template {
        id: "ingest_effect_claim_renew",
        read_only: false,
        required_params: &["effect_id", "claim_token", "lease_ms"],
        cypher: r#"
            MATCH (d:_IngestBatch {effect_id: $effect_id})
            WHERE d.state = 'settled' AND d.effect_acknowledged = false
              AND d.effect_claim_token = $claim_token
              AND d.effect_claim_until_ms > timestamp()
            SET d.effect_claim_until_ms = timestamp() + $lease_ms
            RETURN d.effect_id
        "#,
    });

    reg!(Template {
        id: "ingest_effect_acknowledge",
        read_only: false,
        required_params: &["effect_id", "claim_token"],
        cypher: r#"
            MATCH (d:_IngestBatch {effect_id: $effect_id})
            WHERE d.state = 'settled' AND d.effect_json IS NOT NULL
              AND d.effect_claim_token = $claim_token
              AND d.effect_claim_until_ms > timestamp()
            SET d.effect_acknowledged = true
            REMOVE d.effect_claim_token, d.effect_claim_until_ms
            RETURN d.effect_id
        "#,
    });

    reg!(Template {
        id: "ingest_get_settled",
        read_only: true,
        required_params: &["org_id", "producer_id", "batch_id"],
        cypher: r#"
            MATCH (d:_IngestBatch {
                org_id: $org_id, producer_id: $producer_id, batch_id: $batch_id})
            WHERE d.state = 'settled'
            RETURN d.accepted, d.rejected, d.assigned_lsn LIMIT 1
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
        id: "relationships_touching",
        read_only: true,
        required_params: &["frontier", "limit"],
        cypher: r#"
            MATCH (a:Memory)-[r]->(b:Memory)
            WHERE (a.id IN $frontier OR b.id IN $frontier)
              AND r.lsn IS NOT NULL
              AND r.valid_until IS NULL
            RETURN r ORDER BY r.lsn ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "memories_sharing_attributes",
        read_only: true,
        required_params: &["attribute_keys", "limit"],
        cypher: r#"
            UNWIND $attribute_keys AS key
            MATCH (attribute:_MemoryAttribute {key: key})-[:_INDEXES_MEMORY]->(m:Memory)
            RETURN DISTINCT m ORDER BY m.lsn ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "create_memory_attribute_key_index",
        read_only: false,
        required_params: &[],
        cypher: "CREATE INDEX FOR (attribute:_MemoryAttribute) ON (attribute.key)",
    });

    reg!(Template {
        id: "repair_memory_attribute_index_v1",
        read_only: false,
        required_params: &[],
        cypher: r#"
            MERGE (state:_AttributeIndexState {id: 'v1'})
            ON CREATE SET state.ready = false
            WITH state
            WHERE state.ready = false
            MATCH (m:Memory)
            OPTIONAL MATCH (:_MemoryAttribute)-[old:_INDEXES_MEMORY]->(m)
            DELETE old
            WITH state, m,
                [tag IN coalesce(m.tags, []) | 't:' + tag]
                + [entity IN coalesce(m.entity_ids, []) | 'e:' + entity] AS attribute_keys
            FOREACH (key IN attribute_keys |
                MERGE (attribute:_MemoryAttribute {key: key})
                MERGE (attribute)-[:_INDEXES_MEMORY]->(m))
            WITH DISTINCT state
            SET state.ready = true
            RETURN state.ready
        "#,
    });

    reg!(Template {
        id: "get_memories_by_ids",
        read_only: true,
        required_params: &["ids", "max_visibility"],
        cypher: r#"
            MATCH (m:Memory)
            WHERE m.id IN $ids AND m.visibility <= $max_visibility
            RETURN m
        "#,
    });

    reg!(Template {
        id: "get_visible_memories_by_ids",
        read_only: true,
        required_params: &[
            "ids",
            "max_visibility",
            "org_id",
            "user_id",
            "project_ids",
            "team_ids"
        ],
        cypher: r#"
            MATCH (m:Memory)
            WHERE m.id IN $ids
              AND m.visibility <= $max_visibility
              AND m.tenant_id = $org_id
              AND (m.visibility >= 3
                   OR (m.visibility = 0 AND m.user_id = $user_id)
                   OR (m.visibility = 1 AND m.project_id IN $project_ids)
                   OR (m.visibility = 2 AND m.team_id IN $team_ids))
            RETURN m
        "#,
    });

    reg!(Template {
        id: "memories_in_region",
        read_only: true,
        required_params: &["org_id", "project_id", "memory_type", "limit"],
        cypher: r#"
            MATCH (m:Memory)
            WHERE m.memory_type_id = $memory_type
              AND ($org_id = '*' OR m.tenant_id = $org_id)
              AND ($project_id = '*' OR m.project_id = $project_id)
            RETURN m ORDER BY m.id ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "current_relationships_in_region",
        read_only: true,
        required_params: &["org_id", "project_id", "memory_type", "limit"],
        cypher: r#"
            MATCH (a:Memory)-[r]->(b:Memory)
            WHERE r.lsn IS NOT NULL
              AND a.memory_type_id = $memory_type
              AND b.memory_type_id = $memory_type
              AND ($org_id = '*' OR (a.tenant_id = $org_id AND b.tenant_id = $org_id))
              AND ($project_id = '*' OR
                   (a.project_id = $project_id AND b.project_id = $project_id))
            RETURN r
            ORDER BY a.id ASC, b.id ASC, type(r) ASC, r.id ASC
            LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "relationships_in_region",
        read_only: true,
        required_params: &["org_id", "project_id", "memory_type", "limit"],
        cypher: r#"
            MATCH (a:Memory)-[r]->(b:Memory)
            WHERE r.lsn IS NOT NULL
              AND r.valid_until IS NULL AND r.invalidated_by IS NULL
              AND a.memory_type_id = $memory_type
              AND b.memory_type_id = $memory_type
              AND ($org_id = '*' OR (a.tenant_id = $org_id AND b.tenant_id = $org_id))
              AND ($project_id = '*' OR
                   (a.project_id = $project_id AND b.project_id = $project_id))
            RETURN r
            ORDER BY a.id ASC, b.id ASC, type(r) ASC, r.id ASC
            LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "traverse_one_hop_out",
        read_only: true,
        required_params: &[
            "frontier",
            "kind_labels",
            "max_nodes",
            "max_visibility",
            "org_id",
            "user_id",
            "project_ids",
            "team_ids"
        ],
        cypher: r#"
            MATCH (a:Memory)-[r__KIND_TYPES__]->(b:Memory)
            WHERE a.id IN $frontier
              AND a.tenant_id = $org_id AND b.tenant_id = $org_id
              AND a.visibility <= $max_visibility
              AND b.visibility <= $max_visibility
              AND r.visibility <= $max_visibility
              AND (a.visibility >= 3
                   OR (a.visibility = 0 AND a.user_id = $user_id)
                   OR (a.visibility = 1 AND a.project_id IN $project_ids)
                   OR (a.visibility = 2 AND a.team_id IN $team_ids))
              AND (b.visibility >= 3
                   OR (b.visibility = 0 AND b.user_id = $user_id)
                   OR (b.visibility = 1 AND b.project_id IN $project_ids)
                   OR (b.visibility = 2 AND b.team_id IN $team_ids))
              AND (r.visibility >= 3
                   OR (r.visibility = 0 AND
                       ((a.visibility = 0 AND a.user_id = $user_id) OR
                        (b.visibility = 0 AND b.user_id = $user_id)))
                   OR (r.visibility = 1 AND
                       ((a.visibility = 1 AND a.project_id IN $project_ids) OR
                        (b.visibility = 1 AND b.project_id IN $project_ids)))
                   OR (r.visibility = 2 AND
                       ((a.visibility = 2 AND a.team_id IN $team_ids) OR
                        (b.visibility = 2 AND b.team_id IN $team_ids))))
            RETURN DISTINCT b ORDER BY b.id ASC LIMIT $max_nodes
        "#,
    });

    reg!(Template {
        id: "traverse_one_hop_in",
        read_only: true,
        required_params: &[
            "frontier",
            "kind_labels",
            "max_nodes",
            "max_visibility",
            "org_id",
            "user_id",
            "project_ids",
            "team_ids"
        ],
        cypher: r#"
            MATCH (b:Memory)-[r__KIND_TYPES__]->(a:Memory)
            WHERE a.id IN $frontier
              AND a.tenant_id = $org_id AND b.tenant_id = $org_id
              AND a.visibility <= $max_visibility
              AND b.visibility <= $max_visibility
              AND r.visibility <= $max_visibility
              AND (a.visibility >= 3
                   OR (a.visibility = 0 AND a.user_id = $user_id)
                   OR (a.visibility = 1 AND a.project_id IN $project_ids)
                   OR (a.visibility = 2 AND a.team_id IN $team_ids))
              AND (b.visibility >= 3
                   OR (b.visibility = 0 AND b.user_id = $user_id)
                   OR (b.visibility = 1 AND b.project_id IN $project_ids)
                   OR (b.visibility = 2 AND b.team_id IN $team_ids))
              AND (r.visibility >= 3
                   OR (r.visibility = 0 AND
                       ((a.visibility = 0 AND a.user_id = $user_id) OR
                        (b.visibility = 0 AND b.user_id = $user_id)))
                   OR (r.visibility = 1 AND
                       ((a.visibility = 1 AND a.project_id IN $project_ids) OR
                        (b.visibility = 1 AND b.project_id IN $project_ids)))
                   OR (r.visibility = 2 AND
                       ((a.visibility = 2 AND a.team_id IN $team_ids) OR
                        (b.visibility = 2 AND b.team_id IN $team_ids))))
            RETURN b
            ORDER BY b.id ASC LIMIT $max_nodes
        "#,
    });

    reg!(Template {
        id: "valid_at",
        read_only: true,
        required_params: &["id", "at", "max_visibility"],
        cypher: r#"
            MATCH (m:_MemoryAssertion {id: $id})
            WHERE m.recorded_at <= $at AND m.valid_from <= $at
              AND m.visibility <= $max_visibility
            WITH m ORDER BY m.lsn DESC LIMIT 1
            WHERE m.valid_until IS NULL OR m.valid_until > $at
            RETURN m
        "#,
    });

    reg!(Template {
        id: "find_by_entity",
        read_only: true,
        required_params: &[
            "entity_id",
            "limit",
            "max_visibility",
            "org_id",
            "user_id",
            "project_ids",
            "team_ids",
            "memory_types",
            "project_id",
            "has_project",
            "valid_at",
            "has_valid_at"
        ],
        cypher: r#"
            MATCH (m:Memory)
            WHERE $entity_id IN m.entity_ids
              AND m.visibility <= $max_visibility
              AND m.tenant_id = $org_id
              AND (m.visibility >= 3
                   OR (m.visibility = 0 AND m.user_id = $user_id)
                   OR (m.visibility = 1 AND m.project_id IN $project_ids)
                   OR (m.visibility = 2 AND m.team_id IN $team_ids))
              AND (size($memory_types) = 0 OR m.memory_type_id IN $memory_types)
              AND ($has_project = false OR m.project_id = $project_id)
              AND ($has_valid_at = false OR
                   (m.valid_from <= $valid_at AND
                    (m.valid_until IS NULL OR m.valid_until > $valid_at)))
            RETURN m ORDER BY m.recorded_at DESC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "stream_memories",
        read_only: true,
        required_params: &["after_lsn", "first_page", "limit"],
        // ST2 (audit): the row LSN the WHERE filters on is RETURNED so the
        // pager advances the cursor from the same value it selected on —
        // never from the (possibly stale) copy inside props_json.
        cypher: r#"
            MATCH (m:Memory)
            WHERE m.props_json IS NOT NULL
              AND ($first_page = true OR m.lsn > $after_lsn)
            RETURN m, m.lsn AS node_lsn ORDER BY m.lsn ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "stream_relationships",
        read_only: true,
        required_params: &["after_lsn", "first_page", "limit"],
        // Untyped match: edges carry per-kind types (R-T2); the lsn
        // predicate excludes entity/MENTIONS edges, which have no LSN.
        cypher: r#"
            MATCH ()-[r]->()
            WHERE r.props_json IS NOT NULL
              AND ($first_page = true OR r.lsn > $after_lsn)
            RETURN r, r.lsn AS row_lsn ORDER BY r.lsn ASC LIMIT $limit
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

    reg!(Template {
        id: "write_fingerprint_if_schema_compatible",
        read_only: false,
        required_params: &["fp", "max_schema"],
        cypher: r#"
            MERGE (v:_ExocortexMeta {key: 'schema_version'})
            ON CREATE SET v.value = 0
            WITH v WHERE v.value <= toInteger($max_schema)
            MERGE (m:_ExocortexMeta {key: 'ontology_fingerprint'})
            ON CREATE SET m.value = $fp
            WITH m WHERE m.value = $fp
            RETURN m.value
        "#,
    });

    reg!(Template {
        id: "read_schema_version",
        read_only: true,
        required_params: &[],
        cypher: r#"
            MATCH (m:_ExocortexMeta {key: 'schema_version'})
            RETURN m.value AS version LIMIT 1
        "#,
    });

    reg!(Template {
        id: "write_schema_version",
        read_only: false,
        required_params: &["version"],
        cypher: r#"
            MERGE (m:_ExocortexMeta {key: 'schema_version'})
            SET m.value = $version
            RETURN m.value
        "#,
    });

    reg!(Template {
        id: "claim_schema_v0",
        read_only: false,
        required_params: &[],
        cypher: r#"
            MERGE (m:_ExocortexMeta {key: 'schema_version'})
            ON CREATE SET m.value = 0
            WITH m WHERE m.value = 0
            RETURN m.value
        "#,
    });

    reg!(Template {
        id: "finish_schema_migration_v1",
        read_only: false,
        required_params: &["from_version", "to_version"],
        cypher: r#"
            MATCH (m:_ExocortexMeta {key: 'schema_version', value: $from_version})
            SET m.value = $to_version
            RETURN m.value
        "#,
    });

    reg!(Template {
        id: "mutation_lsn_guard",
        read_only: false,
        required_params: &["lsn"],
        cypher: r#"
            MERGE (order:_ExocortexMeta {key: 'committed_lsn'})
            ON CREATE SET order.value = 0
            WITH order WHERE order.value < $lsn
            SET order.value = $lsn
        "#,
    });

    reg!(Template {
        id: "read_committed_lsn",
        read_only: true,
        required_params: &[],
        cypher: r#"
            MATCH (order:_ExocortexMeta {key: 'committed_lsn'})
            RETURN order.value LIMIT 1
        "#,
    });

    reg!(Template {
        id: "migrate_memory_schema_v1",
        read_only: false,
        required_params: &[
            "id",
            "memory_type_label",
            "memory_type_id",
            "props_json",
            "tags",
            "entity_ids",
            "tenant_id",
            "user_id",
            "project_id",
            "team_id",
            "visibility",
            "valid_from",
            "valid_until",
            "invalidated_by",
            "recorded_at",
            "lsn",
            "expected_schema_version"
        ],
        cypher: r#"
            MATCH (schema:_ExocortexMeta {key: 'schema_version', value: $expected_schema_version})
            WITH schema
            MATCH (m:Memory {id: $id})
            WHERE m.lsn = $lsn
            SET m.memory_type_label = $memory_type_label,
                m.memory_type_id = $memory_type_id,
                m.props_json = $props_json,
                m.tags = $tags,
                m.entity_ids = $entity_ids,
                m.tenant_id = $tenant_id,
                m.user_id = $user_id,
                m.project_id = $project_id,
                m.team_id = $team_id,
                m.visibility = $visibility,
                m.valid_from = $valid_from,
                m.valid_until = $valid_until,
                m.invalidated_by = $invalidated_by,
                m.recorded_at = $recorded_at,
                m.lsn = $lsn
            MERGE (h:_MemoryAssertion {id: m.id, lsn: m.lsn})
            ON CREATE SET h.memory_type_label = m.memory_type_label,
                          h.memory_type_id = m.memory_type_id,
                          h.visibility = m.visibility, h.valid_from = m.valid_from,
                          h.valid_until = m.valid_until, h.recorded_at = m.recorded_at,
                          h.invalidated_by = m.invalidated_by, h.props_json = m.props_json,
                          h.tags = m.tags, h.entity_ids = m.entity_ids,
                          h.tenant_id = m.tenant_id, h.user_id = m.user_id,
                          h.project_id = m.project_id, h.team_id = m.team_id
            RETURN m.id
        "#,
    });

    reg!(Template {
        id: "legacy_memory_candidates",
        read_only: true,
        required_params: &["limit"],
        cypher: r#"
            MATCH (m:Memory)
            WHERE m.props_json IS NOT NULL AND m.lsn IS NOT NULL
            OPTIONAL MATCH (current:_MemoryAssertion {id: m.id, lsn: m.lsn})
            WITH m, count(current) AS current_count
            WHERE current_count = 0
            RETURN m ORDER BY m.lsn ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "repair_legacy_memory_v1",
        read_only: false,
        required_params: &[
            "id",
            "memory_type_label",
            "memory_type_id",
            "props_json",
            "tags",
            "entity_ids",
            "tenant_id",
            "user_id",
            "project_id",
            "team_id",
            "visibility",
            "valid_from",
            "valid_until",
            "invalidated_by",
            "recorded_at",
            "lsn",
            "expected_schema_version"
        ],
        cypher: r#"
            MATCH (schema:_ExocortexMeta {key: 'schema_version', value: $expected_schema_version})
            WITH schema
            MATCH (m:Memory {id: $id})
            WHERE m.lsn = $lsn
            OPTIONAL MATCH (current:_MemoryAssertion {id: m.id, lsn: m.lsn})
            WITH m, count(current) AS current_count
            WHERE current_count = 0
            SET m.memory_type_label = $memory_type_label,
                m.memory_type_id = $memory_type_id,
                m.props_json = $props_json, m.tags = $tags,
                m.entity_ids = $entity_ids, m.tenant_id = $tenant_id,
                m.user_id = $user_id, m.project_id = $project_id,
                m.team_id = $team_id, m.visibility = $visibility,
                m.valid_from = $valid_from, m.valid_until = $valid_until,
                m.invalidated_by = $invalidated_by, m.recorded_at = $recorded_at
            CREATE (h:_MemoryAssertion {id: m.id, lsn: m.lsn,
                memory_type_label: m.memory_type_label, memory_type_id: m.memory_type_id,
                visibility: m.visibility, valid_from: m.valid_from,
                valid_until: m.valid_until, recorded_at: m.recorded_at,
                invalidated_by: m.invalidated_by, props_json: m.props_json,
                tags: m.tags, entity_ids: m.entity_ids, tenant_id: m.tenant_id,
                user_id: m.user_id, project_id: m.project_id, team_id: m.team_id})
            RETURN m.id
        "#,
    });

    reg!(Template {
        id: "migrate_relationship_schema_v1",
        read_only: false,
        required_params: &["rel_id", "expected_schema_version"],
        cypher: r#"
            MATCH (schema:_ExocortexMeta {key: 'schema_version', value: $expected_schema_version})
            WITH schema
            MATCH ()-[r]->() WHERE r.id = $rel_id
            MERGE (h:_RelationshipAssertion {id: r.id, lsn: r.lsn})
            ON CREATE SET h.from = startNode(r).id,
                          h.to = endNode(r).id,
                          h.kind_label = r.kind_label,
                          h.visibility = r.visibility,
                          h.valid_from = r.valid_from,
                          h.valid_until = r.valid_until,
                          h.recorded_at = r.recorded_at,
                          h.invalidated_by = r.invalidated_by,
                          h.props_json = r.props_json
            RETURN r.id
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_make_legacy_schema",
        read_only: false,
        required_params: &[],
        cypher: r#"
            MATCH (m:Memory) REMOVE m.entity_ids
            WITH count(m) AS memory_count
            MATCH (mh:_MemoryAssertion) DELETE mh
            WITH memory_count, count(mh) AS memory_history_count
            MATCH (rh:_RelationshipAssertion) DELETE rh
            WITH memory_count, memory_history_count, count(rh) AS relationship_history_count
            MATCH (v:_ExocortexMeta {key: 'schema_version'}) DELETE v
            RETURN memory_count, memory_history_count, relationship_history_count
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_make_future_schema_without_fingerprint",
        read_only: false,
        required_params: &["version"],
        cypher: r#"
            OPTIONAL MATCH (f:_ExocortexMeta {key: 'ontology_fingerprint'})
            DELETE f
            WITH count(f) AS removed
            MERGE (v:_ExocortexMeta {key: 'schema_version'})
            SET v.value = $version
            RETURN removed, v.value
        "#,
    });

    // ---- M2 additions: soft deletes + snapshot counts (§6.5 todo! sites) ----

    reg!(Template {
        id: "soft_delete_memory",
        read_only: false,
        // ST1 (audit): the serialized row (props_json) is rewritten in the
        // same statement — every read path reconstructs the Memory from
        // props_json, so the node properties and the row can never disagree.
        required_params: &["id", "now", "lsn", "props_json"],
        cypher: r#"
            MATCH (m:Memory {id: $id})
            WHERE m.valid_until IS NULL
            SET m.valid_until = $now, m.recorded_at = $now,
                m.lsn = $lsn, m.props_json = $props_json
            CREATE (h:_MemoryAssertion {id: $id, visibility: m.visibility,
                memory_type_label: m.memory_type_label, memory_type_id: m.memory_type_id,
                valid_from: m.valid_from, valid_until: $now, recorded_at: $now,
                invalidated_by: m.invalidated_by, props_json: $props_json,
                tags: m.tags, entity_ids: m.entity_ids, tenant_id: m.tenant_id,
                user_id: m.user_id, project_id: m.project_id, team_id: m.team_id,
                lsn: $lsn})
            RETURN id(m) AS node_id
        "#,
    });

    reg!(Template {
        id: "batch_soft_delete_memory",
        read_only: false,
        required_params: &["id", "now", "lsn", "props_json"],
        cypher: r#"
            MATCH (m:Memory {id: $id})
            SET m.valid_until = $now,
                m.recorded_at = $now,
                m.lsn = $lsn,
                m.props_json = $props_json
            CREATE (h:_MemoryAssertion {id: $id, visibility: m.visibility,
                memory_type_label: m.memory_type_label, memory_type_id: m.memory_type_id,
                valid_from: m.valid_from, valid_until: $now, recorded_at: $now,
                invalidated_by: m.invalidated_by, props_json: $props_json,
                tags: m.tags, entity_ids: m.entity_ids, tenant_id: m.tenant_id,
                user_id: m.user_id, project_id: m.project_id, team_id: m.team_id,
                lsn: $lsn})
        "#,
    });

    reg!(Template {
        id: "soft_delete_relationship",
        read_only: false,
        // ST9 (audit): match by id WITHOUT pinning a relationship type — the
        // adapter creates per-kind types and never `:RELATES`, so the old
        // typed match was a guaranteed no-op reporting success. The edge's
        // props_json is rewritten too (ST1 parity for edges).
        required_params: &["rel_id", "now", "lsn", "props_json"],
        cypher: r#"
            MATCH ()-[r]->() WHERE r.id = $rel_id AND r.valid_until IS NULL
            SET r.valid_until = $now, r.recorded_at = $now,
                r.lsn = $lsn, r.props_json = $props_json
            CREATE (h:_RelationshipAssertion {id: $rel_id,
                from: startNode(r).id, to: endNode(r).id, kind_label: r.kind_label,
                visibility: r.visibility, valid_from: r.valid_from,
                valid_until: $now, recorded_at: $now,
                invalidated_by: r.invalidated_by, props_json: $props_json, lsn: $lsn})
            RETURN id(r) AS edge_id
        "#,
    });

    reg!(Template {
        id: "get_relationship_by_id",
        read_only: true,
        required_params: &["rel_id"],
        cypher: r#"
            MATCH ()-[r]->() WHERE r.id = $rel_id
            RETURN r ORDER BY r.lsn DESC LIMIT 1
        "#,
    });

    reg!(Template {
        id: "count_state_at",
        read_only: true,
        required_params: &["at", "max_visibility"],
        cypher: r#"
            MATCH (m:_MemoryAssertion)
            WHERE m.recorded_at <= $at AND m.valid_from <= $at
              AND m.visibility <= $max_visibility
            WITH m.id AS assertion_id, max(m.lsn) AS assertion_lsn
            MATCH (current:_MemoryAssertion)
            WHERE current.id = assertion_id AND current.lsn = assertion_lsn
              AND current.valid_from <= $at
              AND (current.valid_until IS NULL OR current.valid_until > $at)
            RETURN count(current) AS memories
        "#,
    });

    reg!(Template {
        id: "count_state_at_rels",
        read_only: true,
        required_params: &["at", "max_visibility"],
        cypher: r#"
            MATCH (r:_RelationshipAssertion)
            WHERE r.recorded_at <= $at AND r.valid_from <= $at
              AND r.visibility <= $max_visibility
            WITH r.id AS assertion_id, max(r.lsn) AS assertion_lsn
            MATCH (current:_RelationshipAssertion)
            WHERE current.id = assertion_id AND current.lsn = assertion_lsn
              AND current.valid_from <= $at
              AND (current.valid_until IS NULL OR current.valid_until > $at)
            RETURN count(current) AS relationships
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
        id: "batch_audit_append",
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
        "#,
    });

    reg!(Template {
        id: "discovery_proposal_create",
        read_only: false,
        required_params: &[
            "discovery_id",
            "org_id",
            "region_project",
            "region_memory_type",
            "from",
            "to",
            "kind",
            "visibility",
            "caller_scope_json",
            "issued_at",
            "props_json"
        ],
        cypher: r#"
            MATCH (d:_Discovery {discovery_id: $discovery_id})
            WHERE d.org_id = $org_id
              AND d.region_project = $region_project
              AND d.region_memory_type = $region_memory_type
              AND d.from = $from AND d.to = $to
            MERGE (p:_DiscoveryProposal {discovery_id: $discovery_id})
            ON CREATE SET p.org_id = $org_id,
                          p.region_project = $region_project,
                          p.region_memory_type = $region_memory_type,
                          p.from = $from, p.to = $to, p.kind = $kind,
                          p.visibility = $visibility,
                          p.caller_scope_json = $caller_scope_json,
                          p.issued_at = $issued_at,
                          p.props_json = $props_json
            WITH d, p
            WHERE p.consumed_at IS NULL
              AND p.org_id = $org_id
              AND p.region_project = $region_project
              AND p.region_memory_type = $region_memory_type
              AND p.from = $from AND p.to = $to AND p.kind = $kind
              AND p.visibility = $visibility
              AND p.caller_scope_json = $caller_scope_json
              AND p.issued_at = $issued_at
            DELETE d
            RETURN p.discovery_id AS discovery_id
        "#,
    });

    reg!(Template {
        id: "discovery_record_store",
        read_only: false,
        required_params: &[
            "discovery_id",
            "org_id",
            "region_project",
            "region_memory_type",
            "from",
            "to",
            "discovered_at",
            "props_json",
            "lsn"
        ],
        cypher: r#"
            OPTIONAL MATCH (p:_DiscoveryProposal {discovery_id: $discovery_id})
            WITH collect(p) AS proposals
            FOREACH (_ IN CASE WHEN size(proposals) = 0 THEN [1] ELSE [] END |
                MERGE (d:_Discovery {discovery_id: $discovery_id})
                ON CREATE SET d.org_id = $org_id,
                              d.region_project = $region_project,
                              d.region_memory_type = $region_memory_type,
                              d.from = $from,
                              d.to = $to,
                              d.discovered_at = $discovered_at,
                              d.props_json = $props_json,
                              d.lsn = $lsn,
                              d.published = false)
            WITH proposals
            MATCH (d:_Discovery {discovery_id: $discovery_id})
            WHERE size(proposals) = 0
              AND d.org_id = $org_id
              AND d.region_project = $region_project
              AND d.region_memory_type = $region_memory_type
              AND d.from = $from AND d.to = $to
              AND d.discovered_at = $discovered_at
              AND d.props_json = $props_json
            RETURN d.discovery_id AS discovery_id, d.lsn AS discovery_lsn,
                   coalesce(d.published, false) AS published
        "#,
    });

    reg!(Template {
        id: "batch_discovery_record_store",
        read_only: false,
        required_params: &[
            "discovery_id",
            "org_id",
            "region_project",
            "region_memory_type",
            "from",
            "to",
            "discovered_at",
            "props_json",
            "lsn"
        ],
        cypher: r#"
            OPTIONAL MATCH (p:_DiscoveryProposal {discovery_id: $discovery_id})
            WITH count(p) AS proposal_count
            WHERE proposal_count = 0
            MERGE (d:_Discovery {discovery_id: $discovery_id})
            ON CREATE SET d.org_id = $org_id, d.region_project = $region_project,
                d.region_memory_type = $region_memory_type, d.from = $from, d.to = $to,
                d.discovered_at = $discovered_at, d.props_json = $props_json,
                d.lsn = $lsn, d.published = false
        "#,
    });

    reg!(Template {
        id: "discovery_outbox_mark_published",
        read_only: false,
        required_params: &["discovery_id", "lsn"],
        cypher: r#"
            MATCH (d:_Discovery {discovery_id: $discovery_id, lsn: $lsn})
            SET d.published = true
            RETURN d.discovery_id
        "#,
    });

    reg!(Template {
        id: "discovery_outbox_pending",
        read_only: true,
        required_params: &["limit"],
        cypher: r#"
            MATCH (d:_Discovery)
            WHERE coalesce(d.published, false) = false
            RETURN d.discovery_id, d.props_json, d.lsn
            ORDER BY d.lsn ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "discovery_record_get",
        read_only: true,
        required_params: &["discovery_id"],
        cypher: r#"
            MATCH (d:_Discovery {discovery_id: $discovery_id})
            RETURN d.props_json AS props_json LIMIT 1
        "#,
    });

    reg!(Template {
        id: "discovery_record_state",
        read_only: true,
        required_params: &["discovery_id"],
        cypher: r#"
            MATCH (d:_Discovery {discovery_id: $discovery_id})
            RETURN d.props_json, d.lsn, coalesce(d.published, false) LIMIT 1
        "#,
    });

    reg!(Template {
        id: "discovery_record_list",
        read_only: true,
        required_params: &["org_id", "limit"],
        cypher: r#"
            MATCH (d:_Discovery {org_id: $org_id})
            RETURN d.props_json AS props_json
            ORDER BY d.discovered_at DESC, d.discovery_id ASC LIMIT $limit
        "#,
    });

    reg!(Template {
        id: "integration_corrupt_discovery_record",
        read_only: false,
        required_params: &["discovery_id", "props_json"],
        cypher: r#"
            MATCH (d:_Discovery {discovery_id: $discovery_id})
            SET d.props_json = $props_json
            RETURN d.discovery_id AS discovery_id
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_corrupt_discovery_proposal",
        read_only: false,
        required_params: &["discovery_id", "props_json"],
        cypher: r#"
            MATCH (p:_DiscoveryProposal {discovery_id: $discovery_id})
            SET p.props_json = $props_json
            RETURN p.discovery_id
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_remove_current_memory_assertion",
        read_only: false,
        required_params: &["id"],
        cypher: r#"
            MATCH (m:Memory {id: $id})
            MATCH (h:_MemoryAssertion {id: $id}) WHERE h.lsn = m.lsn
            DELETE h
            RETURN m.lsn
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_remove_current_relationship_assertion",
        read_only: false,
        required_params: &["rel_id"],
        cypher: r#"
            MATCH ()-[r]->() WHERE r.id = $rel_id
            MATCH (h:_RelationshipAssertion {id: $rel_id}) WHERE h.lsn = r.lsn
            DELETE h
            RETURN r.lsn
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_corrupt_memory_stream_lsn",
        read_only: false,
        required_params: &["id", "lsn"],
        cypher: r#"
            MATCH (m:Memory {id: $id}) SET m.lsn = $lsn RETURN m.id
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_corrupt_relationship_stream_lsn",
        read_only: false,
        required_params: &["rel_id", "lsn"],
        cypher: r#"
            MATCH ()-[r]->() WHERE r.id = $rel_id
            SET r.lsn = $lsn RETURN r.id
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_memory_assertion_count",
        read_only: true,
        required_params: &["id"],
        cypher: r#"
            MATCH (h:_MemoryAssertion {id: $id})
            RETURN count(h)
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_relationship_assertion_count",
        read_only: true,
        required_params: &["id"],
        cypher: r#"
            MATCH (h:_RelationshipAssertion {id: $id})
            RETURN count(h)
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_idempotent_publication_payload_count",
        read_only: true,
        required_params: &["operation_key"],
        cypher: r#"
            MATCH (i:_GovernedImport {key: $operation_key})
            WHERE i.publication_json IS NOT NULL
            RETURN count(i)
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_expire_idempotent_publication_claim",
        read_only: false,
        required_params: &["operation_key"],
        cypher: r#"
            MATCH (i:_GovernedImport {key: $operation_key})
            WHERE i.publication_pending = true
            SET i.publication_claim_until_ms = 0
            RETURN i.key
        "#,
    });

    #[cfg(feature = "integration")]
    reg!(Template {
        id: "integration_current_memory_assertion_temporal_fields",
        read_only: true,
        required_params: &["id"],
        cypher: r#"
            MATCH (m:Memory {id: $id})
            MATCH (h:_MemoryAssertion {id: $id}) WHERE h.lsn = m.lsn
            RETURN h.invalidated_by, h.valid_until, h.recorded_at
        "#,
    });

    reg!(Template {
        id: "integration_corrupt_ingest_settlement",
        read_only: false,
        required_params: &[
            "org_id",
            "producer_id",
            "batch_id",
            "accepted",
            "rejected",
            "assigned_lsn"
        ],
        cypher: r#"
            MATCH (d:_IngestBatch {
                org_id: $org_id, producer_id: $producer_id, batch_id: $batch_id})
            SET d.accepted = $accepted, d.rejected = $rejected,
                d.assigned_lsn = $assigned_lsn
            RETURN d.batch_id
        "#,
    });

    reg!(Template {
        id: "integration_get_discovery_lsn",
        read_only: true,
        required_params: &["discovery_id"],
        cypher: r#"
            MATCH (d:_Discovery {discovery_id: $discovery_id})
            RETURN d.lsn LIMIT 1
        "#,
    });

    reg!(Template {
        id: "discovery_proposal_get",
        read_only: true,
        required_params: &["discovery_id"],
        cypher: r#"
            MATCH (p:_DiscoveryProposal {discovery_id: $discovery_id})
            WHERE p.consumed_at IS NULL
            RETURN p.props_json AS props_json LIMIT 1
        "#,
    });

    reg!(Template {
        id: "discovery_accept_guard",
        read_only: false,
        required_params: &[
            "discovery_id",
            "org_id",
            "region_project",
            "region_memory_type",
            "from",
            "to",
            "kind",
            "visibility",
            "caller_scope_json",
            "proposal_json"
        ],
        cypher: r#"
            MATCH (p:_DiscoveryProposal {discovery_id: $discovery_id}),
                  (proposal_from:Memory {id: $from}),
                  (proposal_to:Memory {id: $to})
            WHERE p.consumed_at IS NULL
              AND p.org_id = $org_id
              AND p.region_project = $region_project
              AND p.region_memory_type = $region_memory_type
              AND p.from = $from AND p.to = $to AND p.kind = $kind
              AND p.visibility = $visibility
              AND p.caller_scope_json = $caller_scope_json
              AND p.props_json = $proposal_json
        "#,
    });

    reg!(Template {
        id: "discovery_proposal_consume",
        read_only: false,
        required_params: &["discovery_id", "consumed_at"],
        cypher: r#"
            MATCH (p:_DiscoveryProposal {discovery_id: $discovery_id})
            WHERE p.consumed_at IS NULL
            SET p.consumed_at = $consumed_at
        "#,
    });

    reg!(Template {
        id: "audit_range",
        read_only: true,
        required_params: &["org_id", "since_lsn", "limit"],
        cypher: r#"
            MATCH (a:_AuditRecord) WHERE a.lsn > $since_lsn AND a.org_id = $org_id
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
