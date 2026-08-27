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
            MERGE (m:Memory {id: $id})
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
            CREATE (h:_MemoryAssertion {id: $id, visibility: $visibility,
                valid_from: $valid_from, valid_until: $valid_until,
                recorded_at: $recorded_at, props_json: $props_json, lsn: $lsn})
            RETURN id(m) AS node_id, m.lsn AS lsn
        "#,
    });

    // Batch mutations are composed into one GRAPH.QUERY by the Falkor
    // adapter. They intentionally omit RETURN so a WITH boundary can join
    // every row mutation into the query engine's single atomic unit.
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
            CREATE (h:_MemoryAssertion {id: $id, visibility: $visibility,
                valid_from: $valid_from, valid_until: $valid_until,
                recorded_at: $recorded_at, props_json: $props_json, lsn: $lsn})
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
            CREATE (h:_RelationshipAssertion {id: $rel_id,
                visibility: $visibility, valid_from: $valid_from,
                valid_until: $valid_until, recorded_at: $recorded_at,
                props_json: $props_json, lsn: $lsn})
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
            CREATE (h:_RelationshipAssertion {id: $rel_id,
                visibility: $visibility, valid_from: $valid_from,
                valid_until: $valid_until, recorded_at: $recorded_at,
                props_json: $props_json, lsn: $lsn})
        "#,
    });

    // Graph-resident leases are the authoritative Falkor fencing state.
    // Keeping the guard and owner mutation in one GRAPH.QUERY makes the
    // query engine's atomic transaction the R-C3 linearization point.
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
            "assigned_lsn"
        ],
        cypher: r#"
            MATCH (d:_IngestBatch {
                org_id: $org_id, producer_id: $producer_id, batch_id: $batch_id})
            WHERE d.claim_token = $claim_token AND d.state = 'claiming'
            SET d.state = 'settled', d.accepted = $accepted,
                d.rejected = $rejected, d.assigned_lsn = $assigned_lsn
            REMOVE d.claim_token
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
        id: "get_relationship_by_id",
        read_only: true,
        required_params: &["id"],
        cypher: r#"
            MATCH ()-[r]->() WHERE r.id = $id
            RETURN r LIMIT 1
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
        required_params: &["tags", "entity_ids", "limit"],
        cypher: r#"
            MATCH (m:Memory)
            WHERE any(tag IN $tags WHERE tag IN m.tags)
               OR any(entity_id IN $entity_ids WHERE entity_id IN m.entity_ids)
            RETURN DISTINCT m ORDER BY m.lsn ASC LIMIT $limit
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
              // The seed anchors the neighborhood; with inverse
              // materialization (R-T4) a round-trip path would otherwise
              // return the seed itself.
              WHERE b.visibility <= $max_visibility AND b.id <> $from
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
              AND (m.tenant_id IS NULL OR m.tenant_id = $org_id)
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
        required_params: &["after_lsn", "limit"],
        // ST2 (audit): the row LSN the WHERE filters on is RETURNED so the
        // pager advances the cursor from the same value it selected on —
        // never from the (possibly stale) copy inside props_json.
        cypher: r#"
            MATCH (m:Memory) WHERE m.lsn > $after_lsn
            RETURN m, m.lsn AS node_lsn ORDER BY m.lsn ASC LIMIT $limit
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
            SET m.valid_until = $now, m.lsn = $lsn, m.props_json = $props_json
            CREATE (h:_MemoryAssertion {id: $id, visibility: m.visibility,
                valid_from: m.valid_from, valid_until: $now,
                recorded_at: $now, props_json: $props_json, lsn: $lsn})
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
                m.lsn = $lsn,
                m.props_json = $props_json
            CREATE (h:_MemoryAssertion {id: $id, visibility: m.visibility,
                valid_from: m.valid_from, valid_until: $now,
                recorded_at: $now, props_json: $props_json, lsn: $lsn})
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
            SET r.valid_until = $now, r.lsn = $lsn, r.props_json = $props_json
            CREATE (h:_RelationshipAssertion {id: $rel_id,
                visibility: r.visibility, valid_from: r.valid_from,
                valid_until: $now, recorded_at: $now,
                props_json: $props_json, lsn: $lsn})
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
            MERGE (p:_DiscoveryProposal {discovery_id: $discovery_id})
            ON CREATE SET p.org_id = $org_id,
                          p.region_project = $region_project,
                          p.region_memory_type = $region_memory_type,
                          p.from = $from, p.to = $to, p.kind = $kind,
                          p.visibility = $visibility,
                          p.caller_scope_json = $caller_scope_json,
                          p.issued_at = $issued_at,
                          p.props_json = $props_json
            WITH p
            WHERE p.org_id = $org_id
              AND p.region_project = $region_project
              AND p.region_memory_type = $region_memory_type
              AND p.from = $from AND p.to = $to AND p.kind = $kind
              AND p.visibility = $visibility
              AND p.caller_scope_json = $caller_scope_json
              AND p.issued_at = $issued_at
            RETURN p.discovery_id AS discovery_id
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
            "caller_scope_json"
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
