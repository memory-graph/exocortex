//! Cypher template catalogue tests (§6.7 step 2).

use exocortex_storage::CypherQuery;

#[test]
fn validate_rejects_unknown_template() {
    let q = CypherQuery {
        template_id: "definitely_not_registered",
        params: serde_json::json!({}),
        read_only: true,
        deadline: chrono::Utc::now(),
    };
    let err = match exocortex_storage::cypher::validate(&q) {
        Ok(_) => panic!("expected rejection"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("unregistered cypher template"));
}

#[test]
fn validate_rejects_missing_params() {
    let q = CypherQuery {
        template_id: "get_memory_by_id",
        params: serde_json::json!({ "id": "00" }), // max_visibility missing
        read_only: true,
        deadline: chrono::Utc::now(),
    };
    let err = match exocortex_storage::cypher::validate(&q) {
        Ok(_) => panic!("expected rejection"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("missing param `max_visibility`"));
}

#[test]
fn validate_rejects_read_only_template_with_write_intent() {
    let q = CypherQuery {
        template_id: "read_fingerprint",
        params: serde_json::json!({}),
        read_only: false,
        deadline: chrono::Utc::now(),
    };
    let err = match exocortex_storage::cypher::validate(&q) {
        Ok(_) => panic!("expected rejection"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("read-only"));
}

#[test]
fn no_cypher_outside_the_catalogue_module() {
    // CR-10: Cypher lives in exocortex-storage only. The workspace grep gate
    // is enforced by xtask at the workspace level; this test asserts every
    // registered template is non-empty and parameterized (R-S2).
    for t in exocortex_storage::cypher::TEMPLATES.values() {
        assert!(!t.cypher.trim().is_empty(), "{}: empty cypher", t.id);
        if !t.required_params.is_empty() {
            assert!(t.cypher.contains('$'), "{}: no parameters used", t.id);
        }
    }
}
