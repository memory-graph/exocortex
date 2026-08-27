use exocortex_kernel::{Embedding, EmbeddingModel};

#[test]
fn embedding_vector_and_model_revision_round_trip_together() {
    let embedding = Embedding {
        model: EmbeddingModel {
            name: "bge-small".into(),
            version: "v1".into(),
        },
        vector: vec![0.25, -0.5, 0.75],
    };

    let encoded = serde_json::to_vec(&embedding).expect("serialize stamped embedding");
    let decoded: Embedding =
        serde_json::from_slice(&encoded).expect("deserialize stamped embedding");
    assert_eq!(decoded, embedding);
    assert!(String::from_utf8(encoded)
        .expect("JSON is UTF-8")
        .contains("\"version\":\"v1\""));
}

#[test]
fn legacy_v1_vector_migrates_to_the_known_production_model_stamp() {
    let decoded: Embedding =
        serde_json::from_str("[0.25,-0.5,0.75]").expect("read pre-stamp v1 vector");
    assert_eq!(decoded.model.name, "bge-small");
    assert_eq!(decoded.model.version, "v1");
    assert_eq!(decoded.vector, vec![0.25, -0.5, 0.75]);
    assert!(serde_json::to_string(&decoded)
        .expect("serialize migrated embedding")
        .contains("\"model\""));
}
