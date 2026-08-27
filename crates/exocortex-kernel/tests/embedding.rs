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
