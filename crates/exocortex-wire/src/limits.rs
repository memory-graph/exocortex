//! Shared, deterministic resource ceilings for MCP and ingestion boundaries.

use crate::ingest::v1::{MemoryDraft, RelationshipDraft};

/// Maximum newline-framed MCP request size before JSON decoding.
pub const MAX_MCP_REQUEST_BYTES: usize = 1024 * 1024;
/// Maximum UTF-8 content bytes in one memory.
pub const MAX_MEMORY_CONTENT_BYTES: usize = 64 * 1024;
/// Maximum tags on one memory.
pub const MAX_TAGS_PER_MEMORY: usize = 32;
/// Maximum UTF-8 bytes in one tag.
pub const MAX_TAG_BYTES: usize = 128;
/// Maximum aggregate tag bytes on one memory.
pub const MAX_TAG_BYTES_PER_MEMORY: usize = 2048;
/// Maximum relationships in one request/batch.
pub const MAX_EDGES_PER_BATCH: usize = 64;

/// Validate one memory's caller-controlled variable-width fields.
pub fn validate_memory_fields(content: &str, tags: &[String]) -> Result<(), &'static str> {
    if content.len() > MAX_MEMORY_CONTENT_BYTES {
        return Err("memory content exceeds 65536 UTF-8 bytes");
    }
    if tags.len() > MAX_TAGS_PER_MEMORY {
        return Err("memory has more than 32 tags");
    }
    if tags.iter().any(|tag| tag.len() > MAX_TAG_BYTES) {
        return Err("tag exceeds 128 UTF-8 bytes");
    }
    if tags.iter().map(String::len).sum::<usize>() > MAX_TAG_BYTES_PER_MEMORY {
        return Err("aggregate tag bytes exceed 2048");
    }
    Ok(())
}

/// Validate a decoded wire batch before hashing, cloning, or storage work.
pub fn validate_batch_resources(
    memories: &[MemoryDraft],
    edges: &[RelationshipDraft],
) -> Result<(), &'static str> {
    if edges.len() > MAX_EDGES_PER_BATCH {
        return Err("batch has more than 64 relationships");
    }
    for memory in memories {
        validate_memory_fields(&memory.content, &memory.tags)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_limits_cover_each_boundary_minus_one_exact_and_plus_one() {
        assert!(validate_memory_fields(&"x".repeat(MAX_MEMORY_CONTENT_BYTES - 1), &[]).is_ok());
        assert!(validate_memory_fields(&"x".repeat(MAX_MEMORY_CONTENT_BYTES), &[]).is_ok());
        assert!(validate_memory_fields(&"x".repeat(MAX_MEMORY_CONTENT_BYTES + 1), &[]).is_err());
        assert!(validate_memory_fields("x", &vec!["t".into(); MAX_TAGS_PER_MEMORY - 1]).is_ok());
        assert!(validate_memory_fields("x", &vec!["t".into(); MAX_TAGS_PER_MEMORY]).is_ok());
        assert!(validate_memory_fields("x", &vec!["t".into(); MAX_TAGS_PER_MEMORY + 1]).is_err());
        assert!(validate_memory_fields("x", &["t".repeat(MAX_TAG_BYTES - 1)]).is_ok());
        assert!(validate_memory_fields("x", &["t".repeat(MAX_TAG_BYTES)]).is_ok());
        assert!(validate_memory_fields("x", &["t".repeat(MAX_TAG_BYTES + 1)]).is_err());

        let aggregate_minus_one = vec!["t".repeat(MAX_TAG_BYTES); 15]
            .into_iter()
            .chain(["t".repeat(MAX_TAG_BYTES - 1)])
            .collect::<Vec<_>>();
        let aggregate_exact = vec!["t".repeat(MAX_TAG_BYTES); 16];
        let aggregate_plus_one = aggregate_exact
            .iter()
            .cloned()
            .chain(["t".into()])
            .collect::<Vec<_>>();
        assert!(validate_memory_fields("x", &aggregate_minus_one).is_ok());
        assert!(validate_memory_fields("x", &aggregate_exact).is_ok());
        assert!(validate_memory_fields("x", &aggregate_plus_one).is_err());

        let memories = [MemoryDraft::default()];
        assert!(validate_batch_resources(
            &memories,
            &vec![RelationshipDraft::default(); MAX_EDGES_PER_BATCH - 1]
        )
        .is_ok());
        assert!(validate_batch_resources(
            &memories,
            &vec![RelationshipDraft::default(); MAX_EDGES_PER_BATCH]
        )
        .is_ok());
        assert!(validate_batch_resources(
            &memories,
            &vec![RelationshipDraft::default(); MAX_EDGES_PER_BATCH + 1]
        )
        .is_err());
    }
}
