//! D9 (§23 #21/#22): the tenancy proof and the concurrent-region proof.
//!
//! #21: an org of 100 users across 3 projects with mixed visibility
//! passes the visibility fuzz with ZERO leaks, and Dreams' cross-domain
//! finder surfaces a cross-user pattern neither user can see alone.
//! #22: three engines ("nodes") concurrently consolidate three distinct
//! regions of ONE org graph, each under its own lease, with per-region
//! MCR² budgets intact.

use std::sync::Arc;

use exocortex_dreams::trigger::DreamsTrigger;
use exocortex_dreams::{DiscoveryKind, DreamsEngine};
use exocortex_kernel::{EntityId, Memory, MemoryContext, MemoryId, Provenance, Visibility, LSN};
use exocortex_storage::{
    memory_visible, InMemoryStorage, LeaseKey, RegionKey, Storage, VisibilityContext,
};

fn ontology() -> Arc<exocortex_kernel::Ontology> {
    Arc::new(
        exocortex_kernel::Ontology::from_packs(vec![exocortex_pack_dev_v1::pack_def()]).unwrap(),
    )
}

/// Deterministic LCG — seeded, no external dependency.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

const USERS: usize = 100;
const PROJECTS: [&str; 3] = ["p0", "p1", "p2"];
const ORG: &str = "o";

fn row(rng: &mut Lcg, index: usize) -> Memory {
    let user = format!("u{}", rng.below(USERS as u64));
    let project = PROJECTS[rng.below(3) as usize];
    let visibility = match rng.below(4) {
        0 => Visibility::Private,
        1 => Visibility::Project,
        2 => Visibility::Team,
        _ => Visibility::Org,
    };
    Memory {
        rights: None,
        id: MemoryId::new_v7(),
        memory_type: 3,
        title: format!("row {index}").into(),
        content: format!("content {index}"),
        summary: None,
        tags: Default::default(),
        visibility,
        provenance: Provenance::Asserted {
            author: user.clone().into(),
            producer_kind: None,
        },
        context: MemoryContext {
            timestamp: chrono::Utc::now(),
            project_id: Some(project.into()),
            project_path: None,
            team_id: Some(format!("t{}", rng.below(4)).into()),
            tenant_id: Some(ORG.into()),
            session_id: None,
            user_id: Some(user.into()),
            created_by: None,
            files_involved: Default::default(),
            languages: Default::default(),
            frameworks: Default::default(),
            technologies: Default::default(),
            git_commit: None,
            git_branch: None,
            working_directory: None,
            entities: Default::default(),
            additional_metadata: serde_json::Value::Null,
        },
        importance: exocortex_kernel::memory::F01::new(0.5).unwrap(),
        confidence: exocortex_kernel::memory::F01::new(0.8).unwrap(),
        effectiveness: None,
        usage_count: 0,
        valid_from: chrono::Utc::now(),
        valid_until: None,
        recorded_at: chrono::Utc::now(),
        invalidated_by: None,
        embedding: Some(exocortex_kernel::Embedding {
            model: exocortex_kernel::EmbeddingModel {
                name: "bge-small".into(),
                version: "v1".into(),
            },
            vector: vec![if index % 64 == 0 { 1.0 } else { 0.0 }; 64],
        }),
        lsn: LSN::new_local(0),
    }
}

/// The caller scope for user `u`: belongs to project `p_i` iff
/// `(user + i) % 2 == 0`, and to team `t{user % 4}` — deterministic,
/// and the same rule the intent check applies.
fn user_vc(user: usize) -> VisibilityContext {
    VisibilityContext {
        user_id: format!("u{user}").into(),
        org_id: ORG.into(),
        project_ids: PROJECTS
            .iter()
            .enumerate()
            .filter(|(i, _)| (user + i) % 2 == 0)
            .map(|(_, p)| (*p).into())
            .collect(),
        team_ids: vec![format!("t{}", user % 4).into()].into(),
        max_visibility: Visibility::Org,
    }
}

/// §23 #21 (first half): zero visibility leaks across the whole tenant
/// matrix — the kernel predicate, the scoped storage read, and the
/// encoded intent agree row for row for every user.
#[tokio::test]
async fn hundred_user_visibility_fuzz_has_zero_leaks() {
    let storage = InMemoryStorage::new(ontology());
    let mut rng = Lcg(0xD9_2026);
    let mut rows = Vec::new();
    for index in 0..600 {
        rows.push(row(&mut rng, index));
    }
    let all_ids: Vec<MemoryId> = rows.iter().map(|r| r.id).collect();
    for r in &rows {
        storage.upsert_memory(r).await.unwrap();
    }

    for user in 0..USERS {
        let vc = user_vc(user);
        // The scoped read returns exactly the rows the predicate grants.
        let visible = storage.get_visible_memories(&all_ids, &vc).await.unwrap();
        let granted: std::collections::HashSet<_> = visible.iter().map(|m| m.id).collect();
        for r in &rows {
            let predicate = memory_visible(r, &vc);
            assert_eq!(
                granted.contains(&r.id),
                predicate,
                "scoped read must agree with the predicate: user {user}, row {}",
                r.title
            );
            // Intent: the visibility label means what §17 says it means.
            let intent = match r.visibility {
                Visibility::Private => r.context.user_id.as_deref() == Some(vc.user_id.as_str()),
                Visibility::Project => r
                    .context
                    .project_id
                    .as_deref()
                    .is_some_and(|p| vc.project_ids.iter().any(|q| q == p)),
                Visibility::Team => r
                    .context
                    .team_id
                    .as_deref()
                    .is_some_and(|t| vc.team_ids.iter().any(|q| q == t)),
                Visibility::Org | Visibility::Public => true,
            };
            assert_eq!(
                predicate, intent,
                "predicate must realize the label's intent: user {user}, {:?} row",
                r.visibility
            );
        }
    }
}

/// §23 #21 (second half): the cross-domain finder surfaces a pattern
/// spanning two users' rows that NEITHER user's scope can read alone.
#[tokio::test]
async fn cross_domain_finder_surfaces_cross_user_patterns() {
    let storage = InMemoryStorage::new(ontology());
    let entity = |name: &str| EntityId::from_parts(ORG, 4, name);
    let mut private_a = row(&mut Lcg(1), 1);
    private_a.visibility = Visibility::Private;
    private_a.context.project_id = Some("p0".into());
    private_a.context.user_id = Some("u7".into());
    private_a.context.entities = vec![entity("rust"), entity("falkordb")].into();
    let mut private_b = row(&mut Lcg(2), 2);
    private_b.visibility = Visibility::Private;
    private_b.context.project_id = Some("p1".into());
    private_b.context.user_id = Some("u9".into());
    private_b.context.entities = vec![entity("rust"), entity("falkordb")].into();
    storage.upsert_memory(&private_a).await.unwrap();
    storage.upsert_memory(&private_b).await.unwrap();

    let engine = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "dreams-xdomain".into(),
    );
    let region = RegionKey {
        org: ORG.into(),
        project: "*".into(),
        memory_type: 3,
    };
    let discoveries = engine.run_discovery(&region).await.expect("discovery");
    let cross: Vec<_> = discoveries
        .iter()
        .filter(|d| d.kind == DiscoveryKind::CrossDomain)
        .collect();
    assert!(!cross.is_empty(), "the shared-entity pair is proposed");
    let pattern = cross[0];
    let (a, b) = pattern.endpoints;

    // Neither endpoint's owner can read the OTHER endpoint: the pattern
    // is genuinely cross-user, invisible to each alone (the seam denies
    // invisible rows with PermissionDenied; ops maps that to
    // Unauthorized — either way, the row does not reach the caller).
    let vc7 = user_vc(7);
    let vc9 = user_vc(9);
    let denied = |result: Result<Option<Memory>, _>| {
        matches!(
            result,
            Err(exocortex_storage::StorageError::PermissionDenied)
        ) || matches!(result, Ok(None))
    };
    let read_a_as_9 = storage.get_memory_for(&a, &vc9).await;
    let read_b_as_7 = storage.get_memory_for(&b, &vc7).await;
    assert!(
        denied(read_a_as_9) && denied(read_b_as_7),
        "each endpoint is private to its owner"
    );
    assert_eq!(pattern.quality, 0.9);
}

/// §23 #22: three engines concurrently consolidate three distinct
/// regions of ONE org graph, each under its own lease (the lease keys
/// differ by region), per-region MCR² budgets hold, and the org-wide
/// reconciliation pass afterwards completes without regression.
#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_consolidate_three_regions_concurrently() {
    let storage = InMemoryStorage::new(ontology());
    let mut rng = Lcg(0x22);
    for (project, seed) in PROJECTS.iter().zip([11u64, 22, 33]) {
        for i in 0..4 {
            let mut m = row(&mut rng, (seed + i) as usize);
            m.context.project_id = Some((*project).into());
            m.embedding = Some(exocortex_kernel::Embedding {
                model: exocortex_kernel::EmbeddingModel {
                    name: "bge-small".into(),
                    version: "v1".into(),
                },
                vector: {
                    let mut v = vec![0.0f32; 64];
                    v[(seed % 60) as usize] = 1.0;
                    v[(seed % 60) as usize + 1] = 0.05;
                    v
                },
            });
            storage.upsert_memory(&m).await.unwrap();
        }
    }
    let engines: Vec<_> = ["node-a", "node-b", "node-c"]
        .into_iter()
        .map(|node| {
            DreamsEngine::new(
                Arc::new(storage.clone_dyn()),
                DreamsTrigger::default(),
                0.01,
                0.05,
                false,
                node.into(),
            )
        })
        .collect();
    let regions: Vec<_> = PROJECTS
        .iter()
        .map(|project| RegionKey {
            org: ORG.into(),
            project: (*project).into(),
            memory_type: 3,
        })
        .collect();
    let (ra, rb, rc) = tokio::join!(
        engines[0].try_consolidate(&regions[0]),
        engines[1].try_consolidate(&regions[1]),
        engines[2].try_consolidate(&regions[2]),
    );
    let results = [ra, rb, rc];
    for (index, result) in results.iter().enumerate() {
        let res = result.as_ref().expect("concurrent cycle succeeds");
        assert!(res.lease_epoch >= 1, "own lease per region: {index}");
        assert!(!res.regression, "per-region MCR2 budget holds: {index}");
        assert_eq!(
            res.region, regions[index],
            "each engine scoped to its region"
        );
    }
    // Each region's leases are distinct: a fourth engine can acquire a
    // DIFFERENT region's lease while one is held (the per-region key).
    let held = storage
        .acquire_lease(
            &LeaseKey::Dreams {
                org: ORG.into(),
                region: format!("{}:3", PROJECTS[0]).into(),
            },
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("region lease acquirable after the cycle released");
    let other = storage
        .acquire_lease(
            &LeaseKey::Dreams {
                org: ORG.into(),
                region: format!("{}:3", PROJECTS[1]).into(),
            },
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("a different region's lease is independent");
    assert_ne!(held.epoch, 0);
    assert_ne!(other.epoch, 0);
    // The org-wide reconciliation pass completes without regression.
    let reconciler = DreamsEngine::new(
        Arc::new(storage.clone_dyn()),
        DreamsTrigger::default(),
        0.01,
        0.05,
        false,
        "reconciler".into(),
    );
    let org_wide = reconciler
        .try_consolidate(&RegionKey {
            org: ORG.into(),
            project: "*".into(),
            memory_type: 3,
        })
        .await
        .expect("reconciliation cycle");
    assert!(!org_wide.regression, "org-wide budget respected");
}
