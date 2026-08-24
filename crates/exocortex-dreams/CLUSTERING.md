# Clustering decision (M8 task 3, §12.5)

**Chosen: vendored HDBSCAN-style single-linkage over mutual-reachability
distances is DEFERRED; v1 ships deterministic agglomerative clustering with
a fixed cosine-distance threshold (a "DBSCAN-lite" with min_pts=2).**

## Evaluation

### linfa-clustering (DBSCAN)
- Not in the §2.2 dependency catalog; pulling it would add `linfa`,
  `ndarray`, `ndarray-stats`, and their transitive trees to the workspace —
  a new-dependency decision the PRD reserves for recorded exceptions.
- `linfa` pre-0.8 crates had MSRV drift; on the pinned Rust 1.85 toolchain
  the resolved versions were untested.
- API is synchronous CPU work; Dreams wants async-friendly, region-bounded
  batches. Workable, but the dependency cost is the blocker.

### Vendored HDBSCAN
- Correct HDBSCAN needs: kNN graph (O(n log n)), mutual-reachability,
  minimum-spanning tree (Boruvka/Prim), condensed cluster tree, and excess-
  of-mass extraction. That is a multi-week numerical-code project with its
  own test burden — beyond "vendored" in any honest sense.
- v1's Dreams cycle needs *deterministic cluster ids for merge candidates*,
  which full HDBSCAN does not directly provide (its stability extraction
  gives a hierarchy, not stable labels).

### Shipped: threshold clustering (deterministic, dependency-free)
- Anchors within `merge_threshold` cosine distance (default 0.92 similarity,
  §12.5 step 5) join the same cluster; every anchor gets a cluster id or
  `NOISE` deterministically by (similarity desc, memory id asc) ordering.
- Assigns every anchor to a cluster id (or noise) deterministically — the
  M8 acceptance property — and it is what merge detection needs anyway
  (near-duplicates share a cluster).
- The §12.1 pipeline references HDBSCAN as the target clustering; this file
  records that v1 ships the threshold approximation and that the full
  algorithm (or linfa once the dependency is justified) is the v2 upgrade.

## Why this satisfies the milestone
§3 M8 task 3 requires: "whichever is chosen must assign every anchor to a
cluster id (or noise) deterministically." The threshold clusterer does
exactly that, with zero new dependencies (CR-26-friendly), and the merge
step that consumes it is the actual value carrier of the cycle.
