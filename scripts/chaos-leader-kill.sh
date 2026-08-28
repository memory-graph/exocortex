#!/usr/bin/env bash
# Chaos harness (§3 M5 AC, round-2 H10): kill the Dreams lease holder
# mid-flight and assert a new holder converges within 2s with no
# post-fence write landing from the old holder.
#
# Prereqs:
#   docker compose -f crates/exocortex-cluster/tests/docker-compose-cluster.yml up -d --build
#   (builds exocortex-node:local from the repo Dockerfile + a FalkorDB)
#
# Usage: ./scripts/chaos-leader-kill.sh
set -euo pipefail

COMPOSE=crates/exocortex-cluster/tests/docker-compose-cluster.yml
TLS_CA=crates/exocortex-server/tests/fixtures/localhost-cert.pem
PRINCIPAL_POLICY=crates/exocortex-cluster/tests/principal-policy.dev.json
AUTH_TOKEN=$(jq -er '.[0].bearer_token | select(type == "string" and length >= 32)' "$PRINCIPAL_POLICY")
CARGO_BIN=${CARGO_BIN:-cargo}

cluster_health() {
  curl --cacert "$TLS_CA" -sf \
    -H "Authorization: Bearer $AUTH_TOKEN" \
    "https://127.0.0.1:$1/health/cluster" 2>/dev/null
}

# Find the current lease holder via /health/cluster on each node. A
# fresh cluster may still be inside a predecessor's lease TTL — wait up
# to 15s for the first holder instead of failing instantly.
leader=""
cleanup() {
  if [ -n "$leader" ]; then
    docker compose -f "$COMPOSE" start "$leader" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
waited=0
while [ -z "$leader" ] && [ "$waited" -lt 15000 ]; do
  for i in 1 2 3; do
    port=$((8080 + i))
    holder=$(cluster_health "$port" | jq -r 'select(.leader_node_id == .node_id) | .leader_node_id // empty' || true)
    if [ -n "$holder" ]; then leader="$holder"; break; fi
  done
  [ -z "$leader" ] && { sleep 0.5; waited=$((waited + 500)); }
done
[ -n "$leader" ] || { echo "FAIL: no lease holder found within 15s"; exit 1; }
echo "leader before kill: $leader"

# Seed two mergeable anchors into the exact graph served by the production
# containers, enqueue a real distributed Dreams fire, and wait until the
# elected node's journaled mutation has committed at the opt-in barrier.
CHAOS_DREAMS_SEED=1 \
  FALKOR_URL=falkor://127.0.0.1:16379 \
  REDIS_URL=redis://127.0.0.1:16379 \
  "$CARGO_BIN" test -p exocortex-storage --features integration \
    --test integration seed_actual_production_dreams_barrier_fixture \
    -- --exact --nocapture

BARRIER_KEY=exocortex:chaos:dreams-r6
barrier=""
waited=0
while [ -z "$barrier" ] && [ "$waited" -lt 20000 ]; do
  barrier=$(redis-cli -p 16379 GET "$BARRIER_KEY:reached")
  [ -z "$barrier" ] && { sleep 0.1; waited=$((waited + 100)); }
done
[ -n "$barrier" ] || { echo "FAIL: production Dreams mutation did not reach the barrier"; exit 1; }
barrier_node=$(jq -er '.node_id' <<<"$barrier")
[ "$barrier_node" = "$leader" ] || {
  echo "FAIL: barrier owner $barrier_node differs from elected leader $leader"
  exit 1
}
old_epoch=$(jq -er '.lease_epoch' <<<"$barrier")
old_lsn_start=$(jq -er '.lsn_start' <<<"$barrier")
old_lsn_end=$(jq -er '.lsn_end_exclusive' <<<"$barrier")
echo "production Dreams barrier: node=$barrier_node epoch=$old_epoch lsns=[$old_lsn_start,$old_lsn_end)"

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
t0=$(now_ms)
docker compose -f "$COMPOSE" kill "$leader"
echo "killed $leader at $t0"

# Poll for a new holder; the M5 AC bound is 2s.
deadline=$((t0 + 2000))
new_leader=""
while [ "$(now_ms)" -lt "$deadline" ]; do
  for i in 1 2 3; do
    port=$((8080 + i))
    holder=$(cluster_health "$port" | jq -r 'select(.leader_node_id == .node_id) | .leader_node_id // empty' || true)
    if [ -n "$holder" ] && [ "$holder" != "$leader" ]; then new_leader="$holder"; break 2; fi
  done
  sleep 0.1
done
t1=$(now_ms)

if [ -z "$new_leader" ]; then
  echo "FAIL: no new lease holder within 2s"
  exit 1
fi
echo "takeover PASS: $leader -> $new_leader in $((t1 - t0))ms"

# The successor recovers the killed node's Redis processing item and runs the
# same real cycle without claiming the one-shot barrier. The global owner lease
# converges inside 2s, while the independent in-flight region lease retains its
# 60s rollback reserve; retry the authoritative Falkor assertion long enough
# to observe the successor cross the same journaled
# mutation boundary and durably acknowledge the recovered fire. The old
# reserved LSN interval must contain no memory/relationship assertion and no
# active old-epoch journal. The duplicate fixture may leave no closed row
# because the production MCR² guard is allowed to roll the merge back.
assert_log="/tmp/exocortex-chaos-dreams-assert.$$.log"
asserted=0
for _ in $(seq 1 320); do
  if CHAOS_DREAMS_ASSERT=1 \
    CHAOS_OLD_LSN_START="$old_lsn_start" \
    CHAOS_OLD_LSN_END="$old_lsn_end" \
    CHAOS_OLD_EPOCH="$old_epoch" \
    FALKOR_URL=falkor://127.0.0.1:16379 \
    REDIS_URL=redis://127.0.0.1:16379 \
    "$CARGO_BIN" test -p exocortex-storage --features integration \
      --test integration assert_actual_production_dreams_barrier_fixture \
      -- --exact --nocapture >"$assert_log" 2>&1; then
    asserted=1
    break
  fi
  sleep 0.25
done
if [ "$asserted" -ne 1 ]; then
  cat "$assert_log"
  echo "FAIL: successor did not complete the production Dreams recovery proof"
  exit 1
fi
cat "$assert_log"
rm -f "$assert_log"

echo "PASS: killed production Dreams owner left no old-LSN residue; successor completed recovery"
