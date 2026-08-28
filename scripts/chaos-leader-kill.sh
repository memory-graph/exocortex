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

leader_of() { docker compose -f "$COMPOSE" ps --format json \
    | jq -r 'select(.Service|startswith("node")) | .Service + " " + .State' \
    | sort; }

# Find the current lease holder via /health/cluster on each node. A
# fresh cluster may still be inside a predecessor's lease TTL — wait up
# to 15s for the first holder instead of failing instantly.
leader=""
probe_pid=""
cleanup() {
  if [ -n "$probe_pid" ]; then kill "$probe_pid" >/dev/null 2>&1 || true; fi
  if [ -n "$leader" ]; then
    docker compose -f "$COMPOSE" start "$leader" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
waited=0
while [ -z "$leader" ] && [ "$waited" -lt 15000 ]; do
  for i in 1 2 3; do
    port=$((8080 + i))
    holder=$(cluster_health "$port" | jq -r '.leader_node_id // empty' || true)
    if [ -n "$holder" ]; then leader="$holder"; break; fi
  done
  [ -z "$leader" ] && { sleep 0.5; waited=$((waited + 500)); }
done
[ -n "$leader" ] || { echo "FAIL: no lease holder found within 15s"; exit 1; }
echo "leader before kill: $leader"

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
    holder=$(cluster_health "$port" | jq -r '.leader_node_id // empty' || true)
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

# Cross the exact Dreams journaled-write linearization point against the same
# live Falkor backend: pause the old owner's mutation after LSN allocation,
# acquire a newer epoch, release the stale attempt, and assert that current,
# historical, and cycle-journal residue all remain empty.
CHAOS_OLD_OWNER="$leader" CHAOS_NEW_OWNER="$new_leader" \
  FALKOR_URL=falkor://127.0.0.1:16379 \
  "$CARGO_BIN" test -p exocortex-storage --features integration \
    --test fencing_live inflight_stale_dreams_write_is_fenced_after_takeover_live \
    -- --exact --nocapture

echo "PASS: authenticated takeover and no-zombie Dreams write fencing"
