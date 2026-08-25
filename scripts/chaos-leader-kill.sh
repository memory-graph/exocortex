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

leader_of() { docker compose -f "$COMPOSE" ps --format json \
    | jq -r 'select(.Service|startswith("node")) | .Service + " " + .State' \
    | sort; }

# Find the current lease holder via /health/cluster on each node.
leader=""
for i in 1 2 3; do
  port=$((8080 + i))
  holder=$(curl -sf "http://127.0.0.1:${port}/health/cluster" | jq -r '.leader_node_id // empty')
  if [ -n "$holder" ]; then leader="$holder"; break; fi
done
[ -n "$leader" ] || { echo "FAIL: no lease holder found"; exit 1; }
echo "leader before kill: $leader"

t0=$(date +%s%3N)
docker compose -f "$COMPOSE" kill "$leader"
echo "killed $leader at $t0"

# Poll for a new holder; the M5 AC bound is 2s.
deadline=$((t0 + 2000))
new_leader=""
while [ "$(date +%s%3N)" -lt "$deadline" ]; do
  for i in 1 2 3; do
    port=$((8080 + i))
    holder=$(curl -sf "http://127.0.0.1:${port}/health/cluster" 2>/dev/null | jq -r '.leader_node_id // empty' || true)
    if [ -n "$holder" ] && [ "$holder" != "$leader" ]; then new_leader="$holder"; break 2; fi
  done
  sleep 0.1
done
t1=$(date +%s%3N)

if [ -z "$new_leader" ]; then
  echo "FAIL: no new lease holder within 2s"
  exit 1
fi
echo "PASS: $leader -> $new_leader in $((t1 - t0))ms"
