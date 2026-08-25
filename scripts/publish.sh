#!/usr/bin/env bash
# Publish the exocortex crates to crates.io in dependency order.
#
# Two facts make this non-trivial:
#  1. crates.io rate-limits NEW crates (~5 per 30 min for a fresh account).
#     The script waits out the window and retries, so a full first run
#     takes ~90 minutes.
#  2. Three crates DEV-depend on crates published later in the order
#     (cluster→server; ingest→client,dreams; server→client). A dev-dep on
#     an unpublished crate fails manifest normalization even with
#     --no-verify, so those lines are temporarily stripped for the
#     publish and restored immediately after.
#
# Idempotent: already-published versions are skipped (cargo publish
# errors with "already exists" and we treat that as success).
set -euo pipefail

ORDER=(
  exocortex-kernel
  exocortex-pack-dev-v1
  exocortex-wire
  exocortex-adapter-sdk
  exocortex-storage
  exocortex-cache
  exocortex-reasoning
  exocortex-cluster
  exocortex-ingest
  exocortex-ops
  exocortex-dreams
  exocortex-server
  exocortex-client
  exocortex-worker
)

# Crates whose exocortex dev-deps must be stripped for the publish
# (they cycle with later-published crates). POSIX-sh compatible.
needs_strip() {
  case "$1" in
    exocortex-cluster|exocortex-ingest|exocortex-server) return 0 ;;
    *) return 1 ;;
  esac
}


strip_dev_dep() { # crate-name — remove exocortex dev-deps (restored after)
  local c="$1"
  python3 - "$c" <<'PY'
import sys
crate = sys.argv[1]
path = f"crates/{crate}/Cargo.toml"
lines = open(path).read().splitlines(keepends=True)
out, in_dev = [], False
for ln in lines:
    if ln.strip() == "[dev-dependencies]":
        in_dev = True
        out.append(ln)
        continue
    if ln.startswith("["):
        in_dev = ln.strip() == "[dev-dependencies]"
    if in_dev and ln.startswith("exocortex-"):
        continue
    out.append(ln)
open(path, "w").write("".join(out))
PY
}

restore() { git checkout -- "crates/$1/Cargo.toml"; }

publish_one() {
  local c="$1"
  echo "==> $c"
  local stripped=0
  if needs_strip "$c"; then
    strip_dev_dep "$c"
    stripped=1
  fi
  for attempt in 1 2 3 4 5 6 7 8; do
    if out=$(cargo publish -p "$c" --allow-dirty --no-verify 2>&1); then
      echo "$out" | tail -1
      [ "$stripped" = 1 ] && restore "$c"
      return 0
    fi
    if echo "$out" | grep -q "already exists"; then
      echo "    already published — skipping"
      [ "$stripped" = 1 ] && restore "$c"
      return 0
    fi
    if echo "$out" | grep -q "429"; then
      echo "    rate-limited; waiting 31 min (attempt $attempt)"
      sleep 1860
      continue
    fi
    echo "$out" | tail -3
    [ "$stripped" = 1 ] && restore "$c"
    return 1
  done
  [ "$stripped" = 1 ] && restore "$c"
  return 1
}

for c in "${ORDER[@]}"; do
  publish_one "$c"
  sleep 20
done
echo "All published."
