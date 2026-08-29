#!/usr/bin/env bash
# Publish one coherent Exocortex workspace release in dependency order.
set -euo pipefail

ORDER=(
  exocortex-kernel exocortex-pack-dev-v1 exocortex-pack-mortgage-v1 exocortex-wire
  exocortex-adapter-sdk exocortex-storage exocortex-cache
  exocortex-reasoning exocortex-cluster exocortex-ingest exocortex-ops
  exocortex-dreams exocortex-server exocortex-client exocortex-worker
)

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
manifests=(Cargo.toml Cargo.lock)
for crate in "${ORDER[@]}"; do manifests+=("crates/$crate/Cargo.toml"); done

dirty=$(git status --porcelain -- "${manifests[@]}")
if [ -n "$dirty" ]; then
  echo "publish refused: release manifests or Cargo.lock have uncommitted changes:" >&2
  echo "$dirty" >&2
  exit 1
fi

if ! metadata=$(cargo metadata --format-version 1 --no-deps); then
  echo "publish refused: cargo metadata failed" >&2
  exit 1
fi
if ! release_version=$(python3 -c '
import json, sys
expected = set(sys.argv[1:])
versions = {p["name"]: p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] in expected}
missing = sorted(expected - versions.keys())
if missing: raise SystemExit("publish refused: missing packages: " + ", ".join(missing))
unique = sorted(set(versions.values()))
if len(unique) != 1:
    raise SystemExit("publish refused: mixed package versions: " + ", ".join(f"{n}={versions[n]}" for n in sorted(versions)))
print(unique[0])
' "${ORDER[@]}" <<<"$metadata"); then
  exit 1
fi
if [ -n "${PUBLISH_VERSION:-}" ] && [ "$PUBLISH_VERSION" != "$release_version" ]; then
  echo "publish refused: PUBLISH_VERSION=$PUBLISH_VERSION but workspace=$release_version" >&2
  exit 1
fi
echo "release version: $release_version"

# No mutation or publication occurs until every mandatory release check passes.
if ! bash scripts/verify-release.sh; then
  echo "publish refused: mandatory correctness prerequisite failed" >&2
  exit 1
fi

backup_dir=$(mktemp -d)
modified=()
cleanup() {
  local manifest key
  if [ "${#modified[@]}" -gt 0 ]; then
    for manifest in "${modified[@]}"; do
      key=${manifest//\//__}
      cp "$backup_dir/$key" "$manifest"
    done
  fi
  rm -rf "$backup_dir"
}
trap cleanup EXIT INT TERM

needs_strip() {
  case "$1" in
    exocortex-cluster|exocortex-ingest|exocortex-server) return 0 ;;
    *) return 1 ;;
  esac
}

strip_dev_deps() {
  local manifest="crates/$1/Cargo.toml" key
  key=${manifest//\//__}
  cp "$manifest" "$backup_dir/$key"
  modified+=("$manifest")
  python3 - "$manifest" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
lines = path.read_text().splitlines(keepends=True)
out, in_dev = [], False
for line in lines:
    if line.strip() == "[dev-dependencies]":
        in_dev = True
        out.append(line)
        continue
    if line.startswith("["):
        in_dev = line.strip() == "[dev-dependencies]"
    if in_dev and line.startswith("exocortex-"):
        continue
    out.append(line)
path.write_text("".join(out))
PY
}

restore_manifest() {
  local manifest="crates/$1/Cargo.toml" key item
  key=${manifest//\//__}
  cp "$backup_dir/$key" "$manifest"
  local kept=()
  for item in "${modified[@]}"; do
    [ "$item" = "$manifest" ] || kept+=("$item")
  done
  if [ "${#kept[@]}" -eq 0 ]; then
    modified=()
  else
    modified=("${kept[@]}")
  fi
}

publish_one() {
  local crate=$1 stripped=0 output attempt
  echo "==> $crate@$release_version"
  if needs_strip "$crate"; then strip_dev_deps "$crate"; stripped=1; fi
  for attempt in 1 2 3 4 5 6 7 8; do
    if output=$(cargo publish -p "$crate" --allow-dirty 2>&1); then
      echo "$output" | tail -1
      [ "$stripped" = 0 ] || restore_manifest "$crate"
      return 0
    fi
    if grep -q "already exists" <<<"$output"; then
      echo "    already published — skipping"
      [ "$stripped" = 0 ] || restore_manifest "$crate"
      return 0
    fi
    if grep -q "429" <<<"$output"; then
      echo "    rate-limited; waiting ${PUBLISH_RATE_LIMIT_SECONDS:-1860}s (attempt $attempt)"
      sleep "${PUBLISH_RATE_LIMIT_SECONDS:-1860}"
      continue
    fi
    echo "$output" | tail -3 >&2
    [ "$stripped" = 0 ] || restore_manifest "$crate"
    return 1
  done
  [ "$stripped" = 0 ] || restore_manifest "$crate"
  return 1
}

for crate in "${ORDER[@]}"; do
  if ! publish_one "$crate"; then
    exit 1
  fi
  sleep "${PUBLISH_DELAY_SECONDS:-20}"
done
echo "All Exocortex crates published at $release_version."
