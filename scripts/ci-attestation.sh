#!/usr/bin/env bash
# REL5: CI attestation for publishing without a local matrix run.
#
# `publish.sh` fail-closed demands the full gate matrix before any
# upload; when the release machine cannot complete one (the 0.5.2
# assessment-queue incident), this script supplies the same evidence
# from CI: a completed-success `ci` workflow run whose headSha is the
# EXACT tree being published and whose gate shards are all green.
# Fail-closed is preserved — no matching run, no publish.
#
# Usage: scripts/ci-attestation.sh [sha]   (default: HEAD)
# Exit 0 = attested; exit 1 = refused (reason on stderr).
set -euo pipefail

REPO="${EXOCORTEX_REPO:-memory-graph/exocortex}"
# The ci.yml shards that carry the gate set (AGENTS.md "Gates" list;
# live-backend legs never run in CI and are reported by verify-release
# as loudly skipped there too).
REQUIRED_JOBS="lint tests fixtures xtask-gates"

command -v gh >/dev/null 2>&1 || {
  echo "ci-attestation refused: gh CLI is not installed" >&2
  exit 1
}
command -v python3 >/dev/null 2>&1 || {
  echo "ci-attestation refused: python3 is not installed" >&2
  exit 1
}

sha="${1:-$(git rev-parse HEAD)}"
case "$sha" in
  *[!0-9a-f]*|'') echo "ci-attestation refused: not a commit sha: $sha" >&2; exit 1 ;;
esac
# gh --commit needs the full 40-char sha; expand when the object exists
# (an unknown sha stays as-is and will simply find no runs).
sha="$(git rev-parse "$sha" 2>/dev/null || echo "$sha")"

runs_json=$(gh run list --repo "$REPO" --workflow ci.yml --commit "$sha" \
  --json databaseId,headSha,conclusion,status 2>/dev/null) || {
  echo "ci-attestation refused: gh could not list ci runs for $sha" >&2
  exit 1
}

run_id=$(python3 - "$runs_json" <<'PY'
import json, sys
try:
    runs = json.loads(sys.argv[1])
except json.JSONDecodeError:
    sys.exit("refused: gh returned unparseable run data")
green = [r for r in runs
         if r.get("status") == "completed"
         and r.get("conclusion") == "success"
         and r.get("headSha")]
if not green:
    sys.exit("refused: no completed-success ci run on this commit")
# headSha equality is checked again below against the requested sha;
# gh --commit already filters, this is defense in depth.
print(green[0]["databaseId"])
PY
) || { echo "ci-attestation $sha: $run_id" >&2; exit 1; }

head_sha=$(gh run view "$run_id" --repo "$REPO" --json headSha --jq .headSha 2>/dev/null) || {
  echo "ci-attestation refused: gh could not read run $run_id" >&2
  exit 1
}
if [ "$head_sha" != "$sha" ]; then
  echo "ci-attestation refused: run $run_id is $head_sha, not $sha" >&2
  exit 1
fi

jobs_json=$(gh run view "$run_id" --repo "$REPO" --json jobs 2>/dev/null) || {
  echo "ci-attestation refused: gh could not read run $run_id jobs" >&2
  exit 1
}

python3 - "$jobs_json" "$REQUIRED_JOBS" "$sha" <<'PY'
import json, sys
try:
    jobs = {j["name"]: j.get("conclusion") for j in json.loads(sys.argv[1])["jobs"]}
except (json.JSONDecodeError, KeyError):
    sys.exit("refused: gh returned unparseable job data")
required = sys.argv[2].split()
missing = [name for name in required if name not in jobs]
if missing:
    sys.exit("refused: ci run is missing gate shards: " + ", ".join(missing))
red = [name for name in required if jobs[name] != "success"]
if red:
    sys.exit("refused: ci gate shards not green: " +
             ", ".join(f"{name}={jobs[name]}" for name in red))
print("attested: all gate shards green on " + sys.argv[3])
PY
