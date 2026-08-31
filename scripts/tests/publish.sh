#!/usr/bin/env bash
# Disposable-repository regression coverage for scripts/publish.sh.
set -euo pipefail

source_root=$(git rev-parse --show-toplevel)
fixture=$(mktemp -d)
# ENOTEMPTY teardown races on shared-runner filesystems: retry, then
# let the runner's temp cleanup take whatever is left (never fail a
# green run on housekeeping).
trap 'rm -rf "$fixture" 2>/dev/null || { sleep 1; rm -rf "$fixture"; } 2>/dev/null || true' EXIT
mkdir -p "$fixture/repo/scripts" "$fixture/repo/fake-bin" "$fixture/repo/crates"
cp "$source_root/scripts/publish.sh" "$fixture/repo/scripts/publish.sh"
cp "$source_root/scripts/verify-release.sh" "$fixture/repo/scripts/verify-release.sh"

order=(
  exocortex-kernel exocortex-pack-dev-v1 exocortex-pack-mortgage-v1 exocortex-wire
  exocortex-adapter-sdk exocortex-storage exocortex-cache
  exocortex-reasoning exocortex-cluster exocortex-dreams exocortex-ingest
  exocortex-ops exocortex-server exocortex-client exocortex-worker
)
printf '[workspace]\n' > "$fixture/repo/Cargo.toml"
printf '# lock\n' > "$fixture/repo/Cargo.lock"
for crate in "${order[@]}"; do
  mkdir -p "$fixture/repo/crates/$crate"
  printf '[package]\nname = "%s"\nversion = "0.2.2"\n' "$crate" \
    > "$fixture/repo/crates/$crate/Cargo.toml"
done
cat >> "$fixture/repo/crates/exocortex-cluster/Cargo.toml" <<'EOF'
[dev-dependencies]
exocortex-server = "0.2.2"
EOF

cat > "$fixture/repo/fake-bin/cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$PUBLISH_TEST_LOG"
if [ "$1" = metadata ]; then
  python3 - <<'PY'
import json, pathlib, re
names = "exocortex-kernel exocortex-pack-dev-v1 exocortex-pack-mortgage-v1 exocortex-wire exocortex-adapter-sdk exocortex-storage exocortex-cache exocortex-reasoning exocortex-cluster exocortex-dreams exocortex-ingest exocortex-ops exocortex-server exocortex-client exocortex-worker".split()
packages = []
for name in names:
    manifest = pathlib.Path("crates", name, "Cargo.toml").read_text()
    version = re.search(r'^version = "([^"]+)"', manifest, re.MULTILINE).group(1)
    packages.append({"name": name, "version": version})
print(json.dumps({"packages": packages}))
PY
  exit 0
fi
if [ "$1" = publish ] && [ "$3" = exocortex-cluster ]; then
  echo 'deliberate fixture publish failure' >&2
  exit 19
fi
exit 0
EOF
chmod +x "$fixture/repo/fake-bin/cargo"

cd "$fixture/repo"
git init -q
git config user.email fixture@example.invalid
git config user.name Fixture
git add Cargo.toml Cargo.lock crates scripts
git commit -qm fixture
export PATH="$fixture/repo/fake-bin:$PATH"
export PUBLISH_TEST_LOG="$fixture/publish.log"
export PUBLISH_DELAY_SECONDS=0 PUBLISH_RATE_LIMIT_SECONDS=0

printf '# user edit\n' >> crates/exocortex-kernel/Cargo.toml
before=$(git hash-object crates/exocortex-kernel/Cargo.toml)
if bash scripts/publish.sh >"$fixture/dirty.out" 2>&1; then
  echo 'expected dirty-manifest refusal' >&2; exit 1
fi
[ "$before" = "$(git hash-object crates/exocortex-kernel/Cargo.toml)" ]
[ ! -e "$PUBLISH_TEST_LOG" ]
git checkout -q -- crates/exocortex-kernel/Cargo.toml

printf 'user note\n' > NOTES.txt
cluster_before=$(git hash-object crates/exocortex-cluster/Cargo.toml)
if bash scripts/publish.sh >"$fixture/failure.out" 2>&1; then
  echo 'expected deliberate publish failure' >&2
  cat "$fixture/failure.out" >&2
  cat "$PUBLISH_TEST_LOG" >&2
  exit 1
fi
[ "$cluster_before" = "$(git hash-object crates/exocortex-cluster/Cargo.toml)" ]
[ "$(cat NOTES.txt)" = 'user note' ]
grep -q '^fmt --all -- --check$' "$PUBLISH_TEST_LOG"
grep -q '^publish -p exocortex-cluster --allow-dirty$' "$PUBLISH_TEST_LOG"
if grep -q -- '--no-verify' "$PUBLISH_TEST_LOG"; then
  echo 'publish verification was bypassed' >&2; exit 1
fi

# A clean but mixed-version repository is refused before verification or any
# network-facing publish command.
printf '[package]\nname = "exocortex-client"\nversion = "9.9.9"\n' \
  > crates/exocortex-client/Cargo.toml
git add crates/exocortex-client/Cargo.toml
git commit -qm mixed-version-fixture
rm -f "$PUBLISH_TEST_LOG"
if bash scripts/publish.sh >"$fixture/mixed.out" 2>&1; then
  echo 'expected mixed-version refusal' >&2; exit 1
fi
grep -q 'mixed package versions' "$fixture/mixed.out"
[ "$(wc -l < "$PUBLISH_TEST_LOG" | tr -d ' ')" = 1 ]
grep -q '^metadata --format-version 1 --no-deps$' "$PUBLISH_TEST_LOG"
echo 'publish fixture ok: fail-closed, verified, byte-preserving'
