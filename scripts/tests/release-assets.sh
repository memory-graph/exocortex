#!/usr/bin/env bash
# Disposable fake-GitHub regression for immutable repeated-tag releases.
set -euo pipefail

source_root=$(git rev-parse --show-toplevel)
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/bin" "$fixture/dist" "$fixture/remote"
cp "$source_root/scripts/publish-release-assets.sh" "$fixture/publish-release-assets.sh"
printf 'archive-v1\n' > "$fixture/dist/exocortex.tar.gz"
printf 'digest-v1\n' > "$fixture/dist/exocortex.sha256"

cat > "$fixture/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$RELEASE_TEST_LOG"
[ "$1" = release ] || exit 90
case "$2" in
  view)
    [ -f "$RELEASE_REMOTE/state" ] || exit 1
    if [[ " $* " = *" --json isDraft "* ]]; then
      cat "$RELEASE_REMOTE/state"
    elif [[ " $* " = *" --json assets "* ]]; then
      for asset in "$RELEASE_REMOTE/assets/"*; do
        [ -f "$asset" ] && basename "$asset"
      done
      :
    fi
    ;;
  create)
    [ ! -f "$RELEASE_REMOTE/state" ] || exit 2
    mkdir -p "$RELEASE_REMOTE/assets"
    for argument in "$@"; do
      [ -f "$argument" ] && cp "$argument" "$RELEASE_REMOTE/assets/$(basename "$argument")"
    done
    printf 'true\n' > "$RELEASE_REMOTE/state"
    ;;
  edit)
    printf 'false\n' > "$RELEASE_REMOTE/state"
    ;;
  upload)
    for argument in "$@"; do
      [ -f "$argument" ] && cp "$argument" "$RELEASE_REMOTE/assets/$(basename "$argument")"
    done
    :
    ;;
  download)
    destination=
    while [ "$#" -gt 0 ]; do
      if [ "$1" = --dir ]; then destination=$2; break; fi
      shift
    done
    [ -n "$destination" ]
    for asset in "$RELEASE_REMOTE/assets/"*; do
      [ -f "$asset" ] && cp "$asset" "$destination/"
    done
    ;;
  *) exit 91 ;;
esac
EOF
chmod +x "$fixture/bin/gh"
export PATH="$fixture/bin:$PATH"
export RELEASE_REMOTE="$fixture/remote"
export RELEASE_TEST_LOG="$fixture/gh.log"

if ! bash "$fixture/publish-release-assets.sh" v1.2.3 owner/repo "$fixture/dist" \
  >"$fixture/partial.out" 2>&1; then
  cat "$fixture/partial.out" >&2
  cat "$RELEASE_TEST_LOG" >&2
  exit 1
fi
[ "$(cat "$RELEASE_REMOTE/state")" = false ]
remote_before=$(git hash-object "$RELEASE_REMOTE/assets/exocortex.tar.gz")

# An exact rerun is a read/compare-only success.
bash "$fixture/publish-release-assets.sh" v1.2.3 owner/repo "$fixture/dist"
[ "$(grep -c '^release create ' "$RELEASE_TEST_LOG")" = 1 ]

# A repeated tag carrying different bytes must fail without touching the remote.
printf 'archive-v2-different\n' > "$fixture/dist/exocortex.tar.gz"
if bash "$fixture/publish-release-assets.sh" v1.2.3 owner/repo "$fixture/dist" \
  >"$fixture/mismatch.out" 2>&1; then
  echo 'expected non-identical repeated release to fail' >&2
  exit 1
fi
grep -q 'not byte-identical' "$fixture/mismatch.out"
[ "$remote_before" = "$(git hash-object "$RELEASE_REMOTE/assets/exocortex.tar.gz")" ]

# A partial draft is recoverable: existing bytes must match, only the missing
# asset is uploaded, and the complete draft is then published.
printf 'archive-v1\n' > "$fixture/dist/exocortex.tar.gz"
printf 'true\n' > "$RELEASE_REMOTE/state"
rm "$RELEASE_REMOTE/assets/exocortex.sha256"
if ! bash "$fixture/publish-release-assets.sh" v1.2.3 owner/repo "$fixture/dist" \
  >"$fixture/partial-retry.out" 2>&1; then
  cat "$fixture/partial-retry.out" >&2
  cat "$RELEASE_TEST_LOG" >&2
  exit 1
fi
[ "$(cat "$RELEASE_REMOTE/state")" = false ]
[ -f "$RELEASE_REMOTE/assets/exocortex.sha256" ]
grep -q '^release upload v1.2.3 .*exocortex.sha256.*--repo owner/repo$' "$RELEASE_TEST_LOG"

# A draft left before any asset upload is also recoverable without deletion.
printf 'true\n' > "$RELEASE_REMOTE/state"
rm "$RELEASE_REMOTE/assets/exocortex.sha256" "$RELEASE_REMOTE/assets/exocortex.tar.gz"
bash "$fixture/publish-release-assets.sh" v1.2.3 owner/repo "$fixture/dist"
[ "$(cat "$RELEASE_REMOTE/state")" = false ]
[ -f "$RELEASE_REMOTE/assets/exocortex.sha256" ]
[ -f "$RELEASE_REMOTE/assets/exocortex.tar.gz" ]
if grep -q -- '--clobber' "$RELEASE_TEST_LOG"; then
  echo 'immutable release fixture observed an overwrite flag' >&2
  exit 1
fi
echo 'release asset fixture ok: draft-first, identical retry, partial recovery, mismatch preserved'
