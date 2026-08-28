#!/bin/sh
set -eu

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM
tag="v9.9.9"
target="$(uname -s)-$(uname -m)"
case "$target" in
  Darwin-arm64) artifact_target="aarch64-apple-darwin" ;;
  Darwin-x86_64) artifact_target="x86_64-apple-darwin" ;;
  Linux-x86_64) artifact_target="x86_64-unknown-linux-gnu" ;;
  *) echo "installer test unsupported on $target" >&2; exit 1 ;;
esac
archive="exocortex-${tag#v}-$artifact_target.tar.gz"
release="$tmp/releases/$tag"
payload="$tmp/payload/exocortex-${tag#v}-$artifact_target"
mkdir -p "$release" "$payload" "$tmp/cargo"
model_dir="Xenova_bge-small-en-v1.5-ea104dacec62c0de699686887e3f920caeb4f3e3"
mkdir -p "$payload/models/$model_dir"
printf 'sidecar fixture\n' > "$payload/models/$model_dir/model.marker"
runtime_supported=0
case "$artifact_target" in
  aarch64-apple-darwin|x86_64-unknown-linux-gnu) runtime_supported=1 ;;
esac
if [ "$runtime_supported" -eq 1 ]; then
  mkdir -p "$payload/standalone-runtime"
  printf '#!/bin/sh\necho "Redis server v=8.2.3"\n' > "$payload/standalone-runtime/redis-server"
  chmod +x "$payload/standalone-runtime/redis-server"
  printf 'module fixture\n' > "$payload/standalone-runtime/falkordb.so"
  printf 'runtime manifest fixture\n' > "$payload/standalone-runtime/RUNTIME-MANIFEST.txt"
fi
for bin in exocortex exocortex-mcp-client exocortex-node exocortex-worker; do
  printf '#!/bin/sh\nexit 0\n' > "$payload/$bin"
  chmod +x "$payload/$bin"
done
tar -czf "$release/$archive" -C "$tmp/payload" "$(basename "$payload")"
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$release" && sha256sum "$archive" > "$archive.sha256")
else
  (cd "$release" && shasum -a 256 "$archive" > "$archive.sha256")
fi

mkdir -p "$tmp/mock-bin"
real_mv="$(command -v mv)"
cat > "$tmp/mock-bin/curl" <<'MOCK_CURL'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$MOCK_CURL_LOG"
output=""
url=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --proto)
      [ "${2:-}" = "=https" ] || { echo "mock curl: HTTPS protocol was not forced" >&2; exit 1; }
      shift 2
      ;;
    --tlsv1.2|--fail|--silent|--show-error|--location)
      shift
      ;;
    -o)
      output="$2"
      shift 2
      ;;
    https://github.com/memory-graph/exocortex/releases/download/*)
      url="$1"
      shift
      ;;
    *)
      echo "mock curl: unexpected argument $1" >&2
      exit 1
      ;;
  esac
done
[ -n "$output" ] && [ -n "$url" ] || { echo "mock curl: missing output or URL" >&2; exit 1; }
cp "$MOCK_RELEASE_ROOT/${url##*/}" "$output"
MOCK_CURL
chmod +x "$tmp/mock-bin/curl"
cat > "$tmp/mock-bin/mv" <<MOCK_MV
#!/bin/sh
set -eu
if [ "\${MOCK_FAIL_MODEL_COMMIT:-}" = 1 ] &&
   [ "\${2:-}" = "\${MOCK_FAIL_MODEL_DEST:-}" ] &&
   [ ! -e "\${MOCK_FAIL_STATE:-}" ]; then
  : > "\$MOCK_FAIL_STATE"
  echo "mock mv: injected model commit failure" >&2
  exit 1
fi
exec "$real_mv" "\$@"
MOCK_MV
chmod +x "$tmp/mock-bin/mv"
mock_log="$tmp/curl.log"

if INSTALL_VERSION="$tag" \
   EXOCORTEX_RELEASE_BASE_URL="https://attacker.example/releases" \
   PATH="$tmp/mock-bin:$PATH" \
   MOCK_RELEASE_ROOT="$release" \
   MOCK_CURL_LOG="$mock_log" \
   CARGO_HOME="$tmp/cargo-attacker" \
   sh scripts/release-install.sh >/dev/null 2>&1; then
  echo "installer accepted an overridden release origin" >&2
  exit 1
fi
test ! -e "$tmp/cargo-attacker/bin/exocortex-node"
test ! -e "$mock_log"

INSTALL_VERSION="$tag" \
PATH="$tmp/mock-bin:$PATH" \
MOCK_RELEASE_ROOT="$release" \
MOCK_CURL_LOG="$mock_log" \
CARGO_HOME="$tmp/cargo" \
sh scripts/release-install.sh >/dev/null
test -x "$tmp/cargo/bin/exocortex-node"
test "$(cat "$tmp/cargo/share/exocortex/models/$model_dir/model.marker")" = "sidecar fixture"
if [ "$runtime_supported" -eq 1 ]; then
  test -x "$tmp/cargo/share/exocortex/standalone/redis-server"
  test "$(cat "$tmp/cargo/share/exocortex/standalone/falkordb.so")" = "module fixture"
fi
test "$(wc -l < "$mock_log" | tr -d ' ')" -eq 2
grep -q -- "--proto =https --tlsv1.2" "$mock_log"

printf 'corrupt\n' > "$tmp/cargo/share/exocortex/models/$model_dir/model.marker"
INSTALL_VERSION="$tag" \
PATH="$tmp/mock-bin:$PATH" \
MOCK_RELEASE_ROOT="$release" \
MOCK_CURL_LOG="$mock_log" \
CARGO_HOME="$tmp/cargo" \
sh scripts/release-install.sh >/dev/null
test "$(cat "$tmp/cargo/share/exocortex/models/$model_dir/model.marker")" = "sidecar fixture"
test "$(wc -l < "$mock_log" | tr -d ' ')" -eq 4

for bin in exocortex exocortex-mcp-client exocortex-node exocortex-worker; do
  printf '#!/bin/sh\nprintf "old-%s\\n" "$0"\n' > "$tmp/cargo/bin/$bin"
  chmod +x "$tmp/cargo/bin/$bin"
done
printf 'old-sidecar\n' > "$tmp/cargo/share/exocortex/models/$model_dir/model.marker"
if [ "$runtime_supported" -eq 1 ]; then
  chmod u+w "$tmp/cargo/share/exocortex/standalone/falkordb.so"
  printf 'old-runtime\n' > "$tmp/cargo/share/exocortex/standalone/falkordb.so"
fi
if INSTALL_VERSION="$tag" \
   PATH="$tmp/mock-bin:$PATH" \
   MOCK_RELEASE_ROOT="$release" \
   MOCK_CURL_LOG="$mock_log" \
   MOCK_FAIL_MODEL_COMMIT=1 \
   MOCK_FAIL_MODEL_DEST="$tmp/cargo/share/exocortex/models/$model_dir" \
   MOCK_FAIL_STATE="$tmp/model-failure-fired" \
   CARGO_HOME="$tmp/cargo" \
   sh scripts/release-install.sh >/dev/null 2>&1; then
  echo "installer ignored injected model commit failure" >&2
  exit 1
fi
test "$(cat "$tmp/cargo/share/exocortex/models/$model_dir/model.marker")" = "old-sidecar"
if [ "$runtime_supported" -eq 1 ]; then
  test "$(cat "$tmp/cargo/share/exocortex/standalone/falkordb.so")" = "old-runtime"
fi
for bin in exocortex exocortex-mcp-client exocortex-node exocortex-worker; do
  grep -q 'old-' "$tmp/cargo/bin/$bin"
done

printf 'tampered' >> "$release/$archive"
if INSTALL_VERSION="$tag" \
   PATH="$tmp/mock-bin:$PATH" \
   MOCK_RELEASE_ROOT="$release" \
   MOCK_CURL_LOG="$mock_log" \
   CARGO_HOME="$tmp/cargo-tampered" \
   sh scripts/release-install.sh >/dev/null 2>&1; then
  echo "installer accepted a checksum-mismatched archive" >&2
  exit 1
fi
test ! -e "$tmp/cargo-tampered/bin/exocortex-node"
echo "release-installer ok: fixed HTTPS origin, failure-atomic install, and tamper refusal"
