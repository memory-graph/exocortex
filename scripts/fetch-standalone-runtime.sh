#!/bin/sh
# Fetch the pinned, self-contained Redis/Falkor runtime published by the
# official FalkorDB falkordblite project. The archive digest is part of the
# release contract; unchecked native code never enters an Exocortex artifact.
set -eu

out=${1:?usage: fetch-standalone-runtime.sh OUT_DIR TARGET}
target=${2:?usage: fetch-standalone-runtime.sh OUT_DIR TARGET}
version=8.2.3-falkordb.4.16.3
case "$target" in
  x86_64-unknown-linux-gnu)
    package=linux-x64
    sha512=d17e0e95bde5067324c6f1ac4624d34f8abf48d4e40cf68d3c1196366f2a481a1691b63a571802b3d8c7464919581d75848ce32d125b7285528e31aeae85e9eb
    ;;
  aarch64-apple-darwin)
    package=darwin-arm64
    sha512=c9299584a4e193c494ffdf414af4fbc3237bcf5e6b4bac63b7693c62c0e11d94b51fc57613100d5e33b2b68ee3df83b0ae7abb427e6b135f9e89a9675b3d8383
    ;;
  *)
    echo "no self-contained Falkor runtime is published for $target" >&2
    exit 2
    ;;
esac

tmp=$(mktemp -d "${TMPDIR:-/tmp}/exocortex-falkor-runtime.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
archive="$tmp/runtime.tgz"
url="https://registry.npmjs.org/@falkordblite/$package/-/$package-$version.tgz"
curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location "$url" -o "$archive"
if command -v sha512sum >/dev/null 2>&1; then
  actual=$(sha512sum "$archive" | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then
  actual=$(shasum -a 512 "$archive" | awk '{print $1}')
else
  echo "standalone runtime verification needs sha512sum or shasum" >&2
  exit 1
fi
[ "$actual" = "$sha512" ] || {
  echo "standalone runtime digest mismatch: expected $sha512, got $actual" >&2
  exit 1
}
tar -xzf "$archive" -C "$tmp"
[ -x "$tmp/package/bin/redis-server" ] || { echo "runtime has no redis-server" >&2; exit 1; }
[ -f "$tmp/package/bin/falkordb.so" ] || { echo "runtime has no FalkorDB module" >&2; exit 1; }
mkdir -p "$out"
install -m 0755 "$tmp/package/bin/redis-server" "$out/redis-server"
install -m 0555 "$tmp/package/bin/falkordb.so" "$out/falkordb.so"

# The darwin-arm64 module's load commands name Homebrew's libomp and
# openssl@3 at absolute paths; stock machines (CI runners, clean Macs)
# do not have them. Bundle the dylibs beside the module (the supervisor
# points the store child's DYLD_LIBRARY_PATH at this directory), pinned
# by content digest: ghcr.io blobs are immutable, and these are the last
# arm64_sequoia bottles (macOS 15 floor, matching the module's own
# minos 15.0) recorded in homebrew-core history:
#   libomp 21.1.6      — homebrew-core commit 291662d783 (2025-11-21)
#   openssl@3 3.6.0    — homebrew-core commit 94108d2635 (2025-10-03)
fetch_bottle_dylib() {
  repo=$1
  digest=$2
  shift 2
  token_json=$(sh -c "curl -fsSL \"https://ghcr.io/token?scope=repository:homebrew/core/$repo:pull\"")
  token=$(printf '%s' "$token_json" | sed -E 's/.*"token":"([^"]+)".*/\1/')
  [ -n "$token" ] || { echo "ghcr token fetch failed for $repo" >&2; exit 1; }
  sh -c "curl -fsSL -H 'Authorization: Bearer $token' \
    'https://ghcr.io/v2/homebrew/core/$repo/blobs/sha256:$digest' -o '$tmp/bottle.tar.gz'"
  if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$tmp/bottle.tar.gz" | awk '{print $1}')
  else
    actual=$(shasum -a 256 "$tmp/bottle.tar.gz" | awk '{print $1}')
  fi
  [ "$actual" = "$digest" ] || {
    echo "bottle digest mismatch for $repo: expected $digest, got $actual" >&2
    exit 1
  }
  rm -rf "$tmp/bottle-x"
  mkdir -p "$tmp/bottle-x"
  tar -xzf "$tmp/bottle.tar.gz" -C "$tmp/bottle-x"
  for name in "$@"; do
    found=$(find "$tmp/bottle-x" -name "$name" | head -1)
    [ -n "$found" ] || { echo "bottle for $repo has no $name" >&2; exit 1; }
    install -m 0555 "$found" "$out/$name"
  done
}
case "$target" in
  aarch64-apple-darwin)
    fetch_bottle_dylib libomp \
      d99017c08056863871197e62dee6e4ca5aaa10a78ea7be2eeb1e1b54cdf3714a \
      libomp.dylib
    fetch_bottle_dylib openssl/3 \
      9a8fa2ae1ef3424b116d7e6422d979e0290f4affdef072b1592e4535d2617d92 \
      libssl.3.dylib libcrypto.3.dylib
    ;;
esac

cat > "$out/RUNTIME-MANIFEST.txt" <<EOF
Official package: @falkordblite/$package@$version
Archive SHA-512: $sha512
Source: https://github.com/FalkorDB/falkordblite-ts
Contains: Redis 8.2.3 and FalkorDB 4.16.3
Package license: MIT (official falkordblite platform package)
Redis source/license: https://github.com/redis/redis/tree/8.2.3
FalkorDB source/license: https://github.com/FalkorDB/FalkorDB/tree/v4.16.3
Platform floors (verified 2026-09-01, release run 33543324895): the
darwin-arm64 module declares minos 15.0 (otool LC_BUILD_VERSION), so it
cannot load on macOS 14; the linux-x64 redis-server references GLIBC_2.38
symbols, so it cannot load on glibc < 2.38 (ubuntu 22.04 = 2.35). Client
and backend-node modes do not use this runtime and keep the exocortex
binaries' own floor.
EOF
case "$target" in
  aarch64-apple-darwin)
    cat >> "$out/RUNTIME-MANIFEST.txt" <<EOF
Bundled macOS dylibs (the module's Homebrew dependencies, pinned by
immutable ghcr.io content digest; loaded via the store child's
DYLD_LIBRARY_PATH pointing here): libomp 21.1.6 (MIT/LLVM Apache-2.0
with LLVM exceptions, https://openmp.llvm.org) and openssl@3 3.6.0
(Apache-2.0, https://www.openssl.org), both the last arm64_sequoia
bottles (macOS 15 floor) from homebrew-core. No Homebrew install is
required on the target machine.
EOF
    ;;
esac
echo "standalone runtime verified: $package@$version"
