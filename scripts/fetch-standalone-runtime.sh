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
cat > "$out/RUNTIME-MANIFEST.txt" <<EOF
Official package: @falkordblite/$package@$version
Archive SHA-512: $sha512
Source: https://github.com/FalkorDB/falkordblite-ts
Contains: Redis 8.2.3 and FalkorDB 4.16.3
Package license: MIT (official falkordblite platform package)
Redis source/license: https://github.com/redis/redis/tree/8.2.3
FalkorDB source/license: https://github.com/FalkorDB/FalkorDB/tree/v4.16.3
EOF
echo "standalone runtime verified: $package@$version"
