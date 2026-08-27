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

INSTALL_VERSION="$tag" \
EXOCORTEX_RELEASE_BASE_URL="file://$tmp/releases" \
CARGO_HOME="$tmp/cargo" \
sh scripts/release-install.sh >/dev/null
test -x "$tmp/cargo/bin/exocortex-node"

printf 'tampered' >> "$release/$archive"
if INSTALL_VERSION="$tag" \
   EXOCORTEX_RELEASE_BASE_URL="file://$tmp/releases" \
   CARGO_HOME="$tmp/cargo-tampered" \
   sh scripts/release-install.sh >/dev/null 2>&1; then
  echo "installer accepted a checksum-mismatched archive" >&2
  exit 1
fi
test ! -e "$tmp/cargo-tampered/bin/exocortex-node"
echo "release-installer ok: valid archive installs; tampered archive is refused before extraction"
