#!/bin/sh
# Exocortex installer uploaded by the Release workflow.
set -eu

repo="memory-graph/exocortex"
tag="${INSTALL_VERSION:-latest}"
[ "$tag" = "latest" ] && tag=$(curl -fsSL "https://api.github.com/repos/$repo/releases/latest" | grep -o '"tag_name": *"[^"]*"' | head -1 | cut -d'"' -f4)
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  target="aarch64-apple-darwin" ;;
  Darwin-x86_64) target="x86_64-apple-darwin" ;;
  Linux-x86_64)  target="x86_64-unknown-linux-gnu" ;;
  *) echo "unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

archive="exocortex-${tag#v}-$target.tar.gz"
base="${EXOCORTEX_RELEASE_BASE_URL:-https://github.com/$repo/releases/download}"
url="$base/$tag/$archive"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM
echo "downloading $url"
curl -fsSL "$url" -o "$tmp/$archive"
curl -fsSL "$url.sha256" -o "$tmp/$archive.sha256"
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$tmp" && sha256sum -c "$archive.sha256")
elif command -v shasum >/dev/null 2>&1; then
  (cd "$tmp" && shasum -a 256 -c "$archive.sha256")
else
  echo "install refused: sha256sum or shasum is required" >&2
  exit 1
fi

tar -xzf "$tmp/$archive" -C "$tmp"
src="$(find "$tmp" -maxdepth 2 -name exocortex-mcp-client -exec dirname {} \; | head -1)"
[ -n "$src" ] || { echo "install refused: archive has no client binary" >&2; exit 1; }
dest="${CARGO_HOME:-$HOME/.cargo}/bin"
mkdir -p "$dest"
for bin in exocortex exocortex-mcp-client exocortex-node exocortex-worker; do
  install -m 0755 "$src/$bin" "$dest/$bin" 2>/dev/null || cp "$src/$bin" "$dest/$bin"
done
case ":$PATH:" in
  *":$dest:"*) ;;
  *) echo "note: add $dest to PATH" ;;
esac
echo "installed: exocortex, exocortex-mcp-client, exocortex-node, exocortex-worker"
echo "next: exocortex --mode mcp-standalone --org my-org --user me"
