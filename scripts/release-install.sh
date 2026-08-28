#!/bin/sh
# Exocortex installer uploaded by the Release workflow.
set -eu

repo="memory-graph/exocortex"
tag="${INSTALL_VERSION:-latest}"
if [ "${EXOCORTEX_RELEASE_BASE_URL+x}" = x ]; then
  echo "install refused: release origin is fixed by the shipped installer" >&2
  exit 1
fi
curl_https() {
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location "$@"
}
[ "$tag" = "latest" ] && tag=$(curl_https "https://api.github.com/repos/$repo/releases/latest" | grep -o '"tag_name": *"[^"]*"' | head -1 | cut -d'"' -f4)
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)  target="aarch64-apple-darwin" ;;
  Darwin-x86_64) target="x86_64-apple-darwin" ;;
  Linux-x86_64)  target="x86_64-unknown-linux-gnu" ;;
  *) echo "unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

archive="exocortex-${tag#v}-$target.tar.gz"
base="https://github.com/$repo/releases/download"
url="$base/$tag/$archive"
tmp="$(mktemp -d)"
committing=0
committed=0
published_bins=""
model_published=0
runtime_published=0
rollback_install() {
  [ "$committing" -eq 1 ] && [ "$committed" -eq 0 ] || return 0
  for bin in exocortex exocortex-mcp-client exocortex-node exocortex-worker; do
    old="$dest/.$bin.old.$$"
    case " $published_bins " in
      *" $bin "*)
        rm -f "$dest/$bin"
        [ ! -e "$old" ] || mv "$old" "$dest/$bin"
        ;;
      *) [ ! -e "$old" ] || mv "$old" "$dest/$bin" ;;
    esac
  done
  if [ "$model_published" -eq 1 ]; then
    rm -rf "$model_dest/$model_name"
    [ ! -e "$previous_model" ] || mv "$previous_model" "$model_dest/$model_name"
  elif [ -e "$previous_model" ]; then
    mv "$previous_model" "$model_dest/$model_name"
  fi
  if [ "${runtime_supported:-0}" -eq 1 ]; then
    if [ "$runtime_published" -eq 1 ]; then
      rm -rf "$runtime_dest"
      [ ! -e "$previous_runtime" ] || mv "$previous_runtime" "$runtime_dest"
    elif [ -e "$previous_runtime" ]; then
      mv "$previous_runtime" "$runtime_dest"
    fi
  fi
}
finish_install() {
  status=$?
  trap - EXIT INT TERM
  rollback_install
  rm -rf "$tmp"
  exit "$status"
}
trap finish_install EXIT INT TERM
echo "downloading $url"
curl_https "$url" -o "$tmp/$archive"
curl_https "$url.sha256" -o "$tmp/$archive.sha256"
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
[ -d "$src/models" ] || { echo "install refused: archive has no embedding model sidecar" >&2; exit 1; }
model_count=0
for model in "$src"/models/*; do
  [ -d "$model" ] || { echo "install refused: embedding model sidecar is empty" >&2; exit 1; }
  model_count=$((model_count + 1))
done
[ "$model_count" -eq 1 ] || { echo "install refused: archive must contain exactly one embedding model" >&2; exit 1; }
runtime_supported=0
case "$target" in
  aarch64-apple-darwin|x86_64-unknown-linux-gnu) runtime_supported=1 ;;
esac
if [ "$runtime_supported" -eq 1 ]; then
  [ -x "$src/standalone-runtime/redis-server" ] || { echo "install refused: archive has no standalone redis-server" >&2; exit 1; }
  [ -f "$src/standalone-runtime/falkordb.so" ] || { echo "install refused: archive has no standalone FalkorDB module" >&2; exit 1; }
  [ -f "$src/standalone-runtime/RUNTIME-MANIFEST.txt" ] || { echo "install refused: archive has no standalone runtime manifest" >&2; exit 1; }
fi
dest="${CARGO_HOME:-$HOME/.cargo}/bin"
mkdir -p "$dest"
model_dest="${CARGO_HOME:-$HOME/.cargo}/share/exocortex/models"
mkdir -p "$model_dest"
model_name="$(basename "$model")"
staged_model="$model_dest/.$model_name.new.$$"
previous_model="$model_dest/.$model_name.old.$$"
cp -R "$model" "$staged_model"
if [ "$runtime_supported" -eq 1 ]; then
  runtime_dest="${CARGO_HOME:-$HOME/.cargo}/share/exocortex/standalone"
  runtime_parent=$(dirname "$runtime_dest")
  mkdir -p "$runtime_parent"
  staged_runtime="$runtime_parent/.standalone.new.$$"
  previous_runtime="$runtime_parent/.standalone.old.$$"
  cp -R "$src/standalone-runtime" "$staged_runtime"
  chmod 0755 "$staged_runtime/redis-server"
  chmod 0555 "$staged_runtime/falkordb.so"
  "$staged_runtime/redis-server" --version >/dev/null
fi
for bin in exocortex exocortex-mcp-client exocortex-node exocortex-worker; do
  staged_bin="$dest/.$bin.new.$$"
  install -m 0755 "$src/$bin" "$staged_bin" 2>/dev/null || {
    cp "$src/$bin" "$staged_bin"
    chmod 0755 "$staged_bin"
  }
done

# Validate the exact staged executable/model pair before any live path changes.
EXOCORTEX_BGE_SMALL_MODEL_DIR="$staged_model" \
  "$dest/.exocortex-node.new.$$" --verify-embedder >/dev/null

committing=1
if [ -e "$model_dest/$model_name" ]; then
  mv "$model_dest/$model_name" "$previous_model"
fi
if [ "$runtime_supported" -eq 1 ] && [ -e "$runtime_dest" ]; then
  mv "$runtime_dest" "$previous_runtime"
fi
for bin in exocortex exocortex-mcp-client exocortex-node exocortex-worker; do
  [ ! -e "$dest/$bin" ] || mv "$dest/$bin" "$dest/.$bin.old.$$"
done
mv "$staged_model" "$model_dest/$model_name"
model_published=1
if [ "$runtime_supported" -eq 1 ]; then
  mv "$staged_runtime" "$runtime_dest"
  runtime_published=1
fi
for bin in exocortex exocortex-mcp-client exocortex-node exocortex-worker; do
  mv "$dest/.$bin.new.$$" "$dest/$bin"
  published_bins="$published_bins $bin"
done

# Exercise the installed share/exocortex resolver, not an environment override.
unset EXOCORTEX_BGE_SMALL_MODEL_DIR
"$dest/exocortex-node" --verify-embedder >/dev/null
committed=1
rm -rf "$previous_model"
if [ "$runtime_supported" -eq 1 ]; then
  rm -rf "$previous_runtime"
fi
for bin in exocortex exocortex-mcp-client exocortex-node exocortex-worker; do
  rm -f "$dest/.$bin.old.$$"
done
case ":$PATH:" in
  *":$dest:"*) ;;
  *) echo "note: add $dest to PATH" ;;
esac
if [ "$runtime_supported" -eq 1 ]; then
  echo "installed: exocortex binaries, verified model sidecar, and standalone Redis/Falkor runtime"
  echo "next: exocortex --mode mcp-standalone --org my-org --user me"
else
  echo "installed: exocortex binaries and verified model sidecar"
  echo "note: mcp-standalone is unavailable on macOS Intel; use mcp-client or backend-node"
fi
