#!/bin/sh
# Fetch the exact production embedding sidecar and verify every byte.
set -eu

revision="ea104dacec62c0de699686887e3f920caeb4f3e3"
directory="Xenova_bge-small-en-v1.5-$revision"
root="${1:?usage: fetch-embedding-model.sh MODEL_ROOT}"
destination="$root/$directory"
[ ! -e "$destination" ] || {
  echo "model fetch refused: destination already exists: $destination" >&2
  exit 1
}
mkdir -p "$root"
tmp="$(mktemp -d "$root/.model.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp/onnx"

fetch() {
  relative="$1"
  expected_size="$2"
  expected_sha256="$3"
  output="$tmp/$relative"
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location \
    "https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/$revision/$relative" \
    -o "$output"
  actual_size="$(wc -c < "$output" | tr -d ' ')"
  [ "$actual_size" = "$expected_size" ] || {
    echo "model fetch refused: $relative has $actual_size bytes, expected $expected_size" >&2
    exit 1
  }
  if command -v sha256sum >/dev/null 2>&1; then
    actual_sha256="$(sha256sum "$output" | cut -d' ' -f1)"
  elif command -v shasum >/dev/null 2>&1; then
    actual_sha256="$(shasum -a 256 "$output" | cut -d' ' -f1)"
  else
    echo "model fetch refused: sha256sum or shasum is required" >&2
    exit 1
  fi
  [ "$actual_sha256" = "$expected_sha256" ] || {
    echo "model fetch refused: $relative sha256 $actual_sha256, expected $expected_sha256" >&2
    exit 1
  }
}

fetch onnx/model.onnx 133093490 828e1496d7fabb79cfa4dcd84fa38625c0d3d21da474a00f08db0f559940cf35
fetch tokenizer.json 711396 d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66
fetch config.json 683 fa73f90bf92c8cace1fbcb709626306f2bdbc9ea3e5b5f94b440df9b6aa56350
fetch special_tokens_map.json 125 b6d346be366a7d1d48332dbc9fdf3bf8960b5d879522b7799ddba59e76237ee3
fetch tokenizer_config.json 366 9261e7d79b44c8195c1cada2b453e55b00aeb81e907a6664974b4d7776172ab3

mv "$tmp" "$destination"
trap - EXIT INT TERM
echo "verified embedding model: $destination"
