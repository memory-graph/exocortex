#!/usr/bin/env bash
set -euo pipefail

readonly PROTOC_VERSION=28.3
readonly RELEASE_ROOT="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}"

if [[ $# -ne 1 || -z "$1" ]]; then
  echo "usage: $0 INSTALL_ROOT" >&2
  exit 64
fi

readonly install_root=$1
if [[ -e "$install_root" ]]; then
  echo "refusing to install protoc over existing path: $install_root" >&2
  exit 1
fi

case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    readonly platform=linux-x86_64
    readonly expected_sha256=0ad949f04a6a174da83cdcbdb36dee0a4925272a5b6d83f79a6bf9852076d53f
    ;;
  Linux:aarch64 | Linux:arm64)
    readonly platform=linux-aarch_64
    readonly expected_sha256=1de522032a8b194002fe35cab86d747848238b5e4de4f99648372079f5b46f9a
    ;;
  Darwin:arm64 | Darwin:aarch64)
    readonly platform=osx-aarch_64
    readonly expected_sha256=92ceefda6a7293ec014e6ecac82d64719357145cb6fc2865badadeb5e62c0431
    ;;
  Darwin:x86_64)
    readonly platform=osx-x86_64
    readonly expected_sha256=97fe5d442090b4dbc23cd1384fb9b444fa1dc6e67d15bb5e1fe4de0da7638b20
    ;;
  *)
    echo "unsupported protoc installer host: $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

readonly archive="protoc-${PROTOC_VERSION}-${platform}.zip"
readonly download_url="${RELEASE_ROOT}/${archive}"
work_dir=$(mktemp -d)
readonly work_dir
trap 'rm -rf "$work_dir"' EXIT

curl --proto '=https' --tlsv1.2 --fail --location --silent --show-error \
  "$download_url" --output "$work_dir/$archive"

if command -v sha256sum >/dev/null 2>&1; then
  actual_sha256=$(sha256sum "$work_dir/$archive" | awk '{print $1}')
else
  actual_sha256=$(shasum -a 256 "$work_dir/$archive" | awk '{print $1}')
fi
readonly actual_sha256
if [[ "$actual_sha256" != "$expected_sha256" ]]; then
  echo "protoc archive checksum mismatch: expected $expected_sha256, got $actual_sha256" >&2
  exit 1
fi

mkdir -p "$install_root"
unzip -q "$work_dir/$archive" -d "$install_root"
installed_version=$("$install_root/bin/protoc" --version)
if [[ "$installed_version" != "libprotoc ${PROTOC_VERSION}" ]]; then
  echo "unexpected installed protoc version: $installed_version" >&2
  exit 1
fi
