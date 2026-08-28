#!/usr/bin/env bash
# Publish one immutable GitHub release asset set, or verify an identical retry.
set -euo pipefail

tag=${1:?usage: publish-release-assets.sh TAG REPOSITORY DIST_DIR}
repository=${2:?usage: publish-release-assets.sh TAG REPOSITORY DIST_DIR}
dist=${3:?usage: publish-release-assets.sh TAG REPOSITORY DIST_DIR}

local_assets=()
for asset in "$dist"/*; do
  [ -f "$asset" ] && local_assets+=("$asset")
done
if [ "${#local_assets[@]}" -eq 0 ]; then
  echo "release refused: no assets in $dist" >&2
  exit 1
fi

if gh release view "$tag" --repo "$repository" >/dev/null 2>&1; then
  compare_dir=$(mktemp -d)
  trap 'rm -rf "$compare_dir"' EXIT INT TERM
  remote_names=$(gh release view "$tag" --repo "$repository" --json assets --jq '.assets[].name')
  if [ -n "$remote_names" ]; then
    gh release download "$tag" --repo "$repository" --dir "$compare_dir"
  fi
  is_draft=$(gh release view "$tag" --repo "$repository" --json isDraft --jq '.isDraft')

  remote_count=0
  for asset in "$compare_dir"/*; do
    [ -f "$asset" ] && remote_count=$((remote_count + 1))
  done
  for remote_asset in "$compare_dir"/*; do
    [ -f "$remote_asset" ] || continue
    remote_name=$(basename "$remote_asset")
    local_asset="$dist/$remote_name"
    if [ ! -f "$local_asset" ] || ! cmp -s "$local_asset" "$remote_asset"; then
      echo "release refused: existing $tag asset $remote_name is not byte-identical" >&2
      exit 1
    fi
  done

  if [ "$is_draft" = true ]; then
    missing_assets=()
    for local_asset in "${local_assets[@]}"; do
      [ -f "$compare_dir/$(basename "$local_asset")" ] || missing_assets+=("$local_asset")
    done
    if [ "${#missing_assets[@]}" -gt 0 ]; then
      gh release upload "$tag" "${missing_assets[@]}" --repo "$repository"
    fi
    gh release edit "$tag" --repo "$repository" --draft=false
  elif [ "${#local_assets[@]}" -ne "$remote_count" ]; then
    echo "release refused: published $tag asset set differs" >&2
    exit 1
  fi
  echo "release $tag already exists with byte-identical assets"
  exit 0
fi

# Assets are attached while the release is private. Only a complete successful
# upload is made public; a failed create cannot overwrite a prior release.
gh release create "$tag" "${local_assets[@]}" --repo "$repository" \
  --draft --title "Exocortex $tag" --generate-notes
gh release edit "$tag" --repo "$repository" --draft=false
