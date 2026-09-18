#!/usr/bin/env bash
# rebuild-mac-tarballs.sh
# Usage: rebuild-mac-tarballs.sh <artifact-dir>
#
# After Apple signing, standalone .pkg files are signed but *_mac_*.tgz archives
# still embed the pre-sign unsigned copies (created by pkg/package). Replace
# every .pkg inside each tarball with the matching signed .pkg from artifact-dir
# and rewrite the archive in place.

set -euo pipefail

ARTIFACT_DIR="${1:?Usage: $0 <artifact-dir>}"

log()  { echo "$*"; }
err()  { echo "::error::$*"; }

shopt -s nullglob
tarballs=( "$ARTIFACT_DIR"/*_mac_*.tgz )

if (( ${#tarballs[@]} == 0 )); then
  err "No *_mac_*.tgz files in '$ARTIFACT_DIR'"
  exit 1
fi

rebuilt=0
for tgz in "${tarballs[@]}"; do
  base=$(basename "$tgz" .tgz)
  work=$(mktemp -d)

  tar -xzf "$tgz" -C "$work"
  top="$work/$base"
  if [[ ! -d "$top" ]]; then
    err "$(basename "$tgz") — expected top-level directory '$base'"
    rm -rf "$work"
    exit 1
  fi

  pkgs=( "$top"/*.pkg )
  if (( ${#pkgs[@]} == 0 )); then
    err "$(basename "$tgz") — no .pkg files inside"
    rm -rf "$work"
    exit 1
  fi

  for pkg in "${pkgs[@]}"; do
    name=$(basename "$pkg")
    signed="$ARTIFACT_DIR/$name"
    if [[ ! -f "$signed" ]]; then
      err "$(basename "$tgz") — missing signed package '$name' in artifact dir"
      rm -rf "$work"
      exit 1
    fi
    cp -f "$signed" "$pkg"
    log "  replaced $name"
  done

  tar -czf "$tgz" -C "$work" "$base"
  rm -rf "$work"

  log "Rebuilt $(basename "$tgz") with ${#pkgs[@]} signed package(s)"
  rebuilt=$((rebuilt + 1))
done

log "Rebuilt $rebuilt macOS distribution tarball(s)."
