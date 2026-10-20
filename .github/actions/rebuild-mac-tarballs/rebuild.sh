#!/usr/bin/env bash
# rebuild-mac-tarballs.sh
# Usage: rebuild-mac-tarballs.sh <artifact-dir>
#
# After Apple signing, standalone .pkg files are signed but *_mac_*.tgz archives
# still embed the pre-sign unsigned copies (created by pkg/package). Replace
# every .pkg inside each tarball with the matching signed .pkg from artifact-dir
# and rewrite the archive in place.

set -euo pipefail

# ---------------------------------------------------------------------------
# Arguments and globals
# ---------------------------------------------------------------------------

ARTIFACT_DIR="${1:?Usage: $0 <artifact-dir>}"

# ---------------------------------------------------------------------------
# Logging helpers (both write to stderr so they are safe inside $(...))
# ---------------------------------------------------------------------------

log() { echo "$*"         >&2; }
err() { echo "::error::$*" >&2; }

# ---------------------------------------------------------------------------
# find_tarballs <artifact-dir>
# Populates the caller's 'tarballs' array with all *_mac_*.tgz paths.
# Exits if none found.
# ---------------------------------------------------------------------------

find_tarballs() {
  local dir="$1"
  shopt -s nullglob
  tarballs=( "$dir"/*_mac_*.tgz )
  shopt -u nullglob

  if (( ${#tarballs[@]} == 0 )); then
    err "No *_mac_*.tgz files in '$dir'"
    exit 1
  fi

  log "Found ${#tarballs[@]} macOS distribution tarball(s)."
}

# ---------------------------------------------------------------------------
# extract_tarball <tgz> <work-dir> <base>
# Extracts <tgz> into <work-dir> and validates the expected top-level
# directory <work-dir>/<base> exists after extraction.
# ---------------------------------------------------------------------------

extract_tarball() {
  local tgz="$1" work="$2" base="$3"

  tar -xzf "$tgz" -C "$work"

  if [[ ! -d "$work/$base" ]]; then
    err "$(basename "$tgz") — expected top-level directory '$base' after extraction"
    return 1
  fi
}

# ---------------------------------------------------------------------------
# replace_pkgs <top-dir> <tgz-name> <artifact-dir>
# For every .pkg inside <top-dir>, copies the signed version from <artifact-dir>.
# Fails if no .pkg files are found or any signed copy is missing.
# ---------------------------------------------------------------------------

replace_pkgs() {
  local top="$1" tgz_name="$2" artifact_dir="$3"
  local pkgs=() name signed

  shopt -s nullglob
  pkgs=( "$top"/*.pkg )
  shopt -u nullglob

  if (( ${#pkgs[@]} == 0 )); then
    err "$tgz_name — no .pkg files found inside tarball"
    return 1
  fi

  for pkg in "${pkgs[@]}"; do
    name=$(basename "$pkg")
    signed="$artifact_dir/$name"

    if [[ ! -f "$signed" ]]; then
      err "$tgz_name — signed package '$name' not found in artifact dir"
      return 1
    fi

    cp -f "$signed" "$pkg"
    log "  replaced $name"
  done
}

# ---------------------------------------------------------------------------
# repack_tarball <tgz> <work-dir> <base>
# Repacks <work-dir>/<base> back into <tgz> in place.
# ---------------------------------------------------------------------------

repack_tarball() {
  local tgz="$1" work="$2" base="$3"
  tar -czf "$tgz" -C "$work" "$base"
}

# ---------------------------------------------------------------------------
# rebuild_one <tgz> <artifact-dir>
# Full rebuild cycle for a single tarball: extract → replace → repack → cleanup.
# ---------------------------------------------------------------------------

rebuild_one() {
  local tgz="$1" artifact_dir="$2"
  local tgz_name base work failed=0

  tgz_name=$(basename "$tgz")
  base=$(basename "$tgz" .tgz)
  work=$(mktemp -d)

  log "Processing $tgz_name ..."

  extract_tarball "$tgz" "$work" "$base"                    || failed=1
  [[ $failed -eq 0 ]] && replace_pkgs "$work/$base" "$tgz_name" "$artifact_dir" || failed=1
  [[ $failed -eq 0 ]] && repack_tarball "$tgz" "$work" "$base"                  || failed=1

  # Always clean up the temp dir — no trap needed.
  rm -rf "$work"

  [[ $failed -eq 0 ]] && log "Rebuilt $tgz_name with signed package(s)."
  return $failed
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  local tarballs=() rebuilt=0

  find_tarballs "$ARTIFACT_DIR"

  for tgz in "${tarballs[@]}"; do
    rebuild_one "$tgz" "$ARTIFACT_DIR"
    rebuilt=$(( rebuilt + 1 ))
  done

  log "Done. Rebuilt $rebuilt macOS distribution tarball(s)."
}

main
