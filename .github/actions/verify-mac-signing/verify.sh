#!/usr/bin/env bash
# verify-mac-signing.sh
# Usage: verify-mac-signing.sh <signed-dir> <expected-count>
#
# Verifies every loose .pkg in <signed-dir> is Apple-signed and Gatekeeper-accepted,
# and that every .pkg embedded in *_mac_*.tgz matches those signed copies.
# Fails if signed count != expected-count or any package fails either check.

set -euo pipefail

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------

SIGNED_DIR="${1:?Usage: $0 <signed-dir> <expected-count>}"
EXPECTED_COUNT="${2:?Usage: $0 <signed-dir> <expected-count>}"

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

log()  { echo "$*"; }
err()  { echo "::error::$*"; }
pass() { echo "  ✅ $*"; }

# ---------------------------------------------------------------------------
# Per-package checks
# ---------------------------------------------------------------------------

# check_signature <pkg>
# Returns 1 if pkgutil reports "no signature".
# Note: pkgutil exits non-zero for unsigned packages — capture output before grepping.
check_signature() {
  local pkg="$1" out
  out=$(pkgutil --check-signature "$pkg" 2>&1) || true
  if echo "$out" | grep -q "no signature"; then
    err "$(basename "$pkg") — no signature (Apple signing failed silently)"
    return 1
  fi
}

# check_gatekeeper <pkg>
# Returns 1 if Gatekeeper does not accept the package.
check_gatekeeper() {
  local pkg="$1"
  if ! spctl --assess --verbose --type install "$pkg" 2>&1 | grep -q "accepted"; then
    err "$(basename "$pkg") — Gatekeeper rejected (not notarized or wrong cert)"
    return 1
  fi
}

# verify_pkg <pkg> [label]
# Runs both checks. Returns 1 if either fails.
verify_pkg() {
  local pkg="$1" label="${2:-$(basename "$pkg")}" failed=0
  check_signature  "$pkg" || failed=1
  check_gatekeeper "$pkg" || failed=1
  if [[ $failed -eq 0 ]]; then
    pass "$label"
  fi
  return $failed
}

# ---------------------------------------------------------------------------
# Count validation
# ---------------------------------------------------------------------------

# validate_count <expected> <actual> <first_file>
# Avoids bash 4.3+ nameref (local -n) for macOS bash 3.2 compat.
validate_count() {
  local expected=$1 actual=$2 first_file=$3

  [[ -f "$first_file" ]] \
    || { err "No .pkg files in '$SIGNED_DIR' — signing produced no output."; return 1; }

  [[ $actual -eq $expected ]] \
    || { err "Count mismatch — expected $expected, got $actual."; return 1; }

  log "✅ Count: $actual / $expected"
}

# ---------------------------------------------------------------------------
# Tarball checks — catch unsigned pkgs still embedded after make package
# ---------------------------------------------------------------------------

# verify_tarballs
# Extracts each *_mac_*.tgz and runs the same checks on embedded .pkg files.
verify_tarballs() {
  local tgz work top pkg name failed=0
  local tarballs=( "$SIGNED_DIR"/*_mac_*.tgz )

  # Bash 3.2: unmatched glob stays literal when nullglob is off.
  if [[ ${#tarballs[@]} -eq 1 && ! -f "${tarballs[0]}" ]]; then
    err "No *_mac_*.tgz in '$SIGNED_DIR' — distribution archives missing."
    return 1
  fi

  log "Checking ${#tarballs[@]} distribution tarball(s)..."

  for tgz in "${tarballs[@]}"; do
    work=$(mktemp -d)
    tar -xzf "$tgz" -C "$work"
    # pkg/package uses basename-without-.tgz as the top-level directory.
    top="$work/$(basename "$tgz" .tgz)"
    if [[ ! -d "$top" ]]; then
      err "$(basename "$tgz") — expected top-level directory '$(basename "$tgz" .tgz)'"
      rm -rf "$work"
      failed=1
      continue
    fi

    local found=0
    for pkg in "$top"/*.pkg; do
      [[ -f "$pkg" ]] || continue
      found=1
      name=$(basename "$pkg")
      if ! verify_pkg "$pkg" "$(basename "$tgz")/$name"; then
        failed=1
      fi
    done

    if [[ $found -eq 0 ]]; then
      err "$(basename "$tgz") — no .pkg files inside"
      failed=1
    fi
    rm -rf "$work"
  done

  return $failed
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  local pkg_files=( "$SIGNED_DIR"/*.pkg ) failed=0

  validate_count "$EXPECTED_COUNT" "${#pkg_files[@]}" "${pkg_files[0]}"

  log "Checking loose .pkg files..."
  for pkg in "${pkg_files[@]}"; do
    verify_pkg "$pkg" || failed=1
  done

  verify_tarballs || failed=1

  [[ $failed -eq 0 ]] || { err "One or more .pkg files failed verification."; exit 1; }
  log "All loose .pkg files and tarball contents signed and notarized."
}

main
