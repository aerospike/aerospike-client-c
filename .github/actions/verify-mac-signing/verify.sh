#!/usr/bin/env bash
# verify-mac-signing.sh
# Usage: verify-mac-signing.sh <signed-dir> <expected-count>
#
# Verifies every .pkg in <signed-dir> is Apple-signed and Gatekeeper-accepted.
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

# verify_pkg <pkg>
# Runs both checks. Returns 1 if either fails.
verify_pkg() {
  local pkg="$1" failed=0
  check_signature  "$pkg" || failed=1
  check_gatekeeper "$pkg" || failed=1
  return $failed
}

# ---------------------------------------------------------------------------
# Count validation
# ---------------------------------------------------------------------------

# validate_count <pkg_files_array_name> <expected>
validate_count() {
  local -n _files=$1
  local expected=$2 actual=${#_files[@]}

  [[ -f "${_files[0]}" ]] \
    || { err "No .pkg files in '$SIGNED_DIR' — signing produced no output."; return 1; }

  [[ $actual -eq $expected ]] \
    || { err "Count mismatch — expected $expected, got $actual."; return 1; }

  log "✅ Count: $actual / $expected"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  local pkg_files=( "$SIGNED_DIR"/*.pkg ) failed=0

  validate_count pkg_files "$EXPECTED_COUNT"

  for pkg in "${pkg_files[@]}"; do
    if verify_pkg "$pkg"; then
      pass "$(basename "$pkg")"
    else
      failed=1
    fi
  done

  [[ $failed -eq 0 ]] || { err "One or more .pkg files failed verification."; exit 1; }
  log "All ${#pkg_files[@]} .pkg files signed and notarized."
}

main
