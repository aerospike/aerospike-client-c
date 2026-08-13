#!/bin/bash
# Install C client API docs into the website repo (branch + draft PR).
#
# Usage:
#   publish-apidoc.sh <source> --website-repo <path> [options]
#
# Options:
#   --website-repo <path>  Website repo checkout (default: $WEBSITE_REPO)
#   --branch-name <name>   Branch to create (default: release-<ver>-apidoc)
#   --dry-run              Stage only; log next command to resume
#   --auto-approve         Skip commit and push+PR confirmations
#   --version <ver>        Override version (default: from artifact name)
#   --base-branch <branch> Branch to cut from (default: main)
#   --artifact-glob <glob> Artifact name match (default: api-docs-*)
#   -h, --help             Show this help

set -euo pipefail

readonly RUN_URL_RE='^https://github\.com/([^/]+/[^/]+)/actions/runs/([0-9]+)'

script_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
download_root="${PUBLISH_APIDOC_DOWNLOAD:-${XDG_CACHE_HOME:-$HOME/.cache}/aerospike-publish-apidoc}"

lang=c
base_branch=main
version=""
artifact_glob="api-docs-*"
branch_name=""
auto_approve=0
dry_run=0
website_repo="${WEBSITE_REPO:-}"
source_arg=""
scratch=""

tgz=""
match_name=""
resume_source=""
branch=""
trg_rel="apps/api/src/apidocs/${lang}"

confirm() {
  local prompt=$1
  if [[ $auto_approve -eq 1 ]]; then
    echo "$prompt [auto-approved: --auto-approve]"
    return 0
  fi
  read -r -p "$prompt [y/N] " reply
  [[ "$reply" =~ ^[Yy]$ ]]
}

abs_path() {
  local path=$1
  echo "$(cd "$(dirname "$path")" && pwd)/$(basename "$path")"
}

summarize_changes() {
  awk '
    NF { c[$1]++; total++ }
    END {
      printf "  Added:    %d\n", c["A"]+0
      printf "  Modified: %d\n", c["M"]+0
      printf "  Deleted:  %d\n", c["D"]+0
      printf "  Total:    %d files\n", total+0
    }
  '
}

locate_tgz() {
  local dir=$1 found zip unzip_dir
  found=$(find "$dir" -name '*.tgz' | head -1)
  [[ -n "$found" ]] && { echo "$found"; return 0; }

  zip=$(find "$dir" -name '*.zip' | head -1)
  [[ -z "$zip" ]] && return 0

  echo "Found a .zip instead of a .tgz ($zip) — unzipping to look inside..." >&2
  unzip_dir="$dir/_unzipped"
  mkdir -p "$unzip_dir"
  unzip -q "$zip" -d "$unzip_dir"
  find "$unzip_dir" -name '*.tgz' | head -1
}

prepare_download_dir() {
  local dir=$1
  if [[ -d "$dir" ]]; then
    echo "Removing existing download at $dir"
    rm -rf "$dir"
  fi
  mkdir -p "$dir"
}

cleanup_scratch() {
  [[ -n "$scratch" ]] && rm -rf "$scratch"
}

pick_artifact_name() {
  local artifacts_json=$1
  echo "$artifacts_json" | python3 -c "
import json, sys, fnmatch
data = json.load(sys.stdin)
pattern = sys.argv[1]
names = [a['name'] for a in data.get('artifacts', []) if fnmatch.fnmatch(a['name'], pattern)]
print(names[0] if names else '')
" "$artifact_glob"
}

list_artifacts() {
  local artifacts_json=$1
  echo "$artifacts_json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for a in data.get('artifacts', []):
    print(' -', a['name'])
"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --website-repo) website_repo=$2; shift 2 ;;
      --branch-name) branch_name=$2; shift 2 ;;
      --dry-run) dry_run=1; shift ;;
      --auto-approve) auto_approve=1; shift ;;
      --version) version=$2; shift 2 ;;
      --base-branch) base_branch=$2; shift 2 ;;
      --artifact-glob) artifact_glob=$2; shift 2 ;;
      -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
      *)
        if [[ -z "$source_arg" ]]; then
          source_arg=$1; shift
        else
          echo "Unexpected argument: $1" >&2; exit 2
        fi
        ;;
    esac
  done
}

validate_inputs() {
  if [[ -z "$source_arg" ]]; then
    echo "Usage: $0 <local-tgz-path-or-github-actions-run-url> --website-repo <path> [options]" >&2
    exit 2
  fi
  if [[ -z "$website_repo" ]]; then
    echo "No website repo path given. Pass --website-repo <path> or set \$WEBSITE_REPO." >&2
    exit 2
  fi
  if [[ ! -d "$website_repo/.git" ]]; then
    echo "Not a git repo: $website_repo" >&2
    exit 1
  fi
}

resolve_from_run_url() {
  local gh_repo=$1 run_id=$2 artifacts_json download_dir

  echo "== Source is a GitHub Actions run =="
  echo "   repo: $gh_repo"
  echo "   run:  $run_id"

  echo "Looking up artifacts on that run..."
  artifacts_json=$(gh api "repos/$gh_repo/actions/runs/$run_id/artifacts" --paginate)
  match_name=$(pick_artifact_name "$artifacts_json")
  if [[ -z "$match_name" ]]; then
    echo "No artifact matching '$artifact_glob' found on run $run_id in $gh_repo." >&2
    echo "Available artifacts:" >&2
    list_artifacts "$artifacts_json" >&2
    exit 1
  fi

  echo ""
  echo "Found this api-doc artifact:"
  echo "  name: $match_name"
  echo "  run:  https://github.com/$gh_repo/actions/runs/$run_id"
  echo ""

  if ! confirm "Download this artifact and proceed?"; then
    echo "Aborted by user."
    exit 1
  fi

  download_dir="$download_root/$match_name"
  prepare_download_dir "$download_dir"
  echo "Downloading to $download_dir"
  gh run download "$run_id" --repo "$gh_repo" --name "$match_name" --dir "$download_dir"

  tgz=$(locate_tgz "$download_dir")
  if [[ -z "$tgz" ]]; then
    echo "Downloaded artifact '$match_name' did not contain a .tgz file (directly or inside a .zip):" >&2
    find "$download_dir" >&2
    exit 1
  fi

  resume_source="$tgz"
  echo "Using: $tgz"
}

resolve_from_local_file() {
  local local_source

  if [[ ! -f "$source_arg" ]]; then
    echo "No such file: $source_arg" >&2
    exit 1
  fi

  echo "== Source is a local file: $source_arg =="

  if [[ "$source_arg" == *.tgz ]]; then
    tgz=$(abs_path "$source_arg")
    resume_source="$tgz"
    echo "Using: $tgz"
  else
    scratch=$(mktemp -d)
    cp "$source_arg" "$scratch"/
    tgz=$(locate_tgz "$scratch")
    if [[ -z "$tgz" ]]; then
      echo "No .tgz found at $source_arg (directly or inside a .zip)." >&2
      exit 1
    fi
    local_source=$(abs_path "$source_arg")
    resume_source="$local_source"
    echo "Using: $tgz (from $local_source)"
  fi

  match_name=$(basename "$tgz" .tgz)
}

resolve_source() {
  if [[ "$source_arg" =~ $RUN_URL_RE ]]; then
    resolve_from_run_url "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
  else
    resolve_from_local_file
  fi
}

derive_version() {
  if [[ -n "$version" ]]; then
    return 0
  fi
  if [[ -z "$match_name" ]]; then
    echo "Could not determine artifact name. Pass --version <ver> explicitly." >&2
    exit 1
  fi
  if [[ "$match_name" == api-docs-* ]]; then
    version="${match_name#api-docs-}"
  else
    version="$match_name"
  fi
  if [[ -z "$version" ]]; then
    echo "Could not parse a version out of '$match_name'. Pass --version <ver> explicitly." >&2
    exit 1
  fi
}

validate_tgz() {
  local tar_listing
  echo "== Validate $tgz =="
  if ! tar_listing=$(tar tf "$tgz" 2>/dev/null) || [[ -z "$tar_listing" ]]; then
    echo "Not a readable tar archive or archive is empty: $tgz" >&2
    exit 1
  fi
}

reset_repo_if_dirty() {
  if [[ -z "$(git status --porcelain)" ]]; then
    return 0
  fi

  echo "Repo is not clean — attempting to reset it to a clean state:" >&2
  git status --short >&2
  git restore --source=HEAD --staged --worktree -- . || true
  git clean -fd -- . || true

  if [[ -n "$(git status --porcelain)" ]]; then
    echo "Could not get the repo to a clean state — resolve manually and re-run:" >&2
    git status --short >&2
    exit 1
  fi
  echo "Repo reset to a clean state."
}

prepare_website_repo() {
  echo "== Prepare $website_repo =="
  cd "$website_repo"
  reset_repo_if_dirty

  branch="${branch_name:-release-${version}-apidoc}"

  echo "== Checkout + pull $base_branch =="
  git fetch origin "$base_branch"
  git checkout "$base_branch"
  git merge --ff-only "origin/$base_branch"
}

checkout_release_branch() {
  echo "== Create branch $branch (from $base_branch) =="
  if git show-ref --verify --quiet "refs/heads/$branch"; then
    if git merge-base --is-ancestor "$branch" "$base_branch"; then
      echo "Branch $branch already exists but has no commits yet (likely a previous --dry-run) — reusing it."
      git checkout "$branch"
    else
      echo "Branch $branch already exists locally with real commits. Delete it yourself if it's stale, or pass --branch-name <name> to use a different one." >&2
      exit 1
    fi
  else
    git checkout -b "$branch"
  fi
}

install_docs() {
  local trg="$website_repo/$trg_rel" stray_tgz

  echo "== Replace $trg_rel =="
  mkdir -p "$trg"
  rm -rf "${trg:?}"/*
  tar xf "$tgz" -C "$trg"

  stray_tgz=$(find "$trg" -name '*.tgz' -print)
  if [[ -n "$stray_tgz" ]]; then
    echo "Removing stray .tgz file(s) found in extracted docs:" >&2
    echo "$stray_tgz" >&2
    find "$trg" -name '*.tgz' -delete
  fi

  git add "$trg_rel"
}

show_staged_summary() {
  local changes
  changes=$(git diff --cached --name-status -- "$trg_rel")
  if [[ -z "$changes" ]]; then
    echo "No changes detected under $trg_rel (docs identical to what's already committed)."
    exit 0
  fi

  echo "== Summary =="
  echo "$changes" | summarize_changes
}

log_dry_run_resume() {
  local resume_cmd
  resume_cmd="$script_path \"$resume_source\" --website-repo \"$website_repo\""
  [[ -n "$branch_name" ]] && resume_cmd+=" --branch-name \"$branch_name\""
  [[ "$base_branch" != "main" ]] && resume_cmd+=" --base-branch \"$base_branch\""

  echo "== Dry run =="
  echo "Staged on branch $branch in $website_repo (not committed)."
  echo "Source: $resume_source"
  echo "Next command (no re-download):"
  echo "  $resume_cmd"
}

commit_changes() {
  if ! confirm "Commit these changes on branch $branch?"; then
    echo "Left staged, uncommitted, on branch $branch."
    exit 0
  fi
  git commit -m "${lang} client ${version} api docs"
  echo "Committed on branch $branch."
}

push_and_open_pr() {
  if ! confirm "Push branch $branch to origin and open a DRAFT PR against $base_branch?"; then
    echo "Committed locally only. Branch $branch was not pushed."
    exit 0
  fi

  git push -u origin "$branch"
  gh pr create --draft \
    --base "$base_branch" \
    --head "$branch" \
    --title "${lang} client ${version} api docs" \
    --body "Update api-docs."

  echo "Done. Draft PR opened for branch $branch."
}

main() {
  parse_args "$@"
  validate_inputs
  trap cleanup_scratch EXIT

  resolve_source
  derive_version
  echo "Version: $version"
  validate_tgz

  prepare_website_repo
  checkout_release_branch
  install_docs
  show_staged_summary

  if [[ $dry_run -eq 1 ]]; then
    log_dry_run_resume
    return 0
  fi

  commit_changes
  push_and_open_pr
}

main "$@"
