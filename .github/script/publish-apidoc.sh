#!/bin/bash
# Installs a generated C client API doc tarball into the website repo on a new
# release branch. Asks before committing, and again before pushing + opening a
# draft PR. --auto-approve skips both prompts; --dry-run stops right after
# staging the change (no commit, no push, no PR).
#
# TODO: this is a manual stopgap. Replace with a shared GitHub Actions workflow
# once cross-repo (client -> website) automation is supported.
#
# Source: a local .tgz, a local .zip containing one (e.g. downloaded by hand
# from the GitHub Actions UI), or a GitHub Actions run URL — the script finds
# the api-docs-* artifact on that run and asks before downloading it.
#
# Usage:
#   publish-apidoc.sh <source> --website-repo <path> [options]
#
# Options:
#   --website-repo <path>  Website repo checkout (default: $WEBSITE_REPO)
#   --branch-name <name>   Branch to create (default: release-<ver>-apidoc).
#                           Fails if it already exists locally — pass a
#                           different name here to run again without colliding.
#   --dry-run              Stage the doc change and show the summary, but
#                           don't commit, push, or open a PR.
#   --auto-approve         Skip the commit and push+PR confirmations
#   --version <ver>        Override version (default: parsed from artifact/file name)
#   --base-branch <branch> Branch to cut from (default: main)
#   --artifact-glob <glob> Artifact name match (default: api-docs-*)
#   -h, --help             Show this help

set -euo pipefail

lang=c   # this script lives in aerospike-client-c's own repo, so it only ever targets the C docs
base_branch=main
version=""
artifact_glob="api-docs-*"
branch_name=""
auto_approve=0
dry_run=0
website_repo="${WEBSITE_REPO:-}"
source_arg=""

confirm() {
  # confirm "question" -> 0 if yes, 1 if no
  local prompt=$1
  if [[ $auto_approve -eq 1 ]]; then
    echo "$prompt [auto-approved: --auto-approve]"
    return 0
  fi
  read -r -p "$prompt [y/N] " reply
  [[ "$reply" =~ ^[Yy]$ ]]
}

# Reads name-status lines ("X<whitespace>path", e.g. from `git diff --name-status`)
# from stdin and prints a concise Added/Modified/Deleted/Total summary.
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

workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT

tgz=""

# Find a .tgz directly under $1, or unzip the first .zip found there and look
# again inside it. Prints the found .tgz path, or nothing if none was found.
locate_tgz() {
  local dir=$1
  local found
  found=$(find "$dir" -name '*.tgz' | head -1)
  if [[ -n "$found" ]]; then
    echo "$found"
    return 0
  fi

  local zip
  zip=$(find "$dir" -name '*.zip' | head -1)
  if [[ -z "$zip" ]]; then
    return 0
  fi

  echo "Found a .zip instead of a .tgz ($zip) — unzipping to look inside..." >&2
  local unzip_dir
  unzip_dir="$dir/_unzipped"
  mkdir -p "$unzip_dir"
  unzip -q "$zip" -d "$unzip_dir"
  find "$unzip_dir" -name '*.tgz' | head -1
}

# ---------------------------------------------------------------------------
# Step 1: resolve the source into a local .tgz path.
# ---------------------------------------------------------------------------
run_url_re='^https://github\.com/([^/]+/[^/]+)/actions/runs/([0-9]+)'

if [[ "$source_arg" =~ $run_url_re ]]; then
  gh_repo="${BASH_REMATCH[1]}"
  run_id="${BASH_REMATCH[2]}"

  echo "== Source is a GitHub Actions run =="
  echo "   repo: $gh_repo"
  echo "   run:  $run_id"

  echo "Looking up artifacts on that run..."
  artifacts_json=$(gh api "repos/$gh_repo/actions/runs/$run_id/artifacts" --paginate)
  match_name=$(echo "$artifacts_json" | python3 -c "
import json, sys, fnmatch
data = json.load(sys.stdin)
pattern = sys.argv[1]
names = [a['name'] for a in data.get('artifacts', []) if fnmatch.fnmatch(a['name'], pattern)]
print(names[0] if names else '')
" "$artifact_glob")

  if [[ -z "$match_name" ]]; then
    echo "No artifact matching '$artifact_glob' found on run $run_id in $gh_repo." >&2
    echo "Available artifacts:" >&2
    echo "$artifacts_json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for a in data.get('artifacts', []):
    print(' -', a['name'])
" >&2
    exit 1
  fi

  run_page_url="https://github.com/$gh_repo/actions/runs/$run_id"
  echo ""
  echo "Found this api-doc artifact:"
  echo "  name: $match_name"
  echo "  run:  $run_page_url"
  echo ""

  if ! confirm "Download this artifact and proceed?"; then
    echo "Aborted by user."
    exit 1
  fi

  download_dir="$workdir/download"
  mkdir -p "$download_dir"
  gh run download "$run_id" --repo "$gh_repo" --name "$match_name" --dir "$download_dir"

  tgz=$(locate_tgz "$download_dir")
  if [[ -z "$tgz" ]]; then
    echo "Downloaded artifact '$match_name' did not contain a .tgz file (directly or inside a .zip):" >&2
    find "$download_dir" >&2
    exit 1
  fi

else
  # Treat as a local path: either a .tgz directly, or a .zip (e.g. downloaded by
  # hand from the GitHub Actions UI) containing one. Fail fast, no prompting.
  if [[ ! -f "$source_arg" ]]; then
    echo "No such file: $source_arg" >&2
    exit 1
  fi
  echo "== Source is a local file: $source_arg =="

  local_dir="$workdir/local-source"
  mkdir -p "$local_dir"
  cp "$source_arg" "$local_dir"/

  tgz=$(locate_tgz "$local_dir")
  if [[ -z "$tgz" ]]; then
    echo "No .tgz found at $source_arg (directly or inside a .zip)." >&2
    exit 1
  fi
  match_name=$(basename "$tgz" .tgz)
  echo "Using: $tgz"
fi

# ---------------------------------------------------------------------------
# Step 2: derive version.
# ---------------------------------------------------------------------------
if [[ -z "$version" ]]; then
  version=$(echo "$match_name" | sed -E 's/^api-docs-([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
  if [[ "$version" == "$match_name" ]]; then
    echo "Could not parse a version out of '$match_name'. Pass --version <ver> explicitly." >&2
    exit 1
  fi
fi
echo "Version: $version"

# ---------------------------------------------------------------------------
# Step 3: validate the tarball, without touching the repo yet.
# (release_apidoc extracts straight into place with no upfront check, which is
#  what let a bad/empty archive wipe the target with nothing to replace it.
#  Validate first here instead.)
# ---------------------------------------------------------------------------
echo "== Validate $tgz =="
tar_listing=$(tar tf "$tgz" 2>/dev/null) || { echo "Not a readable tar archive: $tgz" >&2; exit 1; }
if [[ -z "$tar_listing" ]]; then
  echo "Archive is empty — refusing to continue." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 4: prepare repo — force a clean state, checkout base branch, pull
# latest, create the target branch (fails if it already exists).
# ---------------------------------------------------------------------------
echo "== Prepare $website_repo =="
cd "$website_repo"

if [[ -n "$(git status --porcelain)" ]]; then
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
fi

branch="${branch_name:-release-${version}-apidoc}"
trg_rel="apps/api/src/apidocs/${lang}"

echo "== Checkout + pull $base_branch =="
git fetch origin "$base_branch"
git checkout "$base_branch"
git merge --ff-only "origin/$base_branch"

echo "== Create branch $branch (from $base_branch) =="
if git show-ref --verify --quiet "refs/heads/$branch"; then
  if git merge-base --is-ancestor "$branch" "$base_branch"; then
    # Branch has contributed no commits beyond base_branch — most likely a
    # previous --dry-run's leftover (checking hash equality instead would
    # break here if base_branch has since moved forward on its own).
    # Safe to reuse: Step 5 below overwrites its staged content regardless.
    echo "Branch $branch already exists but has no commits yet (likely a previous --dry-run) — reusing it."
    git checkout "$branch"
  else
    echo "Branch $branch already exists locally with real commits. Delete it yourself if it's stale, or pass --branch-name <name> to use a different one." >&2
    exit 1
  fi
else
  git checkout -b "$branch"
fi

# ---------------------------------------------------------------------------
# Step 5: place docs.
# ---------------------------------------------------------------------------
trg="$website_repo/$trg_rel"
echo "== Replace $trg_rel =="
# Same two moves as release_apidoc: wipe the old tree, extract the new one straight
# into place. No intermediate copy step, so there's no "copy the wrong thing" gap.
mkdir -p "$trg"
rm -rf "${trg:?}"/*
tar xf "$tgz" -C "$trg"

# The doc tree should never itself contain a .tgz (that would mean the raw archive
# ended up in there instead of its contents). .tgz is also repo-gitignored, so
# `git add` would silently skip it, leaving a deletion-only commit. Guard against it.
stray_tgz=$(find "$trg" -name '*.tgz')
if [[ -n "$stray_tgz" ]]; then
  echo "Removing stray .tgz file(s) found in extracted docs (not part of the doc tree):" >&2
  echo "$stray_tgz" >&2
  find "$trg" -name '*.tgz' -print0 | xargs -0 rm -f
fi

git add "$trg_rel"

changes=$(git diff --cached --name-status -- "$trg_rel")
if [[ -z "$changes" ]]; then
  echo "No changes detected under $trg_rel (docs identical to what's already committed)."
  exit 0
fi

echo "== Summary =="
echo "$changes" | summarize_changes

if [[ $dry_run -eq 1 ]]; then
  echo "Dry run: changes staged on branch $branch, not committed. Re-run without --dry-run to commit/push."
  exit 0
fi

# ---------------------------------------------------------------------------
# Step 6: commit.
# ---------------------------------------------------------------------------
if ! confirm "Commit these changes on branch $branch?"; then
  echo "Left staged, uncommitted, on branch $branch."
  exit 0
fi
git commit -m "${lang} client ${version} api docs"
echo "Committed on branch $branch."

# ---------------------------------------------------------------------------
# Step 7: push + draft PR.
# ---------------------------------------------------------------------------
if ! confirm "Push branch $branch to origin and open a DRAFT PR against $base_branch?"; then
  echo "Committed locally only. Branch $branch was not pushed."
  exit 0
fi

git push -u origin "$branch"
gh pr create --draft \
  --base "$base_branch" \
  --head "$branch" \
  --title "${lang} client ${version} api docs" \
  --body "Automated api-doc update. Source: $source_arg"

echo "Done. Draft PR opened for branch $branch."
