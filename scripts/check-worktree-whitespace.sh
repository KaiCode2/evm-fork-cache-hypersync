#!/usr/bin/env bash
set +x
set -euo pipefail

cd "$(dirname "$0")/.."
umask 077

temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
issues="$temporary_dir/issues"
one_file="$temporary_dir/one-file"
redacted_file="$temporary_dir/redacted-file"
: >"$issues"

# `git diff --check` sees only indexed/diffable state in an unborn repository.
# Compare every publication input with /dev/null so tracked and untracked files
# receive Git's standard trailing-space and space-before-tab validation.
while IFS= read -r -d '' path; do
  : >"$one_file"
  result=0
  git -c core.whitespace=trailing-space,space-before-tab \
    diff --no-index --check -- /dev/null "$path" >"$one_file" 2>&1 || result=$?
  # Git follows each diagnostic with the offending added line. Retain only the
  # path/line/reason header so a whitespace failure cannot echo file contents.
  sed -nE \
    '/:[0-9]+: (trailing whitespace|space before tab in indent|new blank line at EOF)\.$/p' \
    "$one_file" >"$redacted_file"
  if [[ -s "$redacted_file" ]]; then
    cat "$redacted_file" >>"$issues"
  elif [[ "$result" -gt 1 ]]; then
    printf '%s: whitespace inspection failed with status %s\n' \
      "$path" "$result" >>"$issues"
  fi
done < <(
  find . \
    \( -name .git -o -name target \) -prune -o \
    -type f ! -name .env -print0
)

if [[ -s "$issues" ]]; then
  echo "Whole-worktree whitespace check failed:" >&2
  cat "$issues" >&2
  exit 1
fi

echo "Whole-worktree whitespace check passed."
