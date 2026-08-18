#!/usr/bin/env bash
set +x
set -euo pipefail

cd "$(dirname "$0")/.."
umask 077

if [[ ! -f .env ]]; then
  echo "Exact-value leak check requires a local .env file." >&2
  exit 1
fi

read_dotenv_value() {
  local name="$1"
  awk -v name="$name" '
    /^[[:space:]]*(#|$)/ { next }
    {
      line = $0
      sub(/\r$/, "", line)
      sub(/^[[:space:]]*export[[:space:]]+/, "", line)
      equals = index(line, "=")
      if (equals == 0) {
        next
      }
      key = substr(line, 1, equals - 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
      if (key != name) {
        next
      }
      value = substr(line, equals + 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      first = substr(value, 1, 1)
      last = substr(value, length(value), 1)
      if (length(value) >= 2 && ((first == "\"" && last == "\"") ||
          (first == "\047" && last == "\047"))) {
        value = substr(value, 2, length(value) - 2)
      }
      found += 1
      selected = value
    }
    END {
      if (found != 1) {
        exit 1
      }
      print selected
    }
  ' .env
}

envio_api_token="$(read_dotenv_value ENVIO_API_TOKEN)" || {
  echo "ENVIO_API_TOKEN must occur exactly once in .env." >&2
  exit 1
}
rpc_url="$(read_dotenv_value RPC_URL)" || {
  echo "RPC_URL must occur exactly once in .env." >&2
  exit 1
}
if [[ -z "$envio_api_token" || -z "$rpc_url" ]]; then
  echo "ENVIO_API_TOKEN and RPC_URL must both be non-empty." >&2
  exit 1
fi
if [[ "$envio_api_token" == *$'\n'* || "$envio_api_token" == *$'\r'* ||
      "$rpc_url" == *$'\n'* || "$rpc_url" == *$'\r'* ]]; then
  echo "Multiline secret values are not supported by this check." >&2
  exit 1
fi

temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
patterns="$temporary_dir/patterns"
worktree_paths="$temporary_dir/worktree-paths"
commits="$temporary_dir/commits"
one_path="$temporary_dir/one-path"
history_paths="$temporary_dir/history-paths"
history_content_matches="$temporary_dir/history-content-matches"
history_name_matches="$temporary_dir/history-name-matches"
history_matches="$temporary_dir/history-matches"
printf '%s\n%s\n' "$envio_api_token" "$rpc_url" >"$patterns"
: >"$worktree_paths"
: >"$commits"
: >"$one_path"
: >"$history_paths"
: >"$history_content_matches"
: >"$history_name_matches"
: >"$history_matches"

# grep receives secrets through a mode-0600 pattern file, never through argv,
# stdout, a filename report, or a matching-line report. Paths remain
# NUL-delimited internally and only aggregate counts leave the script.
if ! find . \
  \( -name .git -o -name target \) -prune -o \
  -type f ! -name .env -print0 >"$worktree_paths" 2>/dev/null
then
  echo "Unable to enumerate worktree files for the exact-value scan." >&2
  exit 1
fi

worktree_count=0
while IFS= read -r -d '' path; do
  matched=0
  printf '%s\0' "$path" >"$one_path"
  if grep -a -z -q -F -f "$patterns" -- "$one_path" 2>/dev/null; then
    matched=1
  else
    status=$?
    if [[ "$status" -ne 1 ]]; then
      echo "Unable to scan one worktree filename." >&2
      exit "$status"
    fi
  fi
  if ((matched == 0)); then
    if grep -a -q -F -f "$patterns" -- "$path" 2>/dev/null; then
      matched=1
    else
      status=$?
      if [[ "$status" -ne 1 ]]; then
        echo "Unable to scan one worktree file." >&2
        exit "$status"
      fi
    fi
  fi
  if ((matched == 1)); then
    ((worktree_count += 1))
  fi
done <"$worktree_paths"

history_count=0
if git rev-parse --verify HEAD >/dev/null 2>&1; then
  if ! git rev-list --all >"$commits" 2>/dev/null; then
    echo "Unable to enumerate Git history for the exact-value scan." >&2
    exit 1
  fi
  while IFS= read -r commit; do
    if git -c color.grep=false grep -a -z -l -F -f "$patterns" \
        "$commit" -- . \
        ':(glob,exclude)target/**' \
        ':(glob,exclude)**/target/**' >>"$history_content_matches" 2>/dev/null
    then
      :
    else
      status=$?
      if [[ "$status" -ne 1 ]]; then
        echo "Unable to scan one Git commit." >&2
        exit "$status"
      fi
    fi

    : >"$history_paths"
    if ! git ls-tree -r -z --name-only "$commit" \
      >"$history_paths" 2>/dev/null
    then
      echo "Unable to enumerate filenames in one Git commit." >&2
      exit 1
    fi
    while IFS= read -r -d '' history_path; do
      printf '%s\0' "$history_path" >"$one_path"
      if grep -a -z -q -F -f "$patterns" -- "$one_path" 2>/dev/null; then
        printf '%s:%s\0' "$commit" "$history_path" >>"$history_name_matches"
      else
        status=$?
        if [[ "$status" -ne 1 ]]; then
          echo "Unable to scan one historical filename." >&2
          exit "$status"
        fi
      fi
    done <"$history_paths"
  done <"$commits"

  if ! LC_ALL=C sort -z -u \
    "$history_content_matches" "$history_name_matches" >"$history_matches"
  then
    echo "Unable to combine exact-value Git history findings." >&2
    exit 1
  fi
  while IFS= read -r -d '' _match; do
    ((history_count += 1))
  done <"$history_matches"
fi

if [[ "$worktree_count" -ne 0 || "$history_count" -ne 0 ]]; then
  echo "Exact secret-value leak check failed: $worktree_count worktree file(s), $history_count Git object path(s)." >&2
  exit 1
fi

echo "Exact secret-value leak check passed: 0 worktree files, 0 Git object paths."
