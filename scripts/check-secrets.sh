#!/usr/bin/env bash
set +x
set -euo pipefail

cd "$(dirname "$0")/.."
umask 077

required_version="8.30.1"
if ! command -v gitleaks >/dev/null 2>&1; then
  echo "gitleaks $required_version is required; install the official pinned release." >&2
  exit 1
fi

installed_version="$(gitleaks version 2>/dev/null || true)"
installed_version="${installed_version#v}"
if [[ "$installed_version" != "$required_version" ]]; then
  echo "gitleaks $required_version is required; found ${installed_version:-unknown}." >&2
  exit 1
fi

# Directory mode covers uncommitted and untracked publication inputs. The
# repository config excludes the intended local .env and generated output.
gitleaks dir --redact --no-banner --config .gitleaks.toml .

# Git mode scans every reachable patch. A new repository has no HEAD yet, so
# the worktree scan above is the complete available gate in that state.
if git rev-parse --verify HEAD >/dev/null 2>&1; then
  temporary_dir="$(mktemp -d)"
  trap 'rm -rf "$temporary_dir"' EXIT
  historical_env_paths="$temporary_dir/historical-env-paths"
  if ! git log -z --all --full-history --name-only --pretty=format: -- \
    .env ':(glob)**/.env' >"$historical_env_paths" 2>/dev/null
  then
    echo "Unable to inspect Git history for committed .env paths." >&2
    exit 1
  fi
  historical_env_count=0
  while IFS= read -r -d '' historical_env_path; do
    [[ -n "$historical_env_path" ]] || continue
    ((historical_env_count += 1))
  done <"$historical_env_paths"
  if ((historical_env_count != 0)); then
    echo "Secret scan failed: $historical_env_count historical .env path record(s)." >&2
    exit 1
  fi
  gitleaks git --redact --no-banner --config .gitleaks.toml \
    --log-opts="--all" .
fi
