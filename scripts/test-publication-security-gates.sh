#!/usr/bin/env bash
set +x
set -euo pipefail

cd "$(dirname "$0")/.."
umask 077

if ! command -v gitleaks >/dev/null 2>&1; then
  echo "Publication security fixtures require the pinned gitleaks executable." >&2
  exit 1
fi

temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT

configure_fixture_git() {
  local fixture="$1"
  git -C "$fixture" init -q
  git -C "$fixture" config user.name "Publication Gate Fixture"
  git -C "$fixture" config user.email "fixture@invalid.example"
}

fixture_token="fixture-token-$(printf '%032d' 0)"
fixture_rpc="https://fixture.invalid/rpc?token=$fixture_token"

# A committed matching file whose name itself contains the exact token, a tab,
# and a newline proves that the exact-value gate keeps paths NUL-delimited and
# emits only aggregate counts.
exact_fixture="$temporary_dir/exact"
mkdir -p "$exact_fixture/scripts"
cp scripts/check-exact-secret-leaks.sh "$exact_fixture/scripts/"
printf 'ENVIO_API_TOKEN=%s\nRPC_URL=%s\n' \
  "$fixture_token" "$fixture_rpc" >"$exact_fixture/.env"
hostile_name=$'control\tpath\n'"$fixture_token"
printf 'filename-only fixture\n' >"$exact_fixture/$hostile_name"
printf '%s\n' "$fixture_rpc" >"$exact_fixture/content-only"
configure_fixture_git "$exact_fixture"
git -C "$exact_fixture" add -- "$hostile_name" content-only
git -C "$exact_fixture" commit -qm "fixture"

set +e
exact_output="$(
  cd "$exact_fixture"
  bash scripts/check-exact-secret-leaks.sh 2>&1
)"
exact_status=$?
set -e
if [[ "$exact_status" -ne 1 ]]; then
  echo "Exact-value negative fixture did not fail with status 1." >&2
  exit 1
fi
expected_exact_output="Exact secret-value leak check failed: 2 worktree file(s), 2 Git object path(s)."
if [[ "$exact_output" != "$expected_exact_output" ]]; then
  echo "Exact-value negative fixture emitted unexpected or non-count-only output." >&2
  exit 1
fi

# The clean unborn-repository shape must pass while excluding only .env.
clean_fixture="$temporary_dir/clean"
mkdir -p "$clean_fixture/scripts"
cp scripts/check-exact-secret-leaks.sh "$clean_fixture/scripts/"
printf 'ENVIO_API_TOKEN=%s\nRPC_URL=%s\n' \
  "$fixture_token" "$fixture_rpc" >"$clean_fixture/.env"
clean_output="$(
  cd "$clean_fixture"
  bash scripts/check-exact-secret-leaks.sh 2>&1
)"
if [[ "$clean_output" != \
  "Exact secret-value leak check passed: 0 worktree files, 0 Git object paths." ]]
then
  echo "Exact-value clean fixture did not pass with count-only output." >&2
  exit 1
fi

# A historical .env must fail after the redacted directory scan without
# emitting the historical pathname or file contents.
history_fixture="$temporary_dir/history"
mkdir -p "$history_fixture/scripts"
cp scripts/check-secrets.sh "$history_fixture/scripts/"
cp .gitleaks.toml "$history_fixture/"
printf 'FIXTURE_VALUE=not-a-credential\n' >"$history_fixture/.env"
configure_fixture_git "$history_fixture"
git -C "$history_fixture" add -- .env
git -C "$history_fixture" commit -qm "fixture"

set +e
history_output="$(
  cd "$history_fixture"
  bash scripts/check-secrets.sh 2>&1
)"
history_status=$?
set -e
if [[ "$history_status" -ne 1 ]]; then
  echo "Historical .env negative fixture did not fail with status 1." >&2
  exit 1
fi
history_last_line="${history_output##*$'\n'}"
if [[ "$history_last_line" != \
  "Secret scan failed: 1 historical .env path record(s)." ]]
then
  echo "Historical .env fixture did not end with the count-only diagnostic." >&2
  exit 1
fi
if printf '%s\n' "$history_output" | grep -Fqx '.env'; then
  echo "Historical .env fixture emitted a pathname." >&2
  exit 1
fi

echo "Publication security negative fixtures passed."
