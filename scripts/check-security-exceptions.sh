#!/usr/bin/env bash
set +x
set -euo pipefail

cd "$(dirname "$0")/.."

# Exact graph comparisons must not depend on a caller-wide color policy. CI
# deliberately sets CARGO_TERM_COLOR=always, which otherwise leaves ANSI bytes
# after Cargo's duplicate-edge `(*)` marker and defeats the normalization below.
export CARGO_TERM_COLOR=never

tracing_policy_error=""

is_patched_tracing_subscriber_version() {
  local version="$1"
  if [[ ! "$version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    return 1
  fi

  local major="${BASH_REMATCH[1]}"
  local minor="${BASH_REMATCH[2]}"
  local patch="${BASH_REMATCH[3]}"
  ((major > 0 || minor > 3 || (minor == 3 && patch >= 20)))
}

validate_tracing_subscriber_policy() {
  local locked_versions="$1"
  local active_versions="$2"
  local version
  local ignored_lock_entries=0
  local active_entries=0
  tracing_policy_error=""

  while IFS= read -r version; do
    [[ -n "$version" ]] || continue
    if [[ "$version" == "0.2.25" ]]; then
      ((ignored_lock_entries += 1))
    elif ! is_patched_tracing_subscriber_version "$version"; then
      tracing_policy_error="Cargo.lock contains another tracing-subscriber version covered by RUSTSEC-2025-0055"
      return 1
    fi
  done <<<"$locked_versions"

  if ((ignored_lock_entries != 1)); then
    tracing_policy_error="Cargo.lock must contain exactly one tracing-subscriber 0.2.25 entry until the advisory ignore is removed"
    return 1
  fi

  while IFS= read -r version; do
    [[ -n "$version" ]] || continue
    ((active_entries += 1))
    if ! is_patched_tracing_subscriber_version "$version"; then
      tracing_policy_error="the active graph contains tracing-subscriber below patched version 0.3.20"
      return 1
    fi
  done <<<"$active_versions"

  if ((active_entries == 0)); then
    tracing_policy_error="the active tracing-subscriber version set is unexpectedly empty"
    return 1
  fi
}

assert_tracing_policy_fixtures() {
  local valid_locked
  valid_locked="$(printf '%s\n' '0.2.25' '0.3.23')"

  if ! validate_tracing_subscriber_policy "$valid_locked" "0.3.23"; then
    echo "Internal tracing-subscriber policy fixture rejected the reviewed shape." >&2
    exit 1
  fi
  if validate_tracing_subscriber_policy "0.3.23" "0.3.23"; then
    echo "Internal tracing-subscriber policy fixture accepted a stale advisory ignore." >&2
    exit 1
  fi
  if validate_tracing_subscriber_policy \
    "$(printf '%s\n' '0.2.25' '0.3.19' '0.3.23')" "0.3.23"
  then
    echo "Internal tracing-subscriber policy fixture accepted another vulnerable lock entry." >&2
    exit 1
  fi
  if validate_tracing_subscriber_policy "$valid_locked" "0.2.25"; then
    echo "Internal tracing-subscriber policy fixture accepted an active vulnerable version." >&2
    exit 1
  fi
  if validate_tracing_subscriber_policy "$valid_locked" ""; then
    echo "Internal tracing-subscriber policy fixture accepted an empty active set." >&2
    exit 1
  fi
}

assert_tracing_policy_fixtures

# RUSTSEC-2025-0143 is accepted only through the current Envio client graph.
# Keep the exact versions here so an upstream dependency change forces a fresh
# review instead of silently inheriting the exception.
capnp_graph="$({
  cargo tree --locked -i capnp@0.23.2 --target all --prefix depth
} | sed -E 's# \(/[^)]*\)$##; s# \(\*\)$##')"

expected_capnp_graph="$(printf '%s\n' \
  '0capnp v0.23.2' \
  '1hypersync-client v1.4.0' \
  '2evm-fork-cache-hypersync v0.1.0-alpha.1' \
  '1hypersync-net-types v0.12.3' \
  '2hypersync-client v1.4.0')"

if [[ "$capnp_graph" != "$expected_capnp_graph" ]]; then
  echo "RUSTSEC-2025-0143 dependency scope changed." >&2
  echo "Expected:" >&2
  echo "$expected_capnp_graph" >&2
  echo "Observed:" >&2
  echo "$capnp_graph" >&2
  exit 1
fi

active_capnp_versions="$(
  cargo tree --locked --workspace --target all --prefix none \
    | grep -E '^capnp v' | sort -u
)"
if [[ "$active_capnp_versions" != 'capnp v0.23.2' ]]; then
  echo "The active workspace capnp version set changed." >&2
  echo "Observed:" >&2
  echo "$active_capnp_versions" >&2
  exit 1
fi

if cargo tree --locked -p evm-fork-cache-hypersync \
  --depth 1 --target all --prefix none | grep -Eq '^capnp v'
then
  echo "The HyperSync adapter acquired a forbidden direct capnp dependency." >&2
  exit 1
fi

for package in \
  evm-fork-cache-event-protocol \
  evm-fork-cache-remote \
  evm-fork-cache-event-service
do
  if cargo tree --locked -p "$package" --target all --prefix none \
    | grep -Eq '^capnp v'
  then
    echo "Cap'n Proto reached provider-neutral package $package." >&2
    exit 1
  fi
done

# RUSTSEC-2025-0055 is advisory-wide, so prove the complete active version set
# is patched and that 0.2.25 is the only vulnerable locked version. Requiring
# the exact inactive entry to remain makes its removal fail closed: the ignore
# must be deleted instead of silently becoming stale.
locked_tracing_versions="$(
  awk '
    /^\[\[package\]\]$/ {
      if (name == "tracing-subscriber") {
        print version
      }
      name = ""
      version = ""
      next
    }
    /^name = / {
      value = $0
      sub(/^name = "/, "", value)
      sub(/"$/, "", value)
      name = value
      next
    }
    /^version = / {
      value = $0
      sub(/^version = "/, "", value)
      sub(/"$/, "", value)
      version = value
      next
    }
    END {
      if (name == "tracing-subscriber") {
        print version
      }
    }
  ' Cargo.lock | sort
)"
active_dependency_tree="$(
  cargo tree --locked --workspace --all-features --target all --prefix none
)"
active_tracing_versions="$(
  printf '%s\n' "$active_dependency_tree" \
    | awk '$1 == "tracing-subscriber" { sub(/^v/, "", $2); print $2 }' \
    | sort -u
)"

if ! validate_tracing_subscriber_policy \
  "$locked_tracing_versions" "$active_tracing_versions"
then
  echo "RUSTSEC-2025-0055 scope check failed: $tracing_policy_error." >&2
  echo "Locked tracing-subscriber versions:" >&2
  printf '%s\n' "$locked_tracing_versions" >&2
  echo "Active tracing-subscriber versions:" >&2
  printf '%s\n' "$active_tracing_versions" >&2
  exit 1
fi

inactive_tracing="$({
  cargo tree --locked --workspace --all-features \
    -i tracing-subscriber@0.2.25 --target all --prefix none 2>/dev/null
} || true)"
if [[ -n "$inactive_tracing" ]]; then
  echo "RUSTSEC-2025-0055 is no longer confined to an inactive lock entry." >&2
  echo "$inactive_tracing" >&2
  exit 1
fi

# RUSTSEC-2026-0253 is accepted only while Alloy is the sole immediate lru
# consumer. Alloy 1.6 uses integer and fixed-hash keys for both provider caches;
# neither key type has a panic-capable destructor, which is required to trigger
# the advisory. A new immediate consumer or version requires renewed review.
lru_graph="$({
  cargo tree --locked -i lru@0.16.4 --target all --prefix depth --depth 1
} | sed -E 's# \(/[^)]*\)$##; s# \(\*\)$##')"
expected_lru_graph="$(printf '%s\n' \
  '0lru v0.16.4' \
  '1alloy-provider v1.6.3')"
if [[ "$lru_graph" != "$expected_lru_graph" ]]; then
  echo "RUSTSEC-2026-0253 dependency scope changed." >&2
  echo "Expected:" >&2
  echo "$expected_lru_graph" >&2
  echo "Observed:" >&2
  echo "$lru_graph" >&2
  exit 1
fi

# bincode 1 is unmaintained. It is accepted only through core's
# compatibility-bound cache format and the current HyperSync client. Hybrid V5
# uses canonical RLP and no extension crate may add either bincode generation.
# A new immediate consumer stops the release.
bincode_v1_graph="$({
  cargo tree --locked -i bincode@1.3.3 --target all --prefix depth --depth 1
} | sed -E 's# \(/[^)]*\)$##; s# \(\*\)$##')"
expected_bincode_v1_graph="$(printf '%s\n' \
  '0bincode v1.3.3' \
  '1evm-fork-cache v0.4.0-alpha.4' \
  '1hypersync-client v1.4.0')"
if [[ "$bincode_v1_graph" != "$expected_bincode_v1_graph" ]]; then
  echo "The accepted bincode 1 dependency scope changed." >&2
  echo "Expected:" >&2
  echo "$expected_bincode_v1_graph" >&2
  echo "Observed:" >&2
  echo "$bincode_v1_graph" >&2
  exit 1
fi

if cargo tree --locked --workspace --target all --prefix none \
  | grep -Eq '^bincode v2\.'
then
  echo "A forbidden bincode 2 dependency reached the extension workspace." >&2
  exit 1
fi

# derivative is tolerated only while unreachable.
inactive_derivative="$({
  cargo tree --locked -i derivative@2.2.0 --target all --prefix none 2>/dev/null
} || true)"
if [[ -n "$inactive_derivative" ]]; then
  echo "Unmaintained derivative 2.2.0 became reachable." >&2
  echo "$inactive_derivative" >&2
  exit 1
fi

# paste is an active transitive procedural macro. Pin its immediate consumers
# so a newly introduced path requires explicit review.
paste_graph="$({
  cargo tree --locked -i paste@1.0.15 --target all --prefix depth --depth 1
} | sed -E 's# \(/[^)]*\)$##; s# \(\*\)$##')"
expected_paste_graph="$(printf '%s\n' \
  '0paste v1.0.15 (proc-macro)' \
  '1alloy-primitives v1.6.1' \
  '1ark-ff v0.5.0' \
  '1parquet v57.3.1' \
  '1syn-solidity v1.6.1')"
if [[ "$paste_graph" != "$expected_paste_graph" ]]; then
  echo "The accepted paste dependency scope changed." >&2
  echo "Expected:" >&2
  echo "$expected_paste_graph" >&2
  echo "Observed:" >&2
  echo "$paste_graph" >&2
  exit 1
fi

echo "Security advisory and unmaintained-dependency scopes match policy."
