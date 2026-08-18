# Release procedure

No release step is automatic. Run this checklist from clean, reviewed commits;
never publish from a dirty working tree or from the credential-bearing `.env`.

## Compatibility set

| Package | Release | Contract |
| --- | --- | --- |
| `evm-fork-cache` | `0.4.0-alpha.4` | Async subscriber lifecycle, delivery tokens, chain controls, capabilities, durable checkpoints |
| `evm-fork-cache-event-protocol` | `0.1.0-alpha.1` | Wire protocol v1 |
| `evm-fork-cache-remote` | `0.1.0-alpha.1` | Runtime client for core 0.4 alpha and protocol v1 |
| `evm-fork-cache-event-service` | `0.1.0-alpha.1` | Provider-neutral durable service for protocol v1 |
| `evm-fork-cache-hypersync` | `0.1.0-alpha.1` | HyperSync source and service composition |

The protocol version is independent from crate semver. Breaking protobuf
changes require a new protocol module/version and an explicit negotiation path;
do not silently change v1 field meaning.

## Preflight

1. Confirm `.env` is mode `0600`, ignored, absent from the index, and absent
   from every reachable commit:

   ```bash
   mode="$(stat -f '%Lp' .env 2>/dev/null || stat -c '%a' .env)"
   test "$mode" = 600
   git check-ignore -v .env
   git ls-files --error-unmatch .env # must fail
   git log --all --full-history -- .env # must print nothing
   ```

   Install the exact reviewed Gitleaks release from the [official v8.30.1
   release](https://github.com/gitleaks/gitleaks/releases/tag/v8.30.1), then run:

   ```bash
   bash scripts/test-publication-security-gates.sh
   bash scripts/check-secrets.sh
   bash scripts/check-exact-secret-leaks.sh
   bash scripts/check-worktree-whitespace.sh
   ```

   The first command exercises synthetic negative fixtures, including a
   matching filename containing the synthetic token, tab, and newline. The
   second scans the worktree and every reachable Git patch with redacted output
   and rejects any historical `.env` with a count-only diagnostic. The third is
   a separate local-only gate that reads the two exact values from the current
   `.env`; its NUL-safe worktree/history scans emit only aggregate counts and
   do not exclude historical `.env` files. The fourth applies Git's whitespace
   policy to every tracked or untracked publication input, which is required
   while the repository has an unborn branch. The intended current `.env`,
   `.git`, and disposable `target` output are excluded; `.env.example` remains
   scanned. Never echo either value or a matching line. Stop the release if any
   command reports a finding.

2. Verify the exact published core artifact rather than a sibling checkout:

   ```bash
   cargo info evm-fork-cache@0.4.0-alpha.4
   cargo tree --locked -p evm-fork-cache-remote -i evm-fork-cache@0.4.0-alpha.4
   ! grep -Eq 'evm-fork-cache.*path[[:space:]]*=' Cargo.toml
   ```

   The complete extension matrix below compiles and tests that registry source.
   Do not substitute a dirty local core checkout for the published artifact.

3. In this workspace, run:

   ```bash
   cargo fmt --all -- --check
   git diff --check
   bash scripts/check-worktree-whitespace.sh
   cargo test --workspace --all-targets --all-features --locked
   cargo test --workspace --doc --locked
   cargo clippy --workspace --all-targets --all-features --locked --no-deps -- -D warnings -D clippy::missing_errors_doc
   RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
   cargo +1.90.0 check --workspace --all-targets --locked
   cargo bench --workspace --no-run --locked
   bash scripts/check-security-exceptions.sh
   cargo install --locked --version 0.20.2 cargo-deny
   cargo install --locked --version 0.22.2 cargo-audit
   cargo deny check --warn unmaintained
   cargo audit --ignore RUSTSEC-2025-0055 --ignore RUSTSEC-2025-0143 --ignore RUSTSEC-2026-0253
   bash scripts/test-publication-security-gates.sh
   bash scripts/check-secrets.sh
   bash scripts/check-exact-secret-leaks.sh
   cargo package --locked -p evm-fork-cache-event-protocol
   cargo package --list -p evm-fork-cache-remote
   cargo package --list -p evm-fork-cache-event-service
   cargo package --list -p evm-fork-cache-hypersync
   ```

   Cargo requires every publish dependency to exist in the target registry even
   with `--no-verify`, so downstream `.crate` archives cannot be assembled before
   their upstream release without falsifying dependency versions. Until then,
   `cargo package --list` is the honest package-content preflight and the
   workspace gates above provide build coverage. The listed contents must not
   include `.env`, credentials, SQLite databases, or build artifacts.

4. Run the ignored authenticated live source, restart/replay, runtime, paired
   head, and bounded historical comparison tests. Record the date, chain,
   ranges, exact-identity result, and directional timings in
   `docs/benchmarks.md`.

5. Run the dependency policy and audit with only the documented exceptions.
   cargo-deny 0.20.2 is the current reviewed version from its [official
   release](https://github.com/EmbarkStudios/cargo-deny/releases/tag/0.20.2);
   cargo-audit is pinned to the current [0.22.2 crates.io
   release](https://crates.io/crates/cargo-audit/0.22.2):

   ```bash
   cargo install --locked --version 0.20.2 cargo-deny
   cargo install --locked --version 0.22.2 cargo-audit
   bash scripts/check-security-exceptions.sh
   cargo deny check --warn unmaintained
   cargo audit --ignore RUSTSEC-2025-0055 --ignore RUSTSEC-2025-0143 --ignore RUSTSEC-2026-0253
   ```

   Re-read `SECURITY.md`, confirm that HyperSync still requires Cap'n Proto
   0.23, and confirm the only vulnerable locked `tracing-subscriber` is the
   required unreachable 0.2.25 entry while every active version is at least
   0.3.20. Remove each advisory exception as soon as its constrained lock
   entry disappears. Any new vulnerability or yanked package fails the release.

   Confirm `RUSTSEC-2026-0253` remains confined to `lru 0.16.4` through
   `alloy-provider 1.6.3`, and that Alloy's caches still use only non-panicking
   integer and fixed-hash keys. Remove the exception as soon as Alloy accepts
   `lru >= 0.18.2`.

   Confirm every third-party `uses:` entry remains pinned to the full reviewed
   commit recorded in `SECURITY.md`; a tag or branch name alone blocks release.

6. Confirm that GitHub Private Vulnerability Reporting is enabled and that the
   [private report
   form](https://github.com/KaiCode2/evm-fork-cache-hypersync/security/advisories/new)
   opens for the public repository.

7. Confirm that `origin`, all manifest repository/homepage URLs, and CI badge
   links identify the public repository that will own the release.

8. Confirm the manifest retains the registry-only exact dependency
   `evm-fork-cache = "=0.4.0-alpha.4"`, contains no sibling path override, and
   run the complete workspace matrix from a fresh standalone clone. A core alpha
   upgrade requires a new extension alpha, lockfile refresh, and complete gate
   rerun; do not silently float between prerelease contracts.

9. Obtain the independent phase/release review. Resolve every P0-P3 finding and
   rerun the affected gates.

## Publish order

Publish only with explicit authorization, one crate at a time:

The exact core dependency is already published. Publish the extension crates in
this order:

1. `evm-fork-cache-event-protocol` 0.1.0-alpha.1.
2. `evm-fork-cache-remote` 0.1.0-alpha.1.
3. `evm-fork-cache-event-service` 0.1.0-alpha.1.
4. `evm-fork-cache-hypersync` 0.1.0-alpha.1.

Wait for each release to appear in the crates.io index before verifying or
publishing its dependent. Run `cargo package --locked` for each downstream
package immediately before publication; at that point it must pass extracted
registry verification, not just content listing. Afterward, verify docs.rs
builds and install the service binary from crates.io in a clean directory. Tag
and push only after the registry artifacts are confirmed.

If a published artifact is materially broken, stop the sequence and yank it;
never overwrite or reuse a released version.
