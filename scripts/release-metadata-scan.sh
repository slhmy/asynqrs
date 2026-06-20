#!/usr/bin/env bash
set -euo pipefail

failures=0

check_present() {
  local description=$1
  local pattern=$2
  local path=$3

  if ! rg -n "$pattern" "$path" >/dev/null; then
    echo "release metadata scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_absent() {
  local description=$1
  local pattern=$2
  local path=$3

  if rg -n "$pattern" "$path" >/dev/null; then
    echo "release metadata scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_text_present() {
  local description=$1
  local pattern=$2
  local text=$3

  if ! printf '%s\n' "${text}" | rg -n "${pattern}" - >/dev/null; then
    echo "release metadata scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_text_absent() {
  local description=$1
  local pattern=$2
  local text=$3

  if printf '%s\n' "${text}" | rg -n "${pattern}" - >/dev/null; then
    echo "release metadata scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_file_exists() {
  local description=$1
  local path=$2

  if [[ ! -f "${path}" ]]; then
    echo "release metadata scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_no_extra_examples() {
  local expected_pattern=$1

  while IFS= read -r path; do
    local name
    name=$(basename "${path}" .rs)
    if [[ ! " ${expected_pattern} " =~ " ${name} " ]]; then
      echo "release metadata scan failed: examples/${name}.rs is not linked from the migration guide" >&2
      failures=$((failures + 1))
    fi
  done < <(find examples -maxdepth 1 -type f -name '*.rs' | sort)
}

check_runtime_boundaries_documented() {
  local docs_path=$1
  shift

  local source_path
  for source_path in "$@"; do
    while IFS= read -r declaration; do
      local boundary
      boundary=${declaration##*trait }
      check_present \
        "runtime architecture doc must explain retained ${boundary} boundary" \
        "\\| \`${boundary}\` \\|" \
        "${docs_path}"
    done < <(rg -o '^pub\(crate\) trait [A-Za-z_][A-Za-z0-9_]*' "${source_path}")
  done
}

check_package_list() {
  local package_list=$1

  for path in \
    'Cargo.toml' \
    'README.md' \
    'CHANGELOG.md' \
    'docs/alignment-gaps.md' \
    'docs/migration.md' \
    'docs/public-api.md' \
    'docs/redis-smoke-matrix.md' \
    'docs/release-readiness-roadmap.md' \
    'docs/rust-native-runtime-redesign.md' \
    'examples/enqueue.rs' \
    'examples/server.rs' \
    'examples/middleware.rs' \
    'examples/handler_failure.rs' \
    'examples/graceful_shutdown.rs' \
    'examples/scheduler.rs' \
    'examples/inspector_metadata.rs' \
    'examples/aggregation.rs' \
    'src/broker/redis/scripts/call.rs' \
    'src/broker/redis/scripts/registry.rs' \
    'src/broker/redis/scripts/sources.rs' \
    'src/broker/redis/scripts/spec.rs' \
    'src/lib.rs'
  do
    check_text_present \
      "cargo package file list must include ${path}" \
      "^${path}$" \
      "${package_list}"
  done

  check_text_absent \
    "cargo package file list must not include deleted Processor module files" \
    '^src/processor(/|\.rs$)' \
    "${package_list}"

  check_text_absent \
    "cargo package file list must not include stale phase-plan docs" \
    '^docs/.*phase.*\.md$' \
    "${package_list}"

  check_text_absent \
    "cargo package file list must not include local CI workflow files" \
    '^\.github/' \
    "${package_list}"

  check_text_absent \
    "cargo package file list must not include local agent instructions" \
    '^AGENTS\.md$' \
    "${package_list}"

  check_text_absent \
    "cargo package file list must not include local release tooling scripts" \
    '^scripts/' \
    "${package_list}"
}

check_matches_sample() {
  local description=$1
  local pattern=$2
  local sample=$3

  if ! printf '%s\n' "$sample" | rg -n "$pattern" - >/dev/null; then
    echo "release metadata scan self-test failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

if [[ "${1:-}" == "--self-test" ]]; then
  check_matches_sample \
    "package name guard pattern" \
    '^name = "asynqrs"$' \
    'name = "asynqrs"'

  check_matches_sample \
    "publish=true package guard pattern" \
    '^publish = true$' \
    'publish = true'

  check_matches_sample \
    "package rust-version guard pattern" \
    '^rust-version = "1\.85"$' \
    'rust-version = "1.85"'

  check_matches_sample \
    "package edition guard pattern" \
    '^edition = "2024"$' \
    'edition = "2024"'

  check_matches_sample \
    "package description guard pattern" \
    '^description = ".+"$' \
    'description = "Rust learning reimplementation of hibiken/asynq, a Redis-backed task queue."'

  check_matches_sample \
    "package repository guard pattern" \
    '^repository = "https://github\.com/slhmy/asynq-rs"$' \
    'repository = "https://github.com/slhmy/asynq-rs"'

  check_matches_sample \
    "package readme guard pattern" \
    '^readme = "README\.md"$' \
    'readme = "README.md"'

  check_matches_sample \
    "package license guard pattern" \
    '^license = "MIT"$' \
    'license = "MIT"'

  check_matches_sample \
    "README workflow API pattern" \
    'RedisBackedClient|RedisBackedServerBuilder|RedisBackedScheduler|GroupAggregator|Inspector' \
    '- Aggregate grouped tasks with `GroupAggregator` or `GroupAggregatorFunc`.'

  check_matches_sample \
    "README inspector worker metadata pattern" \
    'servers, workers, and scheduler metadata' \
    '- Inspect queues, tasks, servers, workers, and scheduler metadata with `Inspector`.'

  check_matches_sample \
    "crate docs aggregation workflow pattern" \
    'aggregate grouped tasks with .*GroupAggregator' \
    '//! - aggregate grouped tasks with [`GroupAggregator`] or'

  check_matches_sample \
    "crate docs inspector worker metadata pattern" \
    'servers, workers, and scheduler metadata' \
    '//! - inspect queues, tasks, servers, workers, and scheduler metadata with'

  check_matches_sample \
    "README Redis preflight pattern" \
    'scripts/redis-smoke-preflight\.sh' \
    'scripts/redis-smoke-preflight.sh runs before Redis smoke.'

  check_matches_sample \
    "package list smoke wording pattern" \
    'cargo package --list --allow-dirty' \
    'cargo package --list --allow-dirty confirms the package file list.'

  check_matches_sample \
    "networked package release step wording pattern" \
    'networked release step' \
    'Full package verification and publishing remain separate networked release steps.'

  check_matches_sample \
    "strict rustdoc wording pattern" \
    'RUSTDOCFLAGS="-D warnings" cargo doc --no-deps|strict rustdoc' \
    'RUSTDOCFLAGS="-D warnings" cargo doc --no-deps builds strict rustdoc.'

  check_matches_sample \
    "clippy warnings denied wording pattern" \
    'cargo clippy --all-targets -- -D warnings|clippy with warnings denied' \
    'cargo clippy --all-targets -- -D warnings runs clippy with warnings denied.'

  check_matches_sample \
    "Redis matrix package file-list smoke wording pattern" \
    'package file-list smoke' \
    'The gate runs package file-list smoke before Redis preflight.'

  check_matches_sample \
    "local package tooling exclusion wording pattern" \
    'local CI workflows, agent instructions, (and )?release tooling scripts' \
    'The package excludes local CI workflows, agent instructions, and release tooling scripts.'

  check_matches_sample \
    "Cargo local tooling exclude pattern" \
    '"(/\.github/|AGENTS\.md|/scripts/)"' \
    '"/scripts/"'

  check_matches_sample \
    "README release roadmap description pattern" \
    'release decision, evidence checklist, and current audit' \
    'docs/release-readiness-roadmap.md: final release decision, evidence checklist, and current audit.'

  check_matches_sample \
    "stale README release roadmap phase wording pattern" \
    'phase checklist' \
    'docs/release-readiness-roadmap.md: final release goal and phase checklist.'

  check_matches_sample \
    "README Redis preflight reachability boundary pattern" \
    'does not replace the strict smoke command.*reachability check|does not replace the strict smoke command.s Redis reachability check' \
    "scripts/redis-smoke-preflight.sh does not replace the strict smoke command's Redis reachability check."

  check_matches_sample \
    "Redis matrix preflight non-proof pattern" \
    'preflight does not prove Redis reachability' \
    'The preflight does not prove Redis reachability and does not replace strict smoke.'

  check_matches_sample \
    "Redis matrix local CLI evidence pattern" \
    'local Redis CLI|redis-server|redis-cli' \
    'The preflight reports local Redis CLI evidence such as redis-server and redis-cli.'

  check_matches_sample \
    "Redis preflight overclaim pattern" \
    'preflight (proves|verifies|confirms).*Redis (reachability|is reachable)|preflight.*reachable Redis instance' \
    'The preflight proves Redis reachability.'

  check_matches_sample \
    "roadmap CI release gate evidence pattern" \
    'same release gate with strict Redis mode' \
    'CI still runs the same release gate with strict Redis mode, a Redis URL, and a Redis service.'

  check_matches_sample \
    "roadmap CI no Docker-in-Docker wording pattern" \
    'Docker-in-Docker' \
    'CI uses a Redis service container rather than relying on Docker-in-Docker.'

  check_matches_sample \
    "runtime redesign mutable attach wording pattern" \
    'mutably attach one shared `ServerRuntimeState`' \
    '| `ServerRuntimeStateAttach` | Construction calls `attach_server_runtime` to mutably attach one shared `ServerRuntimeState` to concrete runtimes. |'

  check_matches_sample \
    "runtime redesign attach method wording pattern" \
    'attach_server_runtime' \
    '`ServerRuntimeStateAttach::attach_server_runtime` wires state during construction.'

  check_matches_sample \
    "runtime redesign retained boundary table pattern" \
    '\| `WorkerBrokerCore` \|' \
    '| `WorkerBrokerCore` | Worker hot-path dequeue, complete, retry, archive, and result writes use opaque futures. |'

  check_matches_sample \
    "source-derived runtime boundary declaration pattern" \
    '^pub\(crate\) trait [A-Za-z_][A-Za-z0-9_]*' \
    'pub(crate) trait WorkerBrokerCore {'

  check_matches_sample \
    "package list deleted processor path pattern" \
    '^src/processor(/|\.rs$)' \
    'src/processor.rs'

  tmpdir=$(mktemp -d)
  trap 'rm -rf "${tmpdir}"' EXIT

  cat >"${tmpdir}/runtime.rs" <<'RUNTIME'
pub(crate) trait FirstRuntimeBoundary {}
pub(crate) trait SecondRuntimeBoundary {}
RUNTIME
  cat >"${tmpdir}/runtime.md" <<'RUNTIME_DOCS'
| Boundary | Why It Remains |
| --- | --- |
| `FirstRuntimeBoundary` | Exercised by release metadata self-test. |
| `SecondRuntimeBoundary` | Exercised by release metadata self-test. |
RUNTIME_DOCS
  check_runtime_boundaries_documented "${tmpdir}/runtime.md" "${tmpdir}/runtime.rs"

  sed -i.bak '/SecondRuntimeBoundary/d' "${tmpdir}/runtime.md"
  before_missing_boundary_failures=${failures}
  check_runtime_boundaries_documented "${tmpdir}/runtime.md" "${tmpdir}/runtime.rs" >/dev/null 2>&1
  if [[ "${failures}" == "${before_missing_boundary_failures}" ]]; then
    echo "release metadata scan self-test failed: undocumented source-derived runtime boundary was accepted" >&2
    failures=$((failures + 1))
  fi
  failures=${before_missing_boundary_failures}

  cat >"${tmpdir}/CHANGELOG.md" <<'CHANGELOG'
# CHANGELOG

## Current Release Summary

Known release blockers:

- None.

Current local evidence: strict Redis smoke passes with Docker-backed
testcontainers (`25 passed; 0 failed`), and `scripts/final-release-gate.sh`
passes both release-gate passes locally.
CHANGELOG
  check_absent \
    "self-test clean changelog must not carry stale refactor known-gap markers" \
    'Known gap:|Processor still|still owns runtime state|remaining compatibility shell' \
    "${tmpdir}/CHANGELOG.md"

  printf '\n- Known gap: Processor still owns runtime state.\n' >>"${tmpdir}/CHANGELOG.md"
  before_stale_changelog_failures=${failures}
  check_absent \
    "self-test stale changelog must fail stale refactor known-gap marker guard" \
    'Known gap:|Processor still|still owns runtime state|remaining compatibility shell' \
    "${tmpdir}/CHANGELOG.md" >/dev/null 2>&1
  if [[ "${failures}" == "${before_stale_changelog_failures}" ]]; then
    echo "release metadata scan self-test failed: stale changelog known-gap marker was accepted" >&2
    failures=$((failures + 1))
  fi
  failures=${before_stale_changelog_failures}

  clean_package_list=$'Cargo.toml\nREADME.md\nCHANGELOG.md\ndocs/alignment-gaps.md\ndocs/migration.md\ndocs/public-api.md\ndocs/redis-smoke-matrix.md\ndocs/release-readiness-roadmap.md\ndocs/rust-native-runtime-redesign.md\nexamples/enqueue.rs\nexamples/server.rs\nexamples/middleware.rs\nexamples/handler_failure.rs\nexamples/graceful_shutdown.rs\nexamples/scheduler.rs\nexamples/inspector_metadata.rs\nexamples/aggregation.rs\nsrc/broker/redis/scripts/call.rs\nsrc/broker/redis/scripts/registry.rs\nsrc/broker/redis/scripts/sources.rs\nsrc/broker/redis/scripts/spec.rs\nsrc/lib.rs'
  check_package_list "${clean_package_list}"

  stale_package_list="${clean_package_list}"$'\nsrc/processor.rs'
  before_stale_package_failures=${failures}
  check_package_list "${stale_package_list}" >/dev/null 2>&1
  if [[ "${failures}" == "${before_stale_package_failures}" ]]; then
    echo "release metadata scan self-test failed: stale processor file in package list was accepted" >&2
    failures=$((failures + 1))
  fi
  failures=${before_stale_package_failures}

  stale_package_list="${clean_package_list}"$'\n.github/workflows/ci.yml\nAGENTS.md\nscripts/release-gate.sh'
  before_local_tooling_package_failures=${failures}
  check_package_list "${stale_package_list}" >/dev/null 2>&1
  if [[ "${failures}" == "${before_local_tooling_package_failures}" ]]; then
    echo "release metadata scan self-test failed: local release tooling files in package list were accepted" >&2
    failures=$((failures + 1))
  fi
  failures=${before_local_tooling_package_failures}

  missing_package_doc_list=$(printf '%s\n' "${clean_package_list}" | sed '/^docs\/migration\.md$/d')
  before_missing_package_doc_failures=${failures}
  check_package_list "${missing_package_doc_list}" >/dev/null 2>&1
  if [[ "${failures}" == "${before_missing_package_doc_failures}" ]]; then
    echo "release metadata scan self-test failed: missing release doc in package list was accepted" >&2
    failures=$((failures + 1))
  fi
  failures=${before_missing_package_doc_failures}

  check_matches_sample \
    "migration compiled example link pattern" \
    'examples/(enqueue|server|middleware|handler_failure|graceful_shutdown|scheduler|inspector_metadata|aggregation)\.rs' \
    'Compiled example: [`examples/middleware.rs`](../examples/middleware.rs).'

  if [[ -f "examples/__missing_release_metadata_scan_self_test__.rs" ]]; then
    echo "release metadata scan self-test failed: missing example fixture unexpectedly exists" >&2
    failures=$((failures + 1))
  fi

  check_matches_sample \
    "registered example allow-list pattern" \
    '(^| )middleware( |$)' \
    'enqueue server middleware handler_failure graceful_shutdown scheduler inspector_metadata aggregation'

  if printf '%s\n' 'enqueue server middleware handler_failure graceful_shutdown scheduler inspector_metadata aggregation' \
    | rg -n '(^| )untracked( |$)' - >/dev/null; then
    echo "release metadata scan self-test failed: untracked example name was accepted by allow-list pattern" >&2
    failures=$((failures + 1))
  fi

  check_matches_sample \
    "changelog release summary heading pattern" \
    '^## Current Release Summary$' \
    '## Current Release Summary'

  check_matches_sample \
    "changelog blockers heading pattern" \
    '^Known release blockers:$' \
    'Known release blockers:'

  check_matches_sample \
    "changelog no active blocker pattern" \
    '^- None\.$' \
    '- None.'

  check_matches_sample \
    "changelog strict Redis pass evidence pattern" \
    'Strict Redis smoke passes|strict Redis smoke passes|25 passed; 0 failed' \
    'Current local evidence: strict Redis smoke passes with Docker-backed testcontainers (`25 passed; 0 failed`).'

  check_matches_sample \
    "changelog final gate pass evidence pattern" \
    'scripts/final-release-gate\.sh.*passes both|final two-pass release gate now pass' \
    'Current local evidence: `scripts/final-release-gate.sh` passes both release-gate passes locally.'

  check_matches_sample \
    "changelog stale gap marker pattern" \
    'Known gap:|Processor still|still owns runtime state|remaining compatibility shell' \
    '- Known gap: Processor still owns runtime state.'

  check_matches_sample \
    "public API metadata codec helper wording pattern" \
    'Metadata wire codec helpers' \
    '- Metadata wire codec helpers for server records stay crate-internal.'

  check_matches_sample \
    "public API metadata codec internal wording pattern" \
    'crate-internal' \
    '- Metadata wire codec helpers for server records stay crate-internal.'

  check_matches_sample \
    "stale public admin codec helper wording pattern" \
    'explicit `admin` metadata codec helpers|public .*metadata codec helpers|metadata codecs stay under `admin`' \
    'Public callers may use the explicit `admin` metadata codec helpers.'

  check_matches_sample \
    "stale migration admin codec wording pattern" \
    'metadata codecs stay under `admin`' \
    'Low-level metadata codecs stay under `admin` for explicit wire-compatibility work.'

  if (( failures > 0 )); then
    exit 1
  fi

  echo "release metadata scan self-test passed"
  exit 0
fi

check_present \
  "Cargo.toml must keep the asynqrs package name" \
  '^name = "asynqrs"$' \
  Cargo.toml

check_present \
  "Cargo.toml must keep publishing enabled after the release decision" \
  '^publish = true$' \
  Cargo.toml

check_present \
  "Cargo.toml package must exclude local CI workflows" \
  '"/\.github/"' \
  Cargo.toml

check_present \
  "Cargo.toml package must exclude local agent instructions" \
  '"AGENTS\.md"' \
  Cargo.toml

check_present \
  "Cargo.toml package must exclude local release tooling scripts" \
  '"/scripts/"' \
  Cargo.toml

check_present \
  "Cargo.toml must keep the Rust 1.85 MSRV policy visible" \
  '^rust-version = "1\.85"$' \
  Cargo.toml

check_present \
  "Cargo.toml must keep the Rust 2024 edition policy visible" \
  '^edition = "2024"$' \
  Cargo.toml

check_present \
  "Cargo.toml must keep a package description before release" \
  '^description = ".+"$' \
  Cargo.toml

check_present \
  "Cargo.toml must keep the repository metadata before release" \
  '^repository = "https://github\.com/slhmy/asynq-rs"$' \
  Cargo.toml

check_present \
  "Cargo.toml must keep README metadata before release" \
  '^readme = "README\.md"$' \
  Cargo.toml

check_present \
  "Cargo.toml must keep MIT license metadata before release" \
  '^license = "MIT"$' \
  Cargo.toml

package_list=$(cargo package --list --allow-dirty)
check_package_list "${package_list}"

check_present \
  "README must mention the preferred Redis-backed client workflow" \
  'RedisBackedClient' \
  README.md

check_present \
  "README must mention the preferred Redis-backed server workflow" \
  'RedisBackedServerBuilder' \
  README.md

check_present \
  "README must mention the preferred scheduler workflow" \
  'RedisBackedScheduler' \
  README.md

check_present \
  "README must mention the preferred aggregation workflow" \
  'GroupAggregator' \
  README.md

check_present \
  "README must mention the preferred inspector workflow" \
  'Inspector' \
  README.md

check_present \
  "README must mention worker metadata in the inspector workflow" \
  'servers, workers, and scheduler metadata' \
  README.md

check_present \
  "README must describe the release roadmap as decision/checklist/audit evidence" \
  'release decision, evidence checklist, and current audit' \
  README.md

check_absent \
  "README must not describe the release roadmap as a phase checklist" \
  'phase checklist' \
  README.md

check_present \
  "crate docs must mention the preferred aggregation workflow" \
  'aggregate grouped tasks with .*GroupAggregator' \
  src/lib.rs

check_present \
  "crate docs must mention worker metadata in the inspector workflow" \
  'servers, workers, and scheduler metadata' \
  src/lib.rs

check_present \
  "README must explain final release uses the two-pass gate" \
  'scripts/final-release-gate\.sh' \
  README.md

check_present \
  "README must mention the Redis smoke preflight" \
  'scripts/redis-smoke-preflight\.sh' \
  README.md

check_present \
  "README must mention the package file-list smoke" \
  'cargo package --list --allow-dirty' \
  README.md

check_present \
  "README must keep full package verification separate from the offline gate" \
  'networked release steps?' \
  README.md

check_present \
  "README must explain local tooling is excluded from the crate package" \
  'release tooling scripts out of the crate' \
  README.md

check_present \
  "README must mention strict rustdoc release verification" \
  'RUSTDOCFLAGS="-D warnings" cargo doc --no-deps' \
  README.md

check_present \
  "README must mention clippy warnings denied" \
  'cargo clippy --all-targets -- -D warnings|clippy with warnings denied' \
  README.md

check_present \
  "README must say preflight does not replace Redis reachability verification" \
  'does not replace the strict smoke command.s Redis reachability check' \
  README.md

check_present \
  "Redis smoke matrix must say preflight does not prove Redis reachability" \
  'preflight does not prove Redis reachability' \
  docs/redis-smoke-matrix.md

check_present \
  "Redis smoke matrix must mention local Redis CLI evidence in preflight diagnostics" \
  'local Redis CLI|redis-server|redis-cli' \
  docs/redis-smoke-matrix.md

check_present \
  "Redis smoke matrix must mention the package file-list smoke evidence" \
  'package file-list smoke' \
  docs/redis-smoke-matrix.md

check_present \
  "Redis smoke matrix must mention strict rustdoc evidence" \
  'strict rustdoc' \
  docs/redis-smoke-matrix.md

check_present \
  "Redis smoke matrix must mention clippy warnings denied evidence" \
  'clippy with warnings denied|cargo clippy --all-targets -- -D warnings' \
  docs/redis-smoke-matrix.md

check_absent \
  "README must not overclaim Redis preflight reachability" \
  'preflight (proves|verifies|confirms).*Redis (reachability|is reachable)|preflight.*reachable Redis instance' \
  README.md

check_absent \
  "Redis smoke matrix must not overclaim Redis preflight reachability" \
  'preflight (proves|verifies|confirms).*Redis (reachability|is reachable)|preflight.*reachable Redis instance' \
  docs/redis-smoke-matrix.md

check_present \
  "release roadmap must mention CI release gate strict Redis evidence" \
  'same release gate with strict Redis mode' \
  docs/release-readiness-roadmap.md

check_present \
  "release roadmap must mention CI Redis URL evidence" \
  'Redis URL' \
  docs/release-readiness-roadmap.md

check_present \
  "release roadmap must mention CI Redis service evidence" \
  'Redis service' \
  docs/release-readiness-roadmap.md

check_present \
  "release roadmap must say CI does not rely on Docker-in-Docker" \
  'Docker-in-Docker' \
  docs/release-readiness-roadmap.md

check_present \
  "Redis smoke matrix must say CI does not rely on Docker-in-Docker" \
  'Docker-in-Docker' \
  docs/redis-smoke-matrix.md

check_present \
  "release roadmap must mention the package file-list smoke" \
  'cargo package --list --allow-dirty' \
  docs/release-readiness-roadmap.md

check_present \
  "release roadmap must keep full package verification separate from the offline gate" \
  'networked release steps?' \
  docs/release-readiness-roadmap.md

check_present \
  "release roadmap must explain local tooling is excluded from the crate package" \
  'release tooling scripts' \
  docs/release-readiness-roadmap.md

check_present \
  "release roadmap must mention strict rustdoc release verification" \
  'RUSTDOCFLAGS="-D warnings" cargo doc --no-deps|rustdoc with' \
  docs/release-readiness-roadmap.md

check_present \
  "release roadmap must mention clippy warnings denied" \
  'clippy with warnings denied|cargo clippy --all-targets -- -D warnings' \
  docs/release-readiness-roadmap.md

check_present \
  "runtime architecture doc must describe mutable runtime-state attachment" \
  'mutably attach one shared `ServerRuntimeState`' \
  docs/rust-native-runtime-redesign.md

check_present \
  "runtime architecture doc must name the current attach_server_runtime boundary" \
  'attach_server_runtime' \
  docs/rust-native-runtime-redesign.md

check_runtime_boundaries_documented \
  docs/rust-native-runtime-redesign.md \
  src/server/worker.rs \
  src/server/worker_broker/core.rs

check_present \
  "public API docs must mention metadata codec helpers" \
  'Metadata wire codec helpers' \
  docs/public-api.md

check_present \
  "public API docs must keep metadata codec helpers crate-internal" \
  'crate-internal' \
  docs/public-api.md

check_absent \
  "public API docs must not describe admin metadata codec helpers as public workflow helpers" \
  'explicit `admin` metadata codec helpers|public .*metadata codec helpers|metadata codecs stay under `admin`' \
  docs/public-api.md

check_absent \
  "migration guide must not describe admin metadata codec helpers as public workflow helpers" \
  'explicit `admin` metadata codec helpers|public .*metadata codec helpers|metadata codecs stay under `admin`' \
  docs/migration.md

for example in \
  enqueue \
  server \
  middleware \
  handler_failure \
  graceful_shutdown \
  scheduler \
  inspector_metadata \
  aggregation
do
  check_present \
    "migration guide must link compiled ${example} example" \
    "examples/${example}\\.rs" \
    docs/migration.md

  check_file_exists \
    "compiled ${example} example linked from migration guide must exist" \
    "examples/${example}.rs"
done

check_no_extra_examples "enqueue server middleware handler_failure graceful_shutdown scheduler inspector_metadata aggregation"

check_present \
  "CHANGELOG must keep a current release summary" \
  '^## Current Release Summary$' \
  CHANGELOG.md

check_present \
  "CHANGELOG must list active release blockers" \
  '^Known release blockers:$' \
  CHANGELOG.md

check_present \
  "CHANGELOG release blockers must record no active blockers" \
  '^- None\.$' \
  CHANGELOG.md

check_present \
  "CHANGELOG must include strict Redis pass evidence" \
  'Strict Redis smoke passes|strict Redis smoke passes|25 passed; 0 failed' \
  CHANGELOG.md

check_present \
  "CHANGELOG must include final two-pass gate pass evidence" \
  'scripts/final-release-gate\.sh.*passes both|final two-pass release gate now pass' \
  CHANGELOG.md

check_absent \
  "CHANGELOG must not carry stale refactor known-gap markers" \
  'Known gap:|Processor still|still owns runtime state|remaining compatibility shell' \
  CHANGELOG.md

if (( failures > 0 )); then
  exit 1
fi

echo "release metadata scan passed"
