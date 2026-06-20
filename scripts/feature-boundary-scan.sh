#!/usr/bin/env bash
set -euo pipefail

failures=0

check_matches_sample() {
  local description=$1
  local pattern=$2
  local sample=$3

  if ! printf '%s\n' "${sample}" | rg -n "${pattern}" - >/dev/null; then
    echo "feature boundary scan self-test failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_tree_present() {
  local description=$1
  local pattern=$2
  local tree=$3

  if ! printf '%s\n' "${tree}" | rg -n "${pattern}" - >/dev/null; then
    echo "feature boundary scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_tree_absent() {
  local description=$1
  local pattern=$2
  local tree=$3

  if printf '%s\n' "${tree}" | rg -n "${pattern}" - >/dev/null; then
    echo "feature boundary scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

cargo_tree() {
  cargo tree \
    -p asynqrs \
    --no-default-features \
    --edges normal \
    --prefix none \
    --format '{p}' \
    "$@"
}

if [[ "${1:-}" == "--self-test" ]]; then
  check_matches_sample \
    "package tree pattern matches macro crate package lines" \
    '^asynqrs-macros v[0-9]+\.[0-9]+\.[0-9]+' \
    'asynqrs-macros v0.2.0 (/workspace/asynqrs-macros)'

  check_matches_sample \
    "package tree pattern matches serde package lines" \
    '^serde v[0-9]+\.[0-9]+\.[0-9]+' \
    'serde v1.0.228'

  check_matches_sample \
    "package tree pattern matches serde_json package lines" \
    '^serde_json v[0-9]+\.[0-9]+\.[0-9]+' \
    'serde_json v1.0.145'

  if [[ "${failures}" -ne 0 ]]; then
    exit 1
  fi

  echo "feature boundary scan self-test passed"
  exit 0
fi

default_tree=$(cargo_tree)
macros_tree=$(cargo_tree --features macros)
serde_tree=$(cargo_tree --features serde)

check_tree_absent \
  "no-default build must not pull asynqrs-macros" \
  '^asynqrs-macros v[0-9]+\.[0-9]+\.[0-9]+' \
  "${default_tree}"
check_tree_absent \
  "no-default build must not pull serde" \
  '^serde v[0-9]+\.[0-9]+\.[0-9]+' \
  "${default_tree}"
check_tree_absent \
  "no-default build must not pull serde_json" \
  '^serde_json v[0-9]+\.[0-9]+\.[0-9]+' \
  "${default_tree}"

check_tree_present \
  "macros feature must pull asynqrs-macros" \
  '^asynqrs-macros v[0-9]+\.[0-9]+\.[0-9]+' \
  "${macros_tree}"
check_tree_absent \
  "macros-only build must not pull serde" \
  '^serde v[0-9]+\.[0-9]+\.[0-9]+' \
  "${macros_tree}"
check_tree_absent \
  "macros-only build must not pull serde_json" \
  '^serde_json v[0-9]+\.[0-9]+\.[0-9]+' \
  "${macros_tree}"

check_tree_present \
  "serde feature must pull serde" \
  '^serde v[0-9]+\.[0-9]+\.[0-9]+' \
  "${serde_tree}"
check_tree_present \
  "serde feature must pull serde_json" \
  '^serde_json v[0-9]+\.[0-9]+\.[0-9]+' \
  "${serde_tree}"
check_tree_absent \
  "serde-only build must not pull asynqrs-macros" \
  '^asynqrs-macros v[0-9]+\.[0-9]+\.[0-9]+' \
  "${serde_tree}"

cargo check -p asynqrs --no-default-features --features macros >/dev/null
cargo check -p asynqrs --no-default-features --features serde >/dev/null
cargo test --test task_payload_derive_feature_boundary --no-default-features --features macros

if [[ "${failures}" -ne 0 ]]; then
  exit 1
fi

echo "feature boundary scan passed"
