#!/usr/bin/env bash
set -euo pipefail

export ASYNQ_RS_REDIS_STRICT="${ASYNQ_RS_REDIS_STRICT:-1}"

buf lint
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test -p asynqrs-macros
cargo test --no-default-features
cargo test --all-features
scripts/feature-boundary-scan.sh --self-test
scripts/feature-boundary-scan.sh
scripts/release-gate-shape-scan.sh --self-test
scripts/release-gate-shape-scan.sh
scripts/release-metadata-scan.sh --self-test
scripts/release-metadata-scan.sh
cargo package -p asynqrs --list --allow-dirty >/dev/null
cargo package -p asynqrs-macros --list --allow-dirty >/dev/null
cargo package -p asynqrs-macros --allow-dirty
cargo package -p asynqrs --allow-dirty
scripts/docs-set-scan.sh --self-test
scripts/docs-set-scan.sh
scripts/public-api-scan.sh --self-test
scripts/public-api-scan.sh
scripts/semantic-gap-scan.sh --self-test
scripts/semantic-gap-scan.sh
scripts/redis-smoke-preflight.sh --self-test
scripts/redis-smoke-preflight.sh
cargo test broker::redis::tests:: -- --nocapture --test-threads=1
cargo test --examples
cargo test --examples --all-features
cargo test --doc
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo test --quiet
git diff --check
