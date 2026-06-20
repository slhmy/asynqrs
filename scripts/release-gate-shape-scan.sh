#!/usr/bin/env bash
set -euo pipefail

failures=0

check_present() {
  local description=$1
  local pattern=$2
  local path=$3

  if ! rg -n "$pattern" "$path" >/dev/null; then
    echo "release gate shape scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_matches_sample() {
  local description=$1
  local pattern=$2
  local sample=$3

  if ! printf '%s\n' "$sample" | rg -n "$pattern" - >/dev/null; then
    echo "release gate shape scan self-test failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_executable() {
  local description=$1
  local path=$2

  if [[ ! -x "${path}" ]]; then
    echo "release gate shape scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

check_order() {
  local description=$1
  local first_pattern=$2
  local second_pattern=$3
  local path=$4

  local first_line second_line
  first_line=$(rg -n "${first_pattern}" "${path}" | head -n 1 | cut -d: -f1 || true)
  second_line=$(rg -n "${second_pattern}" "${path}" | head -n 1 | cut -d: -f1 || true)

  if [[ -z "${first_line}" || -z "${second_line}" || "${first_line}" -ge "${second_line}" ]]; then
    echo "release gate shape scan failed: ${description}" >&2
    failures=$((failures + 1))
  fi
}

scan_release_gate() {
  local release_gate=$1
  local final_gate=$2
  local start_failures=${failures}

  check_present \
    "release gate must default Redis smoke to strict mode" \
    'ASYNQ_RS_REDIS_STRICT="\$\{ASYNQ_RS_REDIS_STRICT:-1\}"' \
    "${release_gate}"

  check_present "release gate must run buf lint" '^buf lint$' "${release_gate}"
  check_present "release gate must run cargo fmt check" '^cargo fmt --check$' "${release_gate}"
  check_present "release gate must run clippy with warnings denied" '^cargo clippy --all-targets -- -D warnings$' "${release_gate}"
  check_present "release gate must run release gate shape scan self-test" '^scripts/release-gate-shape-scan\.sh --self-test$' "${release_gate}"
  check_present "release gate must run release gate shape scan" '^scripts/release-gate-shape-scan\.sh$' "${release_gate}"
  check_present "release gate must run release metadata scan self-test" '^scripts/release-metadata-scan\.sh --self-test$' "${release_gate}"
  check_present "release gate must run release metadata scan" '^scripts/release-metadata-scan\.sh$' "${release_gate}"
  check_present "release gate must run cargo package list smoke" '^cargo package --list --allow-dirty >/dev/null$' "${release_gate}"
  check_present "release gate must run docs set scan self-test" '^scripts/docs-set-scan\.sh --self-test$' "${release_gate}"
  check_present "release gate must run docs set scan" '^scripts/docs-set-scan\.sh$' "${release_gate}"
  check_present "release gate must run public API scan self-test" '^scripts/public-api-scan\.sh --self-test$' "${release_gate}"
  check_present "release gate must run public API scan" '^scripts/public-api-scan\.sh$' "${release_gate}"
  check_present "release gate must run semantic gap scan self-test" '^scripts/semantic-gap-scan\.sh --self-test$' "${release_gate}"
  check_present "release gate must run semantic gap scan" '^scripts/semantic-gap-scan\.sh$' "${release_gate}"
  check_present "release gate must run Redis smoke preflight self-test" '^scripts/redis-smoke-preflight\.sh --self-test$' "${release_gate}"
  check_present "release gate must run Redis smoke preflight" '^scripts/redis-smoke-preflight\.sh$' "${release_gate}"
  check_present "release gate must run strict Redis smoke command" '^cargo test broker::redis::tests:: -- --nocapture --test-threads=1$' "${release_gate}"
  check_present "release gate must compile examples" '^cargo test --examples$' "${release_gate}"
  check_present "release gate must run doctests" '^cargo test --doc$' "${release_gate}"
  check_present "release gate must build rustdoc with warnings denied" '^RUSTDOCFLAGS="-D warnings" cargo doc --no-deps$' "${release_gate}"
  check_present "release gate must run the full test suite" '^cargo test --quiet$' "${release_gate}"
  check_present "release gate must run whitespace diff check" '^git diff --check$' "${release_gate}"

  check_order \
    "release gate must run fmt before clippy" \
    '^cargo fmt --check$' \
    '^cargo clippy --all-targets -- -D warnings$' \
    "${release_gate}"

  check_order \
    "release gate must run clippy before release shape scans" \
    '^cargo clippy --all-targets -- -D warnings$' \
    '^scripts/release-gate-shape-scan\.sh --self-test$' \
    "${release_gate}"

  check_order \
    "release gate must run package file-list smoke before Redis preflight" \
    '^cargo package --list --allow-dirty >/dev/null$' \
    '^scripts/redis-smoke-preflight\.sh$' \
    "${release_gate}"

  check_order \
    "release gate must run Redis preflight before strict Redis smoke" \
    '^scripts/redis-smoke-preflight\.sh$' \
    '^cargo test broker::redis::tests:: -- --nocapture --test-threads=1$' \
    "${release_gate}"

  check_order \
    "release gate must run strict Redis smoke before examples" \
    '^cargo test broker::redis::tests:: -- --nocapture --test-threads=1$' \
    '^cargo test --examples$' \
    "${release_gate}"

  check_order \
    "release gate must run doctests before rustdoc" \
    '^cargo test --doc$' \
    '^RUSTDOCFLAGS="-D warnings" cargo doc --no-deps$' \
    "${release_gate}"

  check_order \
    "release gate must run rustdoc before full tests" \
    '^RUSTDOCFLAGS="-D warnings" cargo doc --no-deps$' \
    '^cargo test --quiet$' \
    "${release_gate}"

  check_present "final release gate must run two passes" '^for pass in 1 2; do$' "${final_gate}"
  check_present "final release gate must print two-pass progress" '^  echo "==> final release gate pass \$\{pass\}/2"$' "${final_gate}"
  check_present "final release gate must call the release gate" '^  scripts/release-gate\.sh$' "${final_gate}"

  [[ "${failures}" == "${start_failures}" ]]
}

scan_script_entrypoints() {
  local scripts_dir=$1
  local start_failures=${failures}

  for script in \
    docs-set-scan.sh \
    final-release-gate.sh \
    public-api-scan.sh \
    redis-smoke-preflight.sh \
    release-gate-shape-scan.sh \
    release-gate.sh \
    release-metadata-scan.sh \
    semantic-gap-scan.sh
  do
    check_present \
      "${script} must keep its bash shebang" \
      '^#!/usr/bin/env bash$' \
      "${scripts_dir}/${script}"
    check_executable \
      "${script} must stay executable because release gates call scripts directly" \
      "${scripts_dir}/${script}"
  done

  [[ "${failures}" == "${start_failures}" ]]
}

scan_ci_gate() {
  local ci_workflow=$1
  local start_failures=${failures}

  check_present \
    "CI must set strict Redis smoke mode" \
    'ASYNQ_RS_REDIS_STRICT: "1"' \
    "${ci_workflow}"

  check_present \
    "CI must provide a Redis URL for strict smoke tests" \
    'ASYNQ_RS_REDIS_URL: redis://127\.0\.0\.1:6379' \
    "${ci_workflow}"

  check_present \
    "CI must run a Redis service for strict smoke tests" \
    'image: redis:' \
    "${ci_workflow}"

  check_present \
    "CI must install ripgrep before release scan scripts run" \
    'apt-get install -y ripgrep' \
    "${ci_workflow}"

  check_present \
    "CI must run the same release gate as local verification" \
    'run: scripts/release-gate\.sh' \
    "${ci_workflow}"

  [[ "${failures}" == "${start_failures}" ]]
}

if [[ "${1:-}" == "--self-test" ]]; then
  check_matches_sample \
    "strict Redis default pattern" \
    'ASYNQ_RS_REDIS_STRICT="\$\{ASYNQ_RS_REDIS_STRICT:-1\}"' \
    'export ASYNQ_RS_REDIS_STRICT="${ASYNQ_RS_REDIS_STRICT:-1}"'

  check_matches_sample \
    "Redis smoke command pattern" \
    '^cargo test broker::redis::tests:: -- --nocapture --test-threads=1$' \
    'cargo test broker::redis::tests:: -- --nocapture --test-threads=1'

  check_matches_sample \
    "clippy warnings denied command pattern" \
    '^cargo clippy --all-targets -- -D warnings$' \
    'cargo clippy --all-targets -- -D warnings'

  check_matches_sample \
    "rustdoc warnings command pattern" \
    '^RUSTDOCFLAGS="-D warnings" cargo doc --no-deps$' \
    'RUSTDOCFLAGS="-D warnings" cargo doc --no-deps'

  check_matches_sample \
    "release gate shape scan command pattern" \
    '^scripts/release-gate-shape-scan\.sh$' \
    'scripts/release-gate-shape-scan.sh'

  check_matches_sample \
    "cargo package list smoke command pattern" \
    '^cargo package --list --allow-dirty >/dev/null$' \
    'cargo package --list --allow-dirty >/dev/null'

  check_matches_sample \
    "Redis smoke preflight command pattern" \
    '^scripts/redis-smoke-preflight\.sh$' \
    'scripts/redis-smoke-preflight.sh'

  check_matches_sample \
    "final two-pass loop pattern" \
    '^for pass in 1 2; do$' \
    'for pass in 1 2; do'

  check_matches_sample \
    "final two-pass progress output pattern" \
    '^  echo "==> final release gate pass \$\{pass\}/2"$' \
    '  echo "==> final release gate pass ${pass}/2"'

  check_matches_sample \
    "CI release gate command pattern" \
    'run: scripts/release-gate\.sh' \
    'run: scripts/release-gate.sh'

  check_matches_sample \
    "CI strict Redis mode pattern" \
    'ASYNQ_RS_REDIS_STRICT: "1"' \
    'ASYNQ_RS_REDIS_STRICT: "1"'

  check_matches_sample \
    "CI Redis URL pattern" \
    'ASYNQ_RS_REDIS_URL: redis://127\.0\.0\.1:6379' \
    'ASYNQ_RS_REDIS_URL: redis://127.0.0.1:6379'

  check_matches_sample \
    "CI Redis service pattern" \
    'image: redis:' \
    'image: redis:5.0'

  check_matches_sample \
    "CI ripgrep install pattern" \
    'apt-get install -y ripgrep' \
    'run: sudo apt-get update && sudo apt-get install -y ripgrep'

  tmpdir=$(mktemp -d)
  trap 'rm -rf "${tmpdir}"' EXIT

  cp scripts/release-gate.sh "${tmpdir}/release-gate.sh"
  cp scripts/final-release-gate.sh "${tmpdir}/final-release-gate.sh"
  cp .github/workflows/ci.yml "${tmpdir}/ci.yml"
  mkdir "${tmpdir}/scripts"
  cp -p scripts/*.sh "${tmpdir}/scripts/"
  scan_release_gate "${tmpdir}/release-gate.sh" "${tmpdir}/final-release-gate.sh"
  scan_script_entrypoints "${tmpdir}/scripts"
  scan_ci_gate "${tmpdir}/ci.yml"

  assert_release_gate_rejects_missing() {
    local sed_pattern=$1
    local description=$2

    sed -i.bak "${sed_pattern}" "${tmpdir}/release-gate.sh"
    if scan_release_gate "${tmpdir}/release-gate.sh" "${tmpdir}/final-release-gate.sh" >/dev/null 2>&1; then
      echo "release gate shape scan self-test failed: missing ${description} was accepted" >&2
      exit 1
    fi
    failures=0
    cp scripts/release-gate.sh "${tmpdir}/release-gate.sh"
  }

  assert_final_gate_rejects_missing() {
    local sed_pattern=$1
    local description=$2

    sed -i.bak "${sed_pattern}" "${tmpdir}/final-release-gate.sh"
    if scan_release_gate "${tmpdir}/release-gate.sh" "${tmpdir}/final-release-gate.sh" >/dev/null 2>&1; then
      echo "release gate shape scan self-test failed: missing ${description} was accepted" >&2
      exit 1
    fi
    failures=0
    cp scripts/final-release-gate.sh "${tmpdir}/final-release-gate.sh"
  }

  assert_ci_gate_rejects_missing() {
    local sed_pattern=$1
    local description=$2

    sed -i.bak "${sed_pattern}" "${tmpdir}/ci.yml"
    if scan_ci_gate "${tmpdir}/ci.yml" >/dev/null 2>&1; then
      echo "release gate shape scan self-test failed: missing ${description} was accepted" >&2
      exit 1
    fi
    failures=0
    cp .github/workflows/ci.yml "${tmpdir}/ci.yml"
  }

  assert_release_gate_rejects_order() {
    local first_line=$1
    local second_line=$2
    local description=$3

    awk -v first="${first_line}" -v second="${second_line}" '
      $0 == first { first_seen = 1; next }
      $0 == second { print second; print first; next }
      { print }
      END {
        if (!first_seen) {
          exit 1
        }
      }
    ' scripts/release-gate.sh >"${tmpdir}/release-gate.sh"
    if scan_release_gate "${tmpdir}/release-gate.sh" "${tmpdir}/final-release-gate.sh" >/dev/null 2>&1; then
      echo "release gate shape scan self-test failed: wrong order for ${description} was accepted" >&2
      exit 1
    fi
    failures=0
    cp scripts/release-gate.sh "${tmpdir}/release-gate.sh"
  }

  assert_release_gate_rejects_missing '/ASYNQ_RS_REDIS_STRICT="\${ASYNQ_RS_REDIS_STRICT:-1}"/d' "strict Redis default"
  assert_release_gate_rejects_missing '/^buf lint$/d' "buf lint command"
  assert_release_gate_rejects_missing '/^cargo fmt --check$/d' "cargo fmt check command"
  assert_release_gate_rejects_missing '/^cargo clippy --all-targets -- -D warnings$/d' "clippy warnings denied command"
  assert_release_gate_rejects_missing '/^scripts\/release-gate-shape-scan\.sh --self-test$/d' "shape scan self-test command"
  assert_release_gate_rejects_missing '/^scripts\/release-gate-shape-scan\.sh$/d' "shape scan command"
  assert_release_gate_rejects_missing '/^scripts\/release-metadata-scan\.sh --self-test$/d' "release metadata scan self-test command"
  assert_release_gate_rejects_missing '/^scripts\/release-metadata-scan\.sh$/d' "release metadata scan command"
  assert_release_gate_rejects_missing '/^cargo package --list --allow-dirty >\/dev\/null$/d' "cargo package list smoke command"
  assert_release_gate_rejects_missing '/^scripts\/docs-set-scan\.sh --self-test$/d' "docs set scan self-test command"
  assert_release_gate_rejects_missing '/^scripts\/docs-set-scan\.sh$/d' "docs set scan command"
  assert_release_gate_rejects_missing '/^scripts\/public-api-scan\.sh --self-test$/d' "public API scan self-test command"
  assert_release_gate_rejects_missing '/^scripts\/public-api-scan\.sh$/d' "public API scan command"
  assert_release_gate_rejects_missing '/^scripts\/semantic-gap-scan\.sh --self-test$/d' "semantic gap scan self-test command"
  assert_release_gate_rejects_missing '/^scripts\/semantic-gap-scan\.sh$/d' "semantic gap scan command"
  assert_release_gate_rejects_missing '/^scripts\/redis-smoke-preflight\.sh --self-test$/d' "Redis preflight self-test command"
  assert_release_gate_rejects_missing '/^scripts\/redis-smoke-preflight\.sh$/d' "Redis preflight command"
  assert_release_gate_rejects_missing '/^cargo test broker::redis::tests:: -- --nocapture --test-threads=1$/d' "strict Redis smoke command"
  assert_release_gate_rejects_missing '/^cargo test --examples$/d' "examples command"
  assert_release_gate_rejects_missing '/^cargo test --doc$/d' "doctest command"
  assert_release_gate_rejects_missing '/^RUSTDOCFLAGS="-D warnings" cargo doc --no-deps$/d' "rustdoc warnings command"
  assert_release_gate_rejects_missing '/^cargo test --quiet$/d' "full test suite command"
  assert_release_gate_rejects_missing '/^git diff --check$/d' "whitespace diff check"
  assert_release_gate_rejects_order 'cargo fmt --check' 'cargo clippy --all-targets -- -D warnings' "fmt before clippy"
  assert_release_gate_rejects_order 'cargo clippy --all-targets -- -D warnings' 'scripts/release-gate-shape-scan.sh --self-test' "clippy before release shape scans"
  assert_release_gate_rejects_order 'cargo package --list --allow-dirty >/dev/null' 'scripts/redis-smoke-preflight.sh' "package smoke before Redis preflight"
  assert_release_gate_rejects_order 'scripts/redis-smoke-preflight.sh' 'cargo test broker::redis::tests:: -- --nocapture --test-threads=1' "Redis preflight before strict Redis smoke"
  assert_release_gate_rejects_order 'cargo test broker::redis::tests:: -- --nocapture --test-threads=1' 'cargo test --examples' "strict Redis smoke before examples"
  assert_release_gate_rejects_order 'cargo test --doc' 'RUSTDOCFLAGS="-D warnings" cargo doc --no-deps' "doctests before rustdoc"
  assert_release_gate_rejects_order 'RUSTDOCFLAGS="-D warnings" cargo doc --no-deps' 'cargo test --quiet' "rustdoc before full tests"
  assert_final_gate_rejects_missing '/^for pass in 1 2; do$/d' "final gate two-pass loop"
  assert_final_gate_rejects_missing '/^  echo "==> final release gate pass \${pass}\/2"$/d' "final gate two-pass progress output"
  assert_final_gate_rejects_missing '/^  scripts\/release-gate\.sh$/d' "final gate release-gate call"
  assert_ci_gate_rejects_missing '/ASYNQ_RS_REDIS_STRICT: "1"/d' "CI strict Redis mode"
  assert_ci_gate_rejects_missing '/ASYNQ_RS_REDIS_URL: redis:\/\/127\.0\.0\.1:6379/d' "CI Redis URL"
  assert_ci_gate_rejects_missing '/image: redis:/d' "CI Redis service"
  assert_ci_gate_rejects_missing '/apt-get install -y ripgrep/d' "CI ripgrep install"
  assert_ci_gate_rejects_missing '/run: scripts\/release-gate\.sh/d' "CI release gate command"

  chmod -x "${tmpdir}/scripts/release-gate.sh"
  if scan_script_entrypoints "${tmpdir}/scripts" >/dev/null 2>&1; then
    echo "release gate shape scan self-test failed: non-executable release gate script was accepted" >&2
    exit 1
  fi
  failures=0
  chmod +x "${tmpdir}/scripts/release-gate.sh"

  if (( failures > 0 )); then
    exit 1
  fi

  echo "release gate shape scan self-test passed"
  exit 0
fi

scan_release_gate scripts/release-gate.sh scripts/final-release-gate.sh
scan_script_entrypoints scripts
scan_ci_gate .github/workflows/ci.yml

if (( failures > 0 )); then
  exit 1
fi

echo "release gate shape scan passed"
