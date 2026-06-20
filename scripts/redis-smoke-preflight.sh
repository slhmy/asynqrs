#!/usr/bin/env bash
set -euo pipefail

redis_smoke_preflight() {
  if [[ -n "${ASYNQ_RS_REDIS_URL:-}" ]]; then
    echo "Redis smoke preflight passed: ASYNQ_RS_REDIS_URL is set; strict Redis smoke will verify reachability"
    return 0
  fi

  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "Redis smoke preflight passed: Docker daemon is available for testcontainers"
    return 0
  fi

  {
    echo "Redis smoke preflight failed: strict Redis smoke requires either:"
    echo "  - ASYNQ_RS_REDIS_URL set to the Redis instance that strict smoke should verify, or"
    echo "  - a working Docker daemon for testcontainers"
    echo
    echo "Current evidence:"
    if [[ -z "${ASYNQ_RS_REDIS_URL:-}" ]]; then
      echo "  - ASYNQ_RS_REDIS_URL is not set"
    fi
    if ! command -v redis-server >/dev/null 2>&1; then
      echo "  - redis-server CLI is not installed"
    fi
    if ! command -v redis-cli >/dev/null 2>&1; then
      echo "  - redis-cli is not installed"
    fi
    if ! command -v docker >/dev/null 2>&1; then
      echo "  - docker CLI is not installed"
    elif ! docker info >/dev/null 2>&1; then
      echo "  - docker CLI is installed but the Docker daemon is unavailable"
    fi
    echo
    echo "Next steps:"
    echo "  - start Redis and export ASYNQ_RS_REDIS_URL=redis://127.0.0.1:6379/0, or"
    echo "  - start Docker so testcontainers can launch Redis"
    echo "  - then rerun scripts/release-gate.sh"
    echo "  - for a release candidate, rerun scripts/final-release-gate.sh"
  } >&2
  return 1
}

self_test() {
  local empty_path
  local output

  empty_path=$(mktemp -d)
  trap 'rm -rf "${empty_path}"' RETURN

  output=$(ASYNQ_RS_REDIS_URL=redis://example.invalid redis_smoke_preflight)
  if [[ "${output}" != *"ASYNQ_RS_REDIS_URL is set; strict Redis smoke will verify reachability"* ]]; then
    echo "redis smoke preflight self-test failed: URL branch did not pass" >&2
    exit 1
  fi
  if [[ "${output}" == *"reachable Redis instance"* ]]; then
    echo "redis smoke preflight self-test failed: preflight overclaimed Redis reachability" >&2
    exit 1
  fi

  if ASYNQ_RS_REDIS_URL= PATH="${empty_path}" redis_smoke_preflight >/dev/null 2>&1; then
    echo "redis smoke preflight self-test failed: missing infrastructure was accepted" >&2
    exit 1
  fi

  output=$(ASYNQ_RS_REDIS_URL= PATH="${empty_path}" redis_smoke_preflight 2>&1 || true)
  if [[ "${output}" != *"ASYNQ_RS_REDIS_URL is not set"* ]]; then
    echo "redis smoke preflight self-test failed: missing URL evidence was not reported" >&2
    exit 1
  fi
  if [[ "${output}" != *"redis-server CLI is not installed"* ]]; then
    echo "redis smoke preflight self-test failed: missing redis-server evidence was not reported" >&2
    exit 1
  fi
  if [[ "${output}" != *"redis-cli is not installed"* ]]; then
    echo "redis smoke preflight self-test failed: missing redis-cli evidence was not reported" >&2
    exit 1
  fi
  if [[ "${output}" != *"docker CLI is not installed"* ]]; then
    echo "redis smoke preflight self-test failed: missing docker evidence was not reported" >&2
    exit 1
  fi
  if [[ "${output}" != *"strict smoke should verify"* ]]; then
    echo "redis smoke preflight self-test failed: strict smoke reachability wording was not reported" >&2
    exit 1
  fi
  if [[ "${output}" != *"working Docker daemon for testcontainers"* ]]; then
    echo "redis smoke preflight self-test failed: Docker remediation was not reported" >&2
    exit 1
  fi
  if [[ "${output}" != *"scripts/release-gate.sh"* ]]; then
    echo "redis smoke preflight self-test failed: release gate rerun command was not reported" >&2
    exit 1
  fi
  if [[ "${output}" != *"scripts/final-release-gate.sh"* ]]; then
    echo "redis smoke preflight self-test failed: final release gate rerun command was not reported" >&2
    exit 1
  fi

  echo "redis smoke preflight self-test passed"
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit 0
fi

redis_smoke_preflight
