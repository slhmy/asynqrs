#!/usr/bin/env bash
set -euo pipefail

readonly expected_docs=$'docs/alignment-gaps.md\ndocs/migration.md\ndocs/public-api.md\ndocs/redis-smoke-matrix.md\ndocs/release-readiness-roadmap.md\ndocs/rust-native-runtime-redesign.md'

scan_docs_dir() {
  local root=${1:-docs}
  local actual
  actual=$(find "${root}" -maxdepth 1 -type f -name '*.md' -print | sort)

  local expected=${expected_docs//docs\//${root}/}

  if [[ "${actual}" != "${expected}" ]]; then
    echo "docs set scan failed: docs directory must contain exactly the release-facing markdown files" >&2
    echo "expected:" >&2
    printf '%s\n' "${expected}" >&2
    echo "actual:" >&2
    printf '%s\n' "${actual}" >&2
    return 1
  fi
}

scan_release_roadmap_content() {
  local root=${1:-docs}
  local roadmap="${root}/release-readiness-roadmap.md"

  for heading in \
    '^## Current Decision$' \
    '^## Release Documentation Set$' \
    '^## Final Evidence Checklist$' \
    '^## Current Release Audit$'; do
    if ! rg -n "${heading}" "${roadmap}" >/dev/null; then
      echo "docs set scan failed: release roadmap must keep release decision/checklist/audit structure" >&2
      return 1
    fi
  done

  if rg -n '^## Phase [0-9]|phase checklist' "${roadmap}" >/dev/null; then
    echo "docs set scan failed: release roadmap must not drift back into a phase-plan document" >&2
    return 1
  fi
}

if [[ "${1:-}" == "--self-test" ]]; then
  tmpdir=$(mktemp -d)
  trap 'rm -rf "${tmpdir}"' EXIT

  mkdir "${tmpdir}/docs"
  while IFS= read -r path; do
    touch "${tmpdir}/${path}"
  done <<<"${expected_docs}"
  cat >"${tmpdir}/docs/release-readiness-roadmap.md" <<'ROADMAP'
# Release Readiness Goal

## Current Decision

## Release Documentation Set

## Final Evidence Checklist

## Current Release Audit
ROADMAP

  scan_docs_dir "${tmpdir}/docs"
  scan_release_roadmap_content "${tmpdir}/docs"

  touch "${tmpdir}/docs/old-phase-plan.md"
  if scan_docs_dir "${tmpdir}/docs" >/dev/null 2>&1; then
    echo "docs set scan self-test failed: extra docs file was accepted" >&2
    exit 1
  fi

  rm "${tmpdir}/docs/old-phase-plan.md"
  rm "${tmpdir}/docs/migration.md"
  if scan_docs_dir "${tmpdir}/docs" >/dev/null 2>&1; then
    echo "docs set scan self-test failed: missing docs file was accepted" >&2
    exit 1
  fi

  touch "${tmpdir}/docs/migration.md"
  printf '\n## Phase 1. Old Cleanup Plan\n' >>"${tmpdir}/docs/release-readiness-roadmap.md"
  if scan_release_roadmap_content "${tmpdir}/docs" >/dev/null 2>&1; then
    echo "docs set scan self-test failed: phase-plan roadmap content was accepted" >&2
    exit 1
  fi

  sed -i.bak '/^## Phase 1\. Old Cleanup Plan$/d' "${tmpdir}/docs/release-readiness-roadmap.md"
  rm "${tmpdir}/docs/release-readiness-roadmap.md.bak"
  sed -i.bak '/^## Current Release Audit$/d' "${tmpdir}/docs/release-readiness-roadmap.md"
  rm "${tmpdir}/docs/release-readiness-roadmap.md.bak"
  if scan_release_roadmap_content "${tmpdir}/docs" >/dev/null 2>&1; then
    echo "docs set scan self-test failed: missing roadmap audit heading was accepted" >&2
    exit 1
  fi

  echo "docs set scan self-test passed"
  exit 0
fi

scan_docs_dir docs
scan_release_roadmap_content docs
echo "docs set scan passed"
