#!/usr/bin/env bash
set -euo pipefail

for pass in 1 2; do
  echo "==> final release gate pass ${pass}/2"
  scripts/release-gate.sh
done
