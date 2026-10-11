#!/usr/bin/env bash
set -euo pipefail
# Renders `go test -cover` output as a Markdown per-package coverage table.
# Usage: scripts/coverage_summary.sh <go-test-log>   (append to $GITHUB_STEP_SUMMARY in CI)
log="${1:?usage: coverage_summary.sh <go-test-log>}"
echo "### Unit-test coverage by package (no infrastructure)"
echo
echo "| Package | Coverage |"
echo "|---|---|"
awk '
  function short(p) { sub(/^github.com\/gasthecreator\/pharos\//, "", p); return p }
  /^ok[ \t]/ && /coverage:/ {
    c = $0; sub(/.*coverage: /, "", c); sub(/ of statements.*/, "", c)
    printf "| `%s` | %s |\n", short($2), c
  }
  /^\?[ \t]/ { printf "| `%s` | no test files |\n", short($2) }
  /^[ \t]+github.com/ && /coverage: 0.0%/ { printf "| `%s` | no tests |\n", short($1) }
' "$log"
