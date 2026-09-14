#!/bin/sh
set -eu

failed=0
for command in go docker kubectl helm; do
  if command -v "$command" >/dev/null 2>&1; then
    printf '%-12s %s\n' "$command" "OK ($(command -v "$command"))"
  else
    printf '%-12s %s\n' "$command" "MISSING"
    failed=1
  fi
done

provider=${CLUSTER_PROVIDER:-kind}
if command -v "$provider" >/dev/null 2>&1; then
  printf '%-12s %s\n' "$provider" "OK ($(command -v "$provider"))"
else
  printf '%-12s %s\n' "$provider" "MISSING"
  failed=1
fi

if docker info >/dev/null 2>&1; then
  printf '%-12s %s\n' docker-daemon "OK"
else
  printf '%-12s %s\n' docker-daemon "NOT RUNNING"
  failed=1
fi
exit "$failed"
