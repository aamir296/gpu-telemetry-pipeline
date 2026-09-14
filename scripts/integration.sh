#!/bin/sh
set -eu

container="gpu-telemetry-test-$$"
cleanup() { docker rm -f "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

docker run --detach --rm --name "$container" \
  -e POSTGRES_DB=telemetry -e POSTGRES_USER=telemetry -e POSTGRES_PASSWORD=telemetry \
  -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
port=$(docker port "$container" 5432/tcp | sed 's/.*://')
i=0
until docker exec "$container" pg_isready -U telemetry >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -gt 60 ]; then echo "PostgreSQL did not become ready" >&2; exit 1; fi
  sleep 1
done
TEST_DATABASE_URL="postgres://telemetry:telemetry@127.0.0.1:$port/telemetry?sslmode=disable" go test -tags=integration -count=1 ./internal/storage
