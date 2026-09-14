#!/bin/sh
set -eu

provider=${CLUSTER_PROVIDER:-kind}
cluster=${CLUSTER_NAME:-gpu-telemetry-e2e}
release=${RELEASE_NAME:-telemetry}
namespace=${NAMESPACE:-gpu-telemetry}
input_csv=${INPUT_CSV:-input/real-dcgm.csv}
dataset=${DATASET_ID:-dcgm-sample}
api_url=${API_URL:-http://127.0.0.1:18080}
expected_rows=$(awk 'END {print NR - 1}' "$input_csv")

if [ "$provider" = kind ]; then
  context="kind-$cluster"
else
  context="$cluster"
fi

streamer="$release-gpu-telemetry-streamer"
collector="$release-gpu-telemetry-collector"
queue="$release-gpu-telemetry-queue"
postgres="$release-gpu-telemetry-postgresql-0"

sql() {
  query=$1
  kubectl --context "$context" -n "$namespace" exec "$postgres" -- \
    sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"' sh "$query"
}

check_metrics() {
  resource=$1
  local_port=$2
  remote_port=$3
  log_file=$(mktemp)
  kubectl --context "$context" -n "$namespace" port-forward "$resource" "$local_port:$remote_port" >"$log_file" 2>&1 &
  forward_pid=$!
  deadline=$(( $(date +%s) + 20 ))
  metrics=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    metrics=$(curl -fsS "http://127.0.0.1:$local_port/metrics" 2>/dev/null || true)
    [ -n "$metrics" ] && break
    sleep 1
  done
  kill "$forward_pid" >/dev/null 2>&1 || true
  wait "$forward_pid" >/dev/null 2>&1 || true
  rm -f "$log_file"
  printf '%s' "$metrics" | grep -q 'gpu_telemetry_operations_total' || {
    echo "Application metrics unavailable for $resource" >&2
    return 1
  }
}

wait_for_partial_cycle() {
	minimum_cycle=$1
  deadline=$(( $(date +%s) + 90 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    cycle=$(sql "SELECT cycle_id FROM telemetry_events WHERE dataset_id='$dataset' AND cycle_id >= $minimum_cycle GROUP BY cycle_id HAVING count(*) > 0 AND count(*) < $expected_rows ORDER BY cycle_id DESC LIMIT 1")
    if [ -n "$cycle" ]; then
      printf '%s\n' "$cycle"
      return 0
    fi
    sleep 1
  done
  echo "No partial cycle observed before timeout" >&2
  return 1
}

wait_for_cycle_completion() {
  cycle=$1
  deadline=$(( $(date +%s) + 150 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    count=$(sql "SELECT count(*) FROM telemetry_events WHERE dataset_id='$dataset' AND cycle_id=$cycle")
    later=$(sql "SELECT count(*) FROM telemetry_events WHERE dataset_id='$dataset' AND cycle_id>$cycle")
    if [ "$count" -eq "$expected_rows" ] && [ "$later" -gt 0 ]; then
      return 0
    fi
    sleep 2
  done
  echo "Cycle $cycle did not recover to $expected_rows rows" >&2
  return 1
}

echo "Resilience: scaling streamers to 5"
minimum_cycle=$(sql "SELECT COALESCE(max(cycle_id), 1) FROM telemetry_events WHERE dataset_id='$dataset'")
kubectl --context "$context" -n "$namespace" scale deployment/"$streamer" --replicas=5
kubectl --context "$context" -n "$namespace" rollout status deployment/"$streamer" --timeout=120s
target_cycle=$(wait_for_partial_cycle "$minimum_cycle")
echo "Resilience: scaling down during partial cycle $target_cycle"
kubectl --context "$context" -n "$namespace" scale deployment/"$streamer" --replicas=1
kubectl --context "$context" -n "$namespace" rollout status deployment/"$streamer" --timeout=120s
wait_for_cycle_completion "$target_cycle"

echo "Resilience: restarting a collector while consumption is active"
kubectl --context "$context" -n "$namespace" scale deployment/"$collector" --replicas=2
kubectl --context "$context" -n "$namespace" rollout status deployment/"$collector" --timeout=120s
collector_pod=$(kubectl --context "$context" -n "$namespace" get pods -l app.kubernetes.io/component=collector -o jsonpath='{.items[0].metadata.name}')
kubectl --context "$context" -n "$namespace" delete pod "$collector_pod" --wait=false
kubectl --context "$context" -n "$namespace" rollout status deployment/"$collector" --timeout=120s

echo "Resilience: restarting the durable queue during publishing"
before=$(sql 'SELECT count(*) FROM telemetry_events')
kubectl --context "$context" -n "$namespace" delete pod "$queue-0" --wait=false
kubectl --context "$context" -n "$namespace" rollout status statefulset/"$queue" --timeout=120s
deadline=$(( $(date +%s) + 120 ))
after=$before
while [ "$(date +%s)" -lt "$deadline" ]; do
  after=$(sql 'SELECT count(*) FROM telemetry_events')
  [ "$after" -gt "$before" ] && break
  sleep 2
done
[ "$after" -gt "$before" ] || { echo "Ingestion did not resume after queue restart" >&2; exit 1; }

duplicates=$(sql 'SELECT count(*) FROM (SELECT source_event_key FROM telemetry_events GROUP BY source_event_key HAVING count(*) > 1) AS duplicates')
[ "$duplicates" -eq 0 ] || { echo "Found $duplicates duplicate source keys" >&2; exit 1; }

echo "Resilience: verifying Prometheus metrics on every component"
curl -fsS "$api_url/metrics" | grep -q 'gpu_telemetry_operations_total'
check_metrics statefulset/"$queue" 19090 9090
check_metrics deployment/"$streamer" 19091 9090
check_metrics deployment/"$collector" 19092 9090

echo "RESILIENCE E2E PASS: scale-down cycle $target_cycle recovered to $expected_rows rows; collector and queue restarts preserved progress; duplicate source keys=$duplicates; metrics verified"
