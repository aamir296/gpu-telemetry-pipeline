#!/bin/sh
set -eu

provider=${CLUSTER_PROVIDER:-kind}
cluster=${CLUSTER_NAME:-gpu-telemetry-e2e}
release=${RELEASE_NAME:-telemetry}
namespace=${NAMESPACE:-gpu-telemetry}
image=${IMAGE_REPOSITORY:-gpu-telemetry}
tag=${IMAGE_TAG:-dev}
input_csv=${INPUT_CSV:-input/real-dcgm.csv}
streamer_replicas=${STREAMER_REPLICAS:-1}
collector_replicas=${COLLECTOR_REPLICAS:-1}
api_local_port=${API_LOCAL_PORT:-18080}
api_url="http://127.0.0.1:$api_local_port"
context=""

./scripts/doctor.sh
case "$input_csv" in /*|../*|*/../*) echo "INPUT_CSV must be a path inside the repository build context" >&2; exit 2;; esac
test -f "$input_csv" || { echo "INPUT_CSV does not exist: $input_csv" >&2; exit 2; }
docker build --build-arg INPUT_CSV="$input_csv" --tag "$image:$tag" .

if [ "$provider" = kind ]; then
  if ! kind get clusters 2>/dev/null | grep -Fx "$cluster" >/dev/null; then
    kind create cluster --name "$cluster" --wait 120s
  fi
  kind load docker-image "$image:$tag" --name "$cluster"
  context="kind-$cluster"
elif [ "$provider" = minikube ]; then
  minikube status --profile "$cluster" >/dev/null 2>&1 || minikube start --profile "$cluster"
  minikube image load "$image:$tag" --profile "$cluster"
  context="$cluster"
else
  echo "CLUSTER_PROVIDER must be kind or minikube" >&2
  exit 2
fi

helm upgrade --install "$release" ./deploy/helm/gpu-telemetry \
  --kube-context "$context" --namespace "$namespace" --create-namespace \
  --set image.repository="$image" --set image.tag="$tag" \
  --set streamer.replicas="$streamer_replicas" --set collector.replicas="$collector_replicas" \
  --wait --timeout 5m

# Local development intentionally reuses the tag, so replace application pods
# to guarantee they run the image that was just loaded into the node.
kubectl --context "$context" -n "$namespace" rollout restart \
  statefulset/"$release-gpu-telemetry-queue" \
  deployment/"$release-gpu-telemetry-streamer" \
  deployment/"$release-gpu-telemetry-collector" \
  deployment/"$release-gpu-telemetry-api"
kubectl --context "$context" -n "$namespace" rollout status statefulset/"$release-gpu-telemetry-queue" --timeout=120s
kubectl --context "$context" -n "$namespace" rollout status deployment/"$release-gpu-telemetry-streamer" --timeout=120s
kubectl --context "$context" -n "$namespace" rollout status deployment/"$release-gpu-telemetry-collector" --timeout=120s
kubectl --context "$context" -n "$namespace" rollout status deployment/"$release-gpu-telemetry-api" --timeout=120s

log_file=$(mktemp)
kubectl --context "$context" -n "$namespace" port-forward service/"$release-gpu-telemetry-api" "$api_local_port:8080" >"$log_file" 2>&1 &
forward_pid=$!
cleanup() {
  kill "$forward_pid" >/dev/null 2>&1 || true
  wait "$forward_pid" >/dev/null 2>&1 || true
  rm -f "$log_file"
}
trap cleanup EXIT INT TERM

api_ready=0
deadline=$(( $(date +%s) + 30 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if ! kill -0 "$forward_pid" >/dev/null 2>&1; then
    echo "API port-forward exited unexpectedly:" >&2
    cat "$log_file" >&2
    exit 1
  fi
  if curl -fsS "$api_url/healthz" >/dev/null 2>&1; then
    api_ready=1
    break
  fi
  sleep 1
done
if [ "$api_ready" -ne 1 ]; then
  echo "API did not become reachable through $api_url:" >&2
  cat "$log_file" >&2
  exit 1
fi

API_URL="$api_url" go run ./cmd/e2e

if [ "${RUN_RESILIENCE_E2E:-1}" = 1 ]; then
  API_URL="$api_url" ./scripts/resilience-e2e.sh
fi

echo "Cluster retained for inspection: kubectl --context $context -n $namespace get pods"
