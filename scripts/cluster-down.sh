#!/bin/sh
set -eu
provider=${CLUSTER_PROVIDER:-kind}
cluster=${CLUSTER_NAME:-gpu-telemetry-e2e}
if [ "$provider" = kind ]; then
  kind delete cluster --name "$cluster"
elif [ "$provider" = minikube ]; then
  minikube delete --profile "$cluster"
else
  echo "CLUSTER_PROVIDER must be kind or minikube" >&2
  exit 2
fi
