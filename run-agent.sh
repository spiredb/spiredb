#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${SPIRE_NAMESPACE:-spire}"
PROJECT="${1:-.}"

cleanup() {
    echo ""
    echo "Stopping port-forwards..."
    kill $PF_PD $PF_DATA 2>/dev/null || true
    wait $PF_PD $PF_DATA 2>/dev/null || true
}
trap cleanup EXIT

# Port-forward SpireDB gRPC services
echo "Forwarding SpireDB ports from namespace '$NAMESPACE'..."
kubectl port-forward -n "$NAMESPACE" svc/spire-spiredb-headless 50051:50051 &>/dev/null &
PF_PD=$!
kubectl port-forward -n "$NAMESPACE" svc/spire-spiredb 50052:50052 &>/dev/null &
PF_DATA=$!

# Wait for ports to be ready
for port in 50051 50052; do
    for _ in $(seq 1 30); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
            break
        fi
        sleep 0.2
    done
done

echo "Ports ready: 50051 (PD), 50052 (DataAccess)"
echo ""

# Build and run the coding agent
cd compute/spire-ai/examples/coding-agent
cargo run -- --project "$PROJECT" "${@:2}"
