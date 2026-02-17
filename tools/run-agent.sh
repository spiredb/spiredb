#!/usr/bin/env bash
set -euo pipefail

# Resolve repo root relative to this script (works from any directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
AGENT_DIR="$REPO_ROOT/compute/spire-ai/examples/coding-agent"

NAMESPACE="${SPIRE_NAMESPACE:-spire}"

# Parse --project / -p early so we can default to $PWD (the caller's directory)
PROJECT=""
EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --project|-p)
            PROJECT="$2"
            shift 2
            ;;
        --project=*)
            PROJECT="${1#*=}"
            shift
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

# Default project to caller's working directory
PROJECT="${PROJECT:-$(pwd)}"

# Resolve to absolute path
PROJECT="$(cd "$PROJECT" 2>/dev/null && pwd)" || {
    echo "Error: project directory does not exist: $PROJECT" >&2
    exit 1
}

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
cargo run --manifest-path "$AGENT_DIR/Cargo.toml" -- --project "$PROJECT" "${EXTRA_ARGS[@]}"
