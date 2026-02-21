#!/usr/bin/env bash
set -euo pipefail

# Generic runner for spire-ai examples.
# Sets up SpireDB port-forwards, then builds and runs the specified example.
#
# Usage: ./tools/run-example.sh <example-name> [-- extra args...]
#   e.g. ./tools/run-example.sh coding-agent -- --project /path/to/project
#        ./tools/run-example.sh code-search -- index ./src
#        ./tools/run-example.sh doc-qa -- interactive

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXAMPLES_DIR="$REPO_ROOT/compute/spire-ai/examples"

NAMESPACE="${SPIRE_NAMESPACE:-spire}"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <example-name> [-- extra args...]"
    echo ""
    echo "Available examples:"
    for d in "$EXAMPLES_DIR"/*/; do
        name="$(basename "$d")"
        echo "  $name"
    done
    exit 1
fi

EXAMPLE="$1"
shift

EXAMPLE_DIR="$EXAMPLES_DIR/$EXAMPLE"

if [[ ! -d "$EXAMPLE_DIR" ]]; then
    echo "Error: example '$EXAMPLE' not found at $EXAMPLE_DIR" >&2
    echo ""
    echo "Available examples:"
    for d in "$EXAMPLES_DIR"/*/; do
        name="$(basename "$d")"
        echo "  $name"
    done
    exit 1
fi

# Strip optional "--" separator
if [[ "${1:-}" == "--" ]]; then
    shift
fi

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

# Build and run the example
cargo run --manifest-path "$EXAMPLE_DIR/Cargo.toml" -- "$@"
