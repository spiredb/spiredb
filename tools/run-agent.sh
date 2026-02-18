#!/usr/bin/env bash
set -euo pipefail

# Convenience wrapper: runs the coding-agent example.
# Parses --project/-p so it can be passed without the "--" separator.
#
# Usage: ./tools/run-agent.sh -p /path/to/project
#        ./tools/run-agent.sh --project /path/to/project

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse --project / -p early so we can default to $PWD
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

exec "$SCRIPT_DIR/run-example.sh" coding-agent -- --project "$PROJECT" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}
