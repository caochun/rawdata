#!/usr/bin/env bash
# Launch Hermes Agent with loom plugin enabled.
#
# Usage:
#   ./hermes.sh                               # interactive chat, LOOM_ROOT=cwd
#   ./hermes.sh --data ~/my-crm-data          # specify data repo
#   ./hermes.sh --data ~/my-crm-data chat -q "查询所有联系人" -t loom
#   ./hermes.sh --a2a                         # start A2A server (port 8100)
#   ./hermes.sh --a2a --port 8200 --data ~/my-crm-data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse loom-specific flags before passing remaining args to hermes
LOOM_ROOT="${LOOM_ROOT:-$SCRIPT_DIR}"
A2A_MODE=0
A2A_HOST="0.0.0.0"
A2A_PORT=8100
A2A_NAME=""
ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --data)
            LOOM_ROOT="$(cd "$2" && pwd)"
            shift 2
            ;;
        --a2a)
            A2A_MODE=1
            shift
            ;;
        --host)
            A2A_HOST="$2"
            shift 2
            ;;
        --port)
            A2A_PORT="$2"
            shift 2
            ;;
        --name)
            A2A_NAME="$2"
            shift 2
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

export LOOM_ROOT
export HERMES_ENABLE_PROJECT_PLUGINS=1
export HERMES_HOME="$SCRIPT_DIR/.hermes"
# Bypass proxy for local LLM server
unset ALL_PROXY all_proxy http_proxy https_proxy HTTP_PROXY HTTPS_PROXY SOCKS_PROXY socks_proxy

if [[ "$A2A_MODE" -eq 1 ]]; then
    echo "Starting loom A2A server on ${A2A_HOST}:${A2A_PORT} (data: ${LOOM_ROOT})"
    EXTRA_ARGS=(--host "$A2A_HOST" --port "$A2A_PORT")
    [[ -n "$A2A_NAME" ]] && EXTRA_ARGS+=(--name "$A2A_NAME")
    exec "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/.hermes/a2a_adapter/__main__.py" "${EXTRA_ARGS[@]}"
else
    exec "$SCRIPT_DIR/.venv/bin/hermes" "${ARGS[@]}"
fi
