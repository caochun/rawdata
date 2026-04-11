#!/usr/bin/env bash
# Launch Hermes Web UI connected to this project's hermes-agent and loom data.
#
# Usage:
#   ./webui.sh                    # starts on default port 8787
#   ./webui.sh 8080               # custom port
#   ./webui.sh --data ~/my-crm    # specify data repo

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_DIR="$SCRIPT_DIR/vendor/hermes-agent"
WEBUI_DIR="$SCRIPT_DIR/vendor/hermes-webui"

# Parse --data flag
LOOM_ROOT="${LOOM_ROOT:-$SCRIPT_DIR}"
PORT="${HERMES_WEBUI_PORT:-8787}"
ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data)
            LOOM_ROOT="$(cd "$2" && pwd)"
            shift 2
            ;;
        [0-9]*)
            PORT="$1"
            shift
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
export HERMES_WEBUI_AGENT_DIR="$AGENT_DIR"
export HERMES_WEBUI_PYTHON="$SCRIPT_DIR/.venv/bin/python"
export HERMES_WEBUI_PORT="$PORT"
# Bypass proxy for local LLM server
unset ALL_PROXY all_proxy http_proxy https_proxy HTTP_PROXY HTTPS_PROXY SOCKS_PROXY socks_proxy

echo "Starting Hermes Web UI (data: $LOOM_ROOT, port: $PORT)"
exec bash "$WEBUI_DIR/start.sh" "$PORT" "${ARGS[@]}"
