#!/usr/bin/env bash
# Start chat-ui, proxying to hermes agent at port 8642 by default.
# Workspace directory is read from LOOM_ROOT env var (set by hermes.sh).
# Usage: ./start.sh [extra args passed to server.py]
set -e
cd "$(dirname "$0")"
PORT="${PORT:-9191}"
AGENT_URL="${AGENT_URL:-http://127.0.0.1:8642/v1}"
exec python server.py --port "$PORT" --agent-url "$AGENT_URL" "$@"
