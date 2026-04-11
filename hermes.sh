#!/usr/bin/env bash
# Launch Hermes Agent with loom plugin enabled.
#
# Usage:
#   ./hermes.sh                          # LOOM_ROOT defaults to cwd
#   ./hermes.sh --data ~/my-crm-data     # specify data repo explicitly
#   ./hermes.sh --data ~/my-crm-data chat -q "查询所有联系人" -t loom

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse --data flag before passing remaining args to hermes
LOOM_ROOT="${LOOM_ROOT:-$SCRIPT_DIR}"
ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data)
            LOOM_ROOT="$(cd "$2" && pwd)"
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

exec "$SCRIPT_DIR/.venv/bin/hermes" "${ARGS[@]}"
