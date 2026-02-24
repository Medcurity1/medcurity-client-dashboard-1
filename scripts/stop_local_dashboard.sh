#!/bin/zsh
set -euo pipefail

PID_FILE="/tmp/medcurity_dashboard.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file found. Dashboard may already be stopped."
  exit 0
fi

PID="$(cat "$PID_FILE" || true)"
if [[ -n "${PID:-}" ]] && kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  sleep 1
  if kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" || true
  fi
  echo "Dashboard stopped (PID $PID)"
else
  echo "Process not running. Cleaning stale PID file."
fi

rm -f "$PID_FILE"
