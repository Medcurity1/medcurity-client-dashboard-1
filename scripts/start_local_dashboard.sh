#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="/tmp/medcurity_dashboard.pid"
LOG_FILE="/tmp/medcurity_dashboard.log"

if [[ -f "$PID_FILE" ]]; then
  OLD_PID="$(cat "$PID_FILE" || true)"
  if [[ -n "${OLD_PID:-}" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    echo "Dashboard already running (PID $OLD_PID)"
    exit 0
  fi
fi

cd "$ROOT_DIR"
nohup python3 -c "from app import app, PORT; app.run(host='0.0.0.0', port=PORT, debug=False)" >"$LOG_FILE" 2>&1 &
NEW_PID="$!"
echo "$NEW_PID" > "$PID_FILE"
sleep 1

if kill -0 "$NEW_PID" 2>/dev/null; then
  echo "Dashboard started (PID $NEW_PID)"
  echo "URL: http://192.168.1.95:8080"
else
  echo "Dashboard failed to start. Check: $LOG_FILE"
  exit 1
fi
