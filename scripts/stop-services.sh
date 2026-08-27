#!/usr/bin/env bash
# Stops the local JVM processes started by scripts/start-services.sh, using the PIDs it recorded.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT_DIR/.pids"

if [ ! -d "$PID_DIR" ]; then
  echo "No running services recorded."
  exit 0
fi

for pidfile in "$PID_DIR"/*.pid; do
  [ -e "$pidfile" ] || continue
  svc="$(basename "$pidfile" .pid)"
  pid="$(cat "$pidfile")"
  if kill -0 "$pid" 2>/dev/null; then
    echo "Stopping $svc (pid $pid)..."
    kill "$pid"
    wait "$pid" 2>/dev/null || true
  fi
  rm -f "$pidfile"
done

echo "All services stopped."
