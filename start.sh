#!/bin/sh
set -eu

node /opt/pot/build/main.js --host 127.0.0.1 --port 4416 &
POT_PID=$!

cleanup() {
  kill "$POT_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Give the local PO-token provider a moment to bind.
sleep 1

exec /opt/venv/bin/uvicorn app:app --host 0.0.0.0 --port "${PORT:-8080}"
