#!/usr/bin/env bash
# Runs a 3-node cluster as local processes on ports 7001-7003.
#   scripts/local-cluster.sh start|stop|status
set -euo pipefail

BIN="${BIN:-./bin/raftkv}"
DATA="${DATA:-./data/local}"
PEERS="n1=http://127.0.0.1:7001,n2=http://127.0.0.1:7002,n3=http://127.0.0.1:7003"

start() {
  [[ -x "$BIN" ]] || { echo "missing $BIN; run 'make build' first" >&2; exit 1; }
  mkdir -p "$DATA"
  for i in 1 2 3; do
    if [[ -f "$DATA/n$i.pid" ]] && kill -0 "$(cat "$DATA/n$i.pid")" 2>/dev/null; then
      echo "n$i already running (pid $(cat "$DATA/n$i.pid"))"
      continue
    fi
    RAFTKV_ID="n$i" RAFTKV_PEERS="$PEERS" RAFTKV_LISTEN="127.0.0.1:700$i" \
      RAFTKV_DATA_DIR="$DATA/n$i" RAFTKV_LOG_FORMAT=text \
      nohup "$BIN" >"$DATA/n$i.log" 2>&1 &
    echo $! >"$DATA/n$i.pid"
    echo "started n$i on http://127.0.0.1:700$i (pid $!, log $DATA/n$i.log)"
  done
}

stop() {
  for i in 1 2 3; do
    if [[ -f "$DATA/n$i.pid" ]]; then
      kill "$(cat "$DATA/n$i.pid")" 2>/dev/null || true
      rm -f "$DATA/n$i.pid"
      echo "stopped n$i"
    fi
  done
}

status() {
  for i in 1 2 3; do
    curl -fsS "http://127.0.0.1:700$i/v1/status" || echo "n$i: down"
    echo
  done
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
