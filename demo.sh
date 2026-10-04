#!/usr/bin/env bash
# End-to-end demo: start broker, create topic, produce via TCP, consume via gRPC,
# restart broker, verify records replay from disk.
set -euo pipefail

cd "$(dirname "$0")"

DATA_DIR="${DATA_DIR:-./data-demo}"
TCP_ADDR="${TCP_ADDR:-localhost:9092}"
GRPC_ADDR="${GRPC_ADDR:-localhost:9093}"
BIN_DIR="./bin"

cleanup() {
  if [[ -n "${BROKER_PID:-}" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

rm -rf "$DATA_DIR"

echo "==> building binaries into $BIN_DIR"
mkdir -p "$BIN_DIR"
go build -o "$BIN_DIR/broker" ./cmd/broker
go build -o "$BIN_DIR/producer" ./cmd/producer
go build -o "$BIN_DIR/consumer" ./cmd/consumer

wait_for_tcp() {
  local host_port=$1
  local host=${host_port%:*}
  local port=${host_port#*:}
  for _ in $(seq 1 50); do
    if (echo > "/dev/tcp/${host}/${port}") 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for $host_port" >&2
  return 1
}

echo "==> starting broker"
"$BIN_DIR/broker" -data "$DATA_DIR" -tcp "$TCP_ADDR" -grpc "$GRPC_ADDR" &
BROKER_PID=$!
wait_for_tcp "$TCP_ADDR"
wait_for_tcp "$GRPC_ADDR"

echo "==> creating topic 'events' with 3 partitions"
"$BIN_DIR/producer" -addr "$TCP_ADDR" -topic events -create-topic -partitions 3

echo "==> producing 30 messages"
seq 1 30 | "$BIN_DIR/producer" -addr "$TCP_ADDR" -topic events

echo "==> consuming partition 0 (non-follow)"
"$BIN_DIR/consumer" -addr "$GRPC_ADDR" -topic events -partition 0 -follow=false

echo "==> consuming partition 1 (non-follow)"
"$BIN_DIR/consumer" -addr "$GRPC_ADDR" -topic events -partition 1 -follow=false

echo "==> consuming partition 2 (non-follow)"
"$BIN_DIR/consumer" -addr "$GRPC_ADDR" -topic events -partition 2 -follow=false

echo "==> stopping broker"
kill "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
unset BROKER_PID

echo "==> log directory on disk:"
ls -la "$DATA_DIR"/events/0

echo "==> restarting broker"
"$BIN_DIR/broker" -data "$DATA_DIR" -tcp "$TCP_ADDR" -grpc "$GRPC_ADDR" &
BROKER_PID=$!
wait_for_tcp "$TCP_ADDR"
wait_for_tcp "$GRPC_ADDR"

echo "==> replaying partition 0 from offset 0 (proves durability)"
"$BIN_DIR/consumer" -addr "$GRPC_ADDR" -topic events -partition 0 -follow=false

echo "==> demo complete"
