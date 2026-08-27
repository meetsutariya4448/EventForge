#!/usr/bin/env bash
# Launches the 4 already-built Spring Boot jars as local JVM processes against the infra started
# by `docker compose` (see docker/docker-compose.yml), then polls each service's actuator health
# endpoint until it reports UP. Intended to be called via `make up`, after `./gradlew bootJar`.
#
# Uses parallel indexed arrays instead of associative arrays: macOS ships bash 3.2, which has no
# `declare -A` support.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT_DIR/.pids"
LOG_DIR="$ROOT_DIR/.logs"
mkdir -p "$PID_DIR" "$LOG_DIR"

VERSION="0.1.0-SNAPSHOT"
SERVICES=(order-service payment-service inventory-service notification-service)
PORTS=(8081 8082 8083 8084)

for i in "${!SERVICES[@]}"; do
  svc="${SERVICES[$i]}"
  jar="$ROOT_DIR/$svc/build/libs/${svc}-${VERSION}.jar"
  if [ ! -f "$jar" ]; then
    echo "Missing jar for $svc: $jar (did './gradlew bootJar' run first?)" >&2
    exit 1
  fi
  echo "Starting $svc on port ${PORTS[$i]}..."
  nohup java -jar "$jar" >"$LOG_DIR/$svc.log" 2>&1 &
  echo $! >"$PID_DIR/$svc.pid"
done

echo "Waiting for services to report healthy..."
for i in "${!SERVICES[@]}"; do
  svc="${SERVICES[$i]}"
  port="${PORTS[$i]}"
  deadline=$((SECONDS + 90))
  until curl -fs "http://localhost:${port}/actuator/health" 2>/dev/null | grep -q '"status":"UP"'; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "$svc did not become healthy within 90s (see $LOG_DIR/$svc.log)" >&2
      exit 1
    fi
    sleep 2
  done
  echo "$svc is healthy."
done

echo "All services healthy."
