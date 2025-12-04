#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[*] Starting Kafka + ClickHouse + Prometheus + Grafana..."
docker compose -f infra/docker-compose.yml up -d

echo
echo "[*] Running containers:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
