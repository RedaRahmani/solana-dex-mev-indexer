#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[*] Stopping infra stack..."
docker compose -f infra/docker-compose.yml down
