#!/bin/bash
# Deploy or update the history-cache-agent sidecar on an Oracle/Aliyun VPS.
# Matches cloud_gateway_v2's docker run deployment pattern (no compose).
#
# Usage: deploy-agent.sh <image-digest> [bake_seconds]
#
# Prerequisites on the VPS:
#   /etc/history-cache/r2-agent.env  (R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)
#
# After this script, cloud_gateway_v2's docker run must add:
#   -v /var/lib/history-cache/socket:/run/history-cache

set -euo pipefail

IMAGE="$1"
BAKE="${2:-120}"
CONTAINER="history-cache-agent"
SOCKET_DIR="/var/lib/history-cache/socket"
ENV_FILE="/etc/history-cache/r2-agent.env"

case "$IMAGE" in
  *@sha256:*) ;;
  *) echo "image must use an immutable digest (...@sha256:...)" >&2; exit 1 ;;
esac

[ -f "$ENV_FILE" ] || { echo "missing $ENV_FILE" >&2; exit 1; }

source "$ENV_FILE"
[ -n "$R2_ACCOUNT_ID" ] && [ -n "$R2_ACCESS_KEY_ID" ] && [ -n "$R2_SECRET_ACCESS_KEY" ] \
  || { echo "R2 credentials incomplete in $ENV_FILE" >&2; exit 1; }

mkdir -p "$SOCKET_DIR"

# Graceful stop if already running
docker stop "$CONTAINER" 2>/dev/null || true
docker rm "$CONTAINER" 2>/dev/null || true

docker pull "$IMAGE"

docker run -d \
  --name "$CONTAINER" \
  --restart unless-stopped \
  --network host \
  --memory 256m \
  --cpus 0.5 \
  -v "$SOCKET_DIR":/run/history-cache \
  --env R2_ACCOUNT_ID="$R2_ACCOUNT_ID" \
  --env R2_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" \
  --env R2_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" \
  "$IMAGE" \
  --socket /run/history-cache/agent.sock \
  --account "$R2_ACCOUNT_ID" \
  --access-key-id "$R2_ACCESS_KEY_ID" \
  --secret-access-key "$R2_SECRET_ACCESS_KEY"

echo "deployed $CONTAINER from $IMAGE"

# Health check: agent socket should appear within 5 seconds
for i in $(seq 1 10); do
  if [ -S "$SOCKET_DIR/agent.sock" ]; then
    echo "agent socket ready"
    echo "baking for ${BAKE}s..."
    sleep "$BAKE"
    if docker ps --filter "name=^/${CONTAINER}$" --filter "status=running" --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
      echo "PASS: agent is running"
      exit 0
    fi
    echo "FAIL: agent stopped during bake" >&2
    exit 1
  fi
  sleep 1
done

echo "FAIL: agent socket did not appear" >&2
docker logs "$CONTAINER" --tail 20 || true
exit 1
