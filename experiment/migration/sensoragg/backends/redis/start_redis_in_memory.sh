#!/usr/bin/env bash
# Start a Redis instance in-memory (no persistence) using docker (requires docker available)
# Usage: ./start_redis_in_memory.sh [<port> [<container_name>]]
PORT=${1:-6379}
NAME=${2:-redis-in-memory}
set -euo pipefail

echo "Starting Redis in-memory container '${NAME}' on port ${PORT}"
# Disable RDB snapshots and AOF to keep DB in-memory-only
docker run -d --rm --name "${NAME}" -p ${PORT}:6379 redis:6 redis-server --save "" --appendonly no

if [ $? -eq 0 ]; then
    echo "Redis started: ${NAME}"
else
    echo "Failed to start Redis container" >&2
    exit 1
fi
