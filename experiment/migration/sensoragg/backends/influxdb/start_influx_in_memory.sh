#!/usr/bin/env bash
# Start an InfluxDB v2 container with data dir on tmpfs to approximate in-memory behaviour
# Requires docker
PORT=${1:-8086}
NAME=${2:-influx-in-memory}
set -euo pipefail

echo "Starting InfluxDB in-memory container '${NAME}' on port ${PORT}'"
# Use tmpfs for /var/lib/influxdb2 to keep data ephemeral and in-memory
# Note: container needs to be configured (tokens/org) if used; tests expect ephemeral write/read only
docker run -d --rm --name "${NAME}" -p ${PORT}:8086 --tmpfs /var/lib/influxdb2:rw,size=64m influxdb:2

if [ $? -eq 0 ]; then
    echo "InfluxDB started: ${NAME}"
else
    echo "Failed to start InfluxDB container" >&2
    exit 1
fi
