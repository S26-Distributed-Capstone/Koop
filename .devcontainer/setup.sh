#!/bin/bash

# 1. Fix permissions on mounted volumes
# Explicit paths are used because the glob /workspaces/*/module/target only
# matches one level deep and misses nested module directories.
sudo mkdir -p \
    /workspaces/Koop/target \
    /workspaces/Koop/common-lib/target \
    /workspaces/Koop/query-processor/target \
    /workspaces/Koop/storage-node/target \
    /workspaces/Koop/system-tests/target \
    /home/vscode/.m2

sudo chown -R vscode:vscode \
    /workspaces/Koop/target \
    /workspaces/Koop/common-lib/target \
    /workspaces/Koop/query-processor/target \
    /workspaces/Koop/storage-node/target \
    /workspaces/Koop/system-tests/target \
    /home/vscode/.m2

# 2. Dynamically apply Testcontainers fix for Docker Desktop
if docker info -f '{{.OperatingSystem}}' 2>/dev/null | grep -q "Docker Desktop"; then
    echo "export TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal" >> ~/.bashrc
fi

# 3. Start a Redis container for local development and testing
if ! docker ps --format '{{.Names}}' | grep -q '^redis-test$'; then
    echo "Starting redis-test container..."
    docker run -d \
        --name redis-test \
        --restart unless-stopped \
        -p 6379:6379 \
        redis:7-alpine
    echo "redis-test started on localhost:6379"
else
    echo "redis-test already running"
fi