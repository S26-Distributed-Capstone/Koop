#!/bin/bash

# 1. Fix permissions
sudo chown -R vscode:vscode /home/vscode/.m2 \
    /workspaces/*/target \
    /workspaces/*/common-lib/target \
    /workspaces/*/query-processor/target \
    /workspaces/*/storage-node/target \
    /workspaces/*/system-tests/target

# 2. Dynamically apply Testcontainers fix for Docker Desktop
if docker info -f '{{.OperatingSystem}}' 2>/dev/null | grep -q "Docker Desktop"; then
    echo "export TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal" >> ~/.bashrc
fi