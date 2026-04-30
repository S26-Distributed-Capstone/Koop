#!/bin/bash
# Build Docker images and import them into k3s (which uses containerd, not Docker daemon).
set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Building query-processor..."
docker build -t koopdb/query-processor:latest -f "${REPO_ROOT}/query-processor/Dockerfile" "${REPO_ROOT}"

echo "Building storage-node..."
docker build -t koopdb/storage-node:latest -f "${REPO_ROOT}/storage-node/Dockerfile" "${REPO_ROOT}"

echo "Importing images into k3s..."
docker save koopdb/query-processor:latest | sudo k3s ctr images import -
docker save koopdb/storage-node:latest    | sudo k3s ctr images import -

echo "Done. Images available in k3s:"
sudo k3s ctr images list | grep koopdb
