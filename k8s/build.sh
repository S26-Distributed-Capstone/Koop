#!/bin/bash
# Builds Docker images locally and saves them as tars for transfer to remote hosts.
set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${REPO_ROOT}/k8s/images"
mkdir -p "${OUT_DIR}"

echo "Building query-processor..."
docker build -t koopdb/query-processor:latest -f "${REPO_ROOT}/query-processor/Dockerfile" "${REPO_ROOT}"

echo "Building storage-node..."
docker build -t koopdb/storage-node:latest -f "${REPO_ROOT}/storage-node/Dockerfile" "${REPO_ROOT}"

echo "Saving images as tars..."
docker save koopdb/query-processor:latest -o "${OUT_DIR}/query-processor.tar"
docker save koopdb/storage-node:latest    -o "${OUT_DIR}/storage-node.tar"

echo "Done. Images saved to ${OUT_DIR}/"
ls -lh "${OUT_DIR}"/*.tar
