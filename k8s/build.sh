#!/bin/bash
# Builds Docker images and pushes them to Docker Hub.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Building query-processor..."
docker build -t "${DOCKERHUB_USER}/query-processor:latest" -f "${REPO_ROOT}/query-processor/Dockerfile" "${REPO_ROOT}"

echo "Building storage-node..."
docker build -t "${DOCKERHUB_USER}/storage-node:latest" -f "${REPO_ROOT}/storage-node/Dockerfile" "${REPO_ROOT}"

echo "Pushing to Docker Hub (${DOCKERHUB_USER})..."
docker push "${DOCKERHUB_USER}/query-processor:latest"
docker push "${DOCKERHUB_USER}/storage-node:latest"

echo "Done."
