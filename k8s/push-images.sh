#!/bin/bash
# Transfers image tars to worker nodes via SSH and imports them into k3s.
#
# Usage:
#   ./push-images.sh
#
# Prerequisites:
#   - Run ./build.sh first to create image tars
#   - sshpass installed locally
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOSTS_FILE="${SCRIPT_DIR}/hosts.conf"
IMAGE_DIR="${SCRIPT_DIR}/images"
REMOTE_TMP="/tmp/koopdb-images"

SSH_PASS="sack"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

do_ssh() {
    sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} "$@"
}

do_scp() {
    sshpass -p "${SSH_PASS}" scp ${SSH_OPTS} "$@"
}

if [ ! -f "${HOSTS_FILE}" ]; then
    echo "Error: ${HOSTS_FILE} not found."
    exit 1
fi

if [ ! -f "${IMAGE_DIR}/query-processor.tar" ] || [ ! -f "${IMAGE_DIR}/storage-node.tar" ]; then
    echo "Error: Image tars not found in ${IMAGE_DIR}/."
    echo "Run ./build.sh first."
    exit 1
fi

mapfile -t HOSTS < <(grep -v '^\s*#' "${HOSTS_FILE}" | grep -v '^\s*$')

# Only worker nodes need the custom images.
WORKER_HOSTS=()
for host in "${HOSTS[@]}"; do
    ip="${host#*@}"
    case "${ip}" in
        192.168.8.10[1-9]|192.168.8.11[0-2]) WORKER_HOSTS+=("${host}") ;;
    esac
done

echo "=== Transferring images to ${#WORKER_HOSTS[@]} worker node(s) ==="
for host in "${WORKER_HOSTS[@]}"; do
    echo "  -> ${host}"
    do_ssh "${host}" "mkdir -p ${REMOTE_TMP}"
    do_scp -q "${IMAGE_DIR}/query-processor.tar" "${IMAGE_DIR}/storage-node.tar" "${host}:${REMOTE_TMP}/"
done

echo ""
echo "=== Importing images into k3s on worker nodes ==="
for host in "${WORKER_HOSTS[@]}"; do
    echo "  -> ${host}"
    do_ssh "${host}" "sudo k3s ctr images import ${REMOTE_TMP}/query-processor.tar && \
                      sudo k3s ctr images import ${REMOTE_TMP}/storage-node.tar && \
                      rm -rf ${REMOTE_TMP}"
done

echo ""
echo "Done. Images available on all worker nodes."
