#!/bin/bash
# Labels nodes and applies k8s manifests to the cluster.
#
# Usage:
#   ./deploy.sh
#
# Prerequisites:
#   - k3s cluster already running
#   - Images already pushed (run ./push-images.sh first)
#   - sshpass installed locally
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOSTS_FILE="${SCRIPT_DIR}/hosts.conf"

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

mapfile -t HOSTS < <(grep -v '^\s*#' "${HOSTS_FILE}" | grep -v '^\s*$')

if [ ${#HOSTS[@]} -eq 0 ]; then
    echo "Error: No hosts found in ${HOSTS_FILE}."
    exit 1
fi

SERVER="${HOSTS[0]}"

echo "=== Labeling nodes ==="
"${SCRIPT_DIR}/label-nodes.sh"

echo ""
echo "=== Applying manifests from server node (${SERVER}) ==="
do_ssh "${SERVER}" "mkdir -p /tmp/koopdb-k8s"
do_scp -q "${SCRIPT_DIR}"/*.yaml "${SERVER}:/tmp/koopdb-k8s/"

do_ssh "${SERVER}" 'for f in $(ls /tmp/koopdb-k8s/*.yaml | sort); do
    echo "  applying $(basename $f)..."
    sudo k3s kubectl apply -f "$f"
done
rm -rf /tmp/koopdb-k8s'

echo ""
echo "=== Deploy complete ==="
do_ssh "${SERVER}" "sudo k3s kubectl get pods -n koopdb -o wide"
