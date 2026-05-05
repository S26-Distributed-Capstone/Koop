#!/bin/bash
# Shows the status of koopdb resources in the cluster.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

do_ssh() {
    sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} "$@"
}

echo "=== Pods ==="
do_ssh "${SERVER}" "doas k3s kubectl get pods -n koopdb -o wide"

echo ""
echo "=== Services ==="
do_ssh "${SERVER}" "doas k3s kubectl get svc -n koopdb"

echo ""
echo "=== StatefulSets / Deployments ==="
do_ssh "${SERVER}" "doas k3s kubectl get statefulsets,deployments -n koopdb"
