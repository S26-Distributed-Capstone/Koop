#!/bin/bash
# Deletes all koopdb resources from the cluster.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

do_ssh() {
    sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} "$@"
}

echo "=== Deleting koopdb namespace (removes all resources) ==="
do_ssh "${SERVER}" "doas k3s kubectl delete namespace koopdb --ignore-not-found"

echo ""
echo "Done. All koopdb resources removed."
