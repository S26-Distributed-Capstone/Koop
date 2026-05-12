#!/bin/bash
# Live-updating status view of koopdb resources. Refreshes every 2s.
# Ctrl-C to exit.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

INTERVAL="${1:-2}"

# Run a redraw loop on the remote so we keep a single SSH connection open.
sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} -t "${SERVER}" "
    while true; do
        clear
        echo '=== koopdb @ \$(date) (refresh ${INTERVAL}s, Ctrl-C to exit) ==='
        echo
        echo '--- Pods ---'
        doas k3s kubectl get pods -n koopdb -o wide
        echo
        echo '--- Services ---'
        doas k3s kubectl get svc -n koopdb
        echo
        echo '--- StatefulSets / Deployments ---'
        doas k3s kubectl get statefulsets,deployments -n koopdb
        sleep ${INTERVAL}
    done
"
