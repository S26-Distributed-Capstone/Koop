#!/bin/bash
# Tail logs for a koopdb pod. Usage: bash k8s/logs.sh <pod-name> [tail-lines]
#   bash k8s/logs.sh kafka-0
#   bash k8s/logs.sh kafka-0 500
#   bash k8s/logs.sh kafka-0 -f          # follow
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

POD="${1:?usage: logs.sh <pod-name> [tail-lines | -f]}"
ARG="${2:-200}"

if [ "${ARG}" = "-f" ]; then
    sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} -t "${SERVER}" \
        "doas k3s kubectl logs -n koopdb ${POD} -f"
else
    sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} "${SERVER}" \
        "echo '=== logs (tail ${ARG}) ==='; doas k3s kubectl logs -n koopdb ${POD} --tail=${ARG}; echo; echo '=== describe (last 40 lines) ==='; doas k3s kubectl describe pod -n koopdb ${POD} | tail -40"
fi
