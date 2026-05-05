#!/bin/bash
# Labels k3s nodes so that workloads schedule on the correct machines.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

CP_IPS=(192.168.8.11 192.168.8.12 192.168.8.13)
WORKER_IPS=(192.168.8.101 192.168.8.102 192.168.8.103 192.168.8.104 192.168.8.105 192.168.8.106 192.168.8.108 192.168.8.109 192.168.8.110 192.168.8.111 192.168.8.112)

do_ssh() {
    sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} "$@"
}

echo "Fetching node names from k3s..."
NODE_LIST=$(do_ssh "${SERVER}" "doas k3s kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name} {.status.addresses[?(@.type==\"InternalIP\")].address}{\"\n\"}{end}'")

label_node() {
    local ip="$1"
    local role="$2"
    local name
    name=$(echo "${NODE_LIST}" | awk -v ip="${ip}" '$2 == ip {print $1}')
    if [ -z "${name}" ]; then
        echo "  WARN: no k3s node found for IP ${ip}, skipping"
        return
    fi
    echo "  ${name} (${ip}) -> koopdb/role=${role}"
    do_ssh "${SERVER}" "doas k3s kubectl label node ${name} koopdb/role=${role} --overwrite"
}

echo "=== Labeling CP nodes ==="
for ip in "${CP_IPS[@]}"; do
    label_node "${ip}" "cp"
done

echo ""
echo "=== Labeling worker nodes ==="
for ip in "${WORKER_IPS[@]}"; do
    label_node "${ip}" "worker"
done

echo ""
echo "Done. Current labels:"
do_ssh "${SERVER}" "doas k3s kubectl get nodes -L koopdb/role"
