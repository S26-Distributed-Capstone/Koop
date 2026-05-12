#!/bin/bash
# Configures passwordless doas for the sack user on all nodes.
# Run once before first deploy.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

ALL_NODES=(
    sack@192.168.8.11 sack@192.168.8.12 sack@192.168.8.13
    sack@192.168.8.101 sack@192.168.8.102 sack@192.168.8.103
    sack@192.168.8.104 sack@192.168.8.105 sack@192.168.8.106
    sack@192.168.8.108 sack@192.168.8.109 sack@192.168.8.110
    sack@192.168.8.111 sack@192.168.8.112
)

DOAS_RULE="permit nopass sack"

echo "=== Configuring passwordless doas on all nodes ==="
for node in "${ALL_NODES[@]}"; do
    echo "  -> ${node}"
    sshpass -p "${SSH_PASS}" ssh -t ${SSH_OPTS} "${node}" \
        "grep -qxF '${DOAS_RULE}' /etc/doas.d/doas.conf 2>/dev/null || echo '${DOAS_RULE}' | doas tee -a /etc/doas.d/doas.conf >/dev/null && doas chmod 600 /etc/doas.d/doas.conf"
done

echo ""
echo "=== Verifying ==="
for node in "${ALL_NODES[@]}"; do
    if sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} "${node}" "doas -n true" 2>/dev/null; then
        echo "  OK: ${node}"
    else
        echo "  FAIL: ${node}"
    fi
done

echo ""
echo "Done."
