#!/bin/bash
# Labels nodes and applies k8s manifests to the cluster.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

do_ssh() {
    sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} "$@"
}

do_scp() {
    sshpass -p "${SSH_PASS}" scp ${SSH_OPTS} "$@"
}

echo "=== Labeling nodes ==="
"${SCRIPT_DIR}/label-nodes.sh"

echo ""
echo "=== Applying manifests ==="
do_ssh "${SERVER}" "mkdir -p /tmp/koopdb-k8s/etcd-data"
do_scp -q "${SCRIPT_DIR}"/*.yaml "${SERVER}:/tmp/koopdb-k8s/"
do_scp -q "${SCRIPT_DIR}"/etcd-data/*.json "${SERVER}:/tmp/koopdb-k8s/etcd-data/"

# Patch image references with the configured Docker Hub user
do_ssh "${SERVER}" "sed -i 's|image: .*/query-processor:|image: ${DOCKERHUB_USER}/query-processor:|g; s|image: .*/storage-node:|image: ${DOCKERHUB_USER}/storage-node:|g' /tmp/koopdb-k8s/04-storage-nodes.yaml /tmp/koopdb-k8s/06-query-processor.yaml"

# Apply namespace first so the ConfigMap creation below has a target namespace
echo "  applying 00-namespace.yaml..."
do_ssh "${SERVER}" 'doas k3s kubectl apply -f /tmp/koopdb-k8s/00-namespace.yaml'

# Generate the etcd-seeder ConfigMap from the JSON files (idempotent via dry-run + apply)
echo "  generating etcd-seeder-config ConfigMap..."
do_ssh "${SERVER}" 'doas k3s kubectl create configmap etcd-seeder-config \
  --namespace=koopdb \
  --from-file=/tmp/koopdb-k8s/etcd-data/ \
  --dry-run=client -o yaml | doas k3s kubectl apply -f -'

# Apply remaining manifests in order (00-namespace.yaml is a no-op on re-apply)
do_ssh "${SERVER}" 'for f in $(ls /tmp/koopdb-k8s/*.yaml | sort); do
    echo "  applying $(basename $f)..."
    doas k3s kubectl apply -f "$f"
done
rm -rf /tmp/koopdb-k8s'

echo ""
echo "=== Deploy complete ==="
do_ssh "${SERVER}" "doas k3s kubectl get pods -n koopdb -o wide"
