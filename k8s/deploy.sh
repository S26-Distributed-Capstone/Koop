#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Change to the directory where the script is located to ensure paths resolve correctly
cd "$(dirname "$0")"

echo "Starting KoopDB deployment..."

echo "Applying namespace..."
kubectl apply -f 00-namespace.yaml

echo "Deploying Etcd cluster..."
kubectl apply -f 01-etcd.yaml

# Pause to allow Etcd quorum to form before dependent services connect
echo "Waiting 10 seconds for Etcd initialization..."
sleep 10

echo "Deploying Kafka..."
kubectl apply -f 02-kafka.yaml

echo "Deploying Redis..."
kubectl apply -f 03-redis.yaml

echo "Deploying Storage Nodes..."
kubectl apply -f 04-storage-nodes.yaml

echo "Deploying Etcd Seeder..."
kubectl apply -f 05-etcd-seeder.yaml

echo "Deploying Query Processors..."
kubectl apply -f 06-query-processor.yaml

echo "Deployment sequence complete."