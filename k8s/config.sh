#!/bin/bash
# Shared configuration for all k8s scripts.
DOCKERHUB_USER="ygins"
SSH_PASS="sack"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SERVER="sack@192.168.8.11"
