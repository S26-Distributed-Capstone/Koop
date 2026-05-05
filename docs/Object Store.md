# KoopDB — Documentation Index

**Team Members:** Koop, Eitan, Yonatan

## Overview

KoopDB is a distributed, S3-compatible object store. Objects are split into Reed-Solomon erasure-coded shards and spread across a cluster of storage nodes; mutations are ordered through Kafka and persisted in RocksDB on each node. A stateless Query Processor fronts the cluster with the S3 HTTP API.

For a top-level summary of features and the system topology, see the project [README](../README.md).

## Project Plan

- [Scope (scenarios covered)](scope.md)
- [Distributed System Challenges](challenges.md)
- [Workflow Diagrams](workflow.md)
- [Software Architecture](architecture.md)
- [Tools & Technologies](technologies.md)

## Installation and Usage Guide

- [Installation Instructions](installation.md)
- [API and Usage Documentation](api.md)
- [Query Processor (API Gateway) module README](../query-processor/README.md)
