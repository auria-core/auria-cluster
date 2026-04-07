# auria-cluster

Cluster coordination for distributed execution in AURIA Runtime Core.

## Overview

Manages worker nodes and coordinates distributed expert execution across a cluster.

## Cluster Architecture

```
Coordinator Node
├── Worker Node A
├── Worker Node B
└── Worker Node C
```

## Usage

```rust
use auria_cluster::{ClusterCoordinator, WorkerNode};

let mut coordinator = ClusterCoordinator::new("coordinator-001");
coordinator.add_worker(WorkerNode {
    id: "worker-001".to_string(),
    address: "192.168.1.1:50051".to_string(),
    capabilities: Tier::Standard,
});
```
