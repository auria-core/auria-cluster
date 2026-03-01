// File: lib.rs - This file is part of AURIA
// Copyright (c) 2026 AURIA Developers and Contributors
// Description:
//     Cluster coordination for distributed execution in AURIA Runtime Core.
//     Manages worker nodes and coordinates distributed expert execution
//     across a cluster for the Max tier.
//
use auria_core::{AuriaError, AuriaResult, ExpertId, RequestId, ShardId, Tier};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct ClusterCoordinator {
    node_id: String,
    workers: Arc<RwLock<Vec<WorkerNode>>>,
    expert_assignments: Arc<RwLock<HashMap<ExpertId, String>>>,
}

#[derive(Clone, Debug)]
pub struct WorkerNode {
    pub id: String,
    pub address: String,
    pub capabilities: Tier,
    pub status: WorkerStatus,
    pub load: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkerStatus {
    Idle,
    Busy,
    Offline,
}

#[derive(Clone)]
pub struct ClusterTask {
    pub task_id: RequestId,
    pub expert_ids: Vec<ExpertId>,
    pub assigned_worker: String,
    pub status: TaskStatus,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
}

impl ClusterCoordinator {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            workers: Arc::new(RwLock::new(Vec::new())),
            expert_assignments: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn add_worker(&self, worker: WorkerNode) -> AuriaResult<()> {
        let mut workers = self.workers.write().await;
        
        if workers.iter().any(|w| w.id == worker.id) {
            return Err(AuriaError::ClusterError(
                format!("Worker {} already exists", worker.id),
            ));
        }
        
        workers.push(worker);
        Ok(())
    }

    pub async fn remove_worker(&self, worker_id: &str) -> AuriaResult<()> {
        let mut workers = self.workers.write().await;
        let initial_len = workers.len();
        workers.retain(|w| w.id != worker_id);
        
        if workers.len() == initial_len {
            return Err(AuriaError::ClusterError(
                format!("Worker {} not found", worker_id),
            ));
        }
        
        Ok(())
    }

    pub async fn get_available_workers(&self) -> Vec<WorkerNode> {
        let workers = self.workers.read().await;
        workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Idle && w.capabilities == Tier::Max)
            .cloned()
            .collect()
    }

    pub async fn distribute_experts(
        &self,
        expert_ids: &[ExpertId],
    ) -> AuriaResult<Vec<(WorkerNode, Vec<ExpertId>)>> {
        let workers = self.workers.read().await;
        
        if workers.is_empty() {
            return Err(auria_core::AuriaError::ClusterError(
                "No workers available".to_string(),
            ));
        }

        let available: Vec<_> = workers
            .iter()
            .filter(|w| w.capabilities == Tier::Max && w.status == WorkerStatus::Idle)
            .collect();

        if available.is_empty() {
            return Err(auria_core::AuriaError::ClusterError(
                "No available workers with Max tier capability".to_string(),
            ));
        }

        let mut distribution = Vec::new();
        for (i, expert_id) in expert_ids.iter().enumerate() {
            let worker = available[i % available.len()].clone();
            distribution.push((worker, vec![*expert_id]));
        }

        Ok(distribution)
    }

    pub async fn assign_expert(&self, expert_id: ExpertId, worker_id: String) -> AuriaResult<()> {
        let mut assignments = self.expert_assignments.write().await;
        assignments.insert(expert_id, worker_id);
        Ok(())
    }

    pub async fn get_expert_location(&self, expert_id: ExpertId) -> Option<String> {
        let assignments = self.expert_assignments.read().await;
        assignments.get(&expert_id).cloned()
    }

    pub async fn update_worker_status(&self, worker_id: &str, status: WorkerStatus) -> AuriaResult<()> {
        let mut workers = self.workers.write().await;
        
        if let Some(worker) = workers.iter_mut().find(|w| w.id == worker_id) {
            worker.status = status;
            Ok(())
        } else {
            Err(AuriaError::ClusterError(
                format!("Worker {} not found", worker_id),
            ))
        }
    }

    pub async fn update_worker_load(&self, worker_id: &str, load: f32) -> AuriaResult<()> {
        let mut workers = self.workers.write().await;
        
        if let Some(worker) = workers.iter_mut().find(|w| w.id == worker_id) {
            worker.load = load;
            if load > 0.8 {
                worker.status = WorkerStatus::Busy;
            } else if worker.status == WorkerStatus::Busy {
                worker.status = WorkerStatus::Idle;
            }
            Ok(())
        } else {
            Err(AuriaError::ClusterError(
                format!("Worker {} not found", worker_id),
            ))
        }
    }

    pub async fn get_worker_count(&self) -> usize {
        self.workers.read().await.len()
    }

    pub async fn health_check(&self) -> Vec<(String, bool)> {
        let workers = self.workers.read().await;
        workers
            .iter()
            .map(|w| (w.id.clone(), w.status != WorkerStatus::Offline))
            .collect()
    }
}

pub struct ClusterSession {
    coordinator: ClusterCoordinator,
    tasks: Arc<RwLock<Vec<ClusterTask>>>,
}

impl ClusterSession {
    pub fn new(coordinator: ClusterCoordinator) -> Self {
        Self {
            coordinator,
            tasks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn submit_task(&self, request_id: RequestId, expert_ids: Vec<ExpertId>) -> AuriaResult<ClusterTask> {
        let distribution = self.coordinator.distribute_experts(&expert_ids).await?;
        
        if distribution.is_empty() {
            return Err(AuriaError::ClusterError("No workers available".to_string()));
        }

        let (worker, _) = distribution[0].clone();

        let task = ClusterTask {
            task_id: request_id,
            expert_ids,
            assigned_worker: worker.id,
            status: TaskStatus::Pending,
        };

        self.tasks.write().await.push(task.clone());
        
        Ok(task)
    }

    pub async fn get_task_status(&self, task_id: RequestId) -> Option<TaskStatus> {
        let tasks = self.tasks.read().await;
        tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .map(|t| t.status.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_coordinator() {
        let coordinator = ClusterCoordinator::new("coordinator-1".to_string());
        
        coordinator.add_worker(WorkerNode {
            id: "worker-1".to_string(),
            address: "192.168.1.1:8080".to_string(),
            capabilities: Tier::Max,
            status: WorkerStatus::Idle,
            load: 0.0,
        }).await.unwrap();

        let workers = coordinator.get_available_workers().await;
        assert_eq!(workers.len(), 1);
    }
}
