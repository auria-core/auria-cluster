// File: lib.rs - This file is part of AURIA
// Copyright (c) 2026 AURIA Developers and Contributors
// Description:
//     Cluster coordination for distributed execution in AURIA Runtime Core.
//     Manages worker nodes and coordinates distributed expert execution
//     across a cluster for the Max tier.
//
pub mod raft;

use auria_core::{AuriaError, AuriaResult, ExpertId, RequestId, Tier};
use crate::raft::{RaftNode, RaftConfig, ClusterInfo, NodeRole};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_ms: u64,
    pub max_workers: usize,
    pub task_timeout_seconds: u64,
    pub failure_detection_threshold: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_id: String::new(),
            heartbeat_interval_ms: 1000,
            election_timeout_ms: 5000,
            max_workers: 100,
            task_timeout_seconds: 300,
            failure_detection_threshold: 3,
        }
    }
}

#[derive(Clone)]
pub struct ClusterCoordinator {
    config: ClusterConfig,
    node_id: String,
    is_leader: Arc<RwLock<bool>>,
    leader_id: Arc<RwLock<Option<String>>>,
    workers: Arc<RwLock<HashMap<String, WorkerNode>>>,
    tasks: Arc<RwLock<VecDeque<ClusterTask>>>,
    task_results: Arc<RwLock<HashMap<RequestId, TaskResult>>>,
    expert_assignments: Arc<RwLock<HashMap<ExpertId, String>>>,
    pending_tasks: Arc<RwLock<Vec<PendingTask>>>,
    failed_tasks: Arc<RwLock<Vec<RequestId>>>,
    gossip_state: Arc<RwLock<GossipState>>,
    last_heartbeat: Arc<RwLock<HashMap<String, u64>>>,
    raft_node: Option<Arc<RaftNode>>,
    cluster_nodes: Arc<RwLock<HashSet<String>>>,
}

#[derive(Clone, Debug)]
pub struct WorkerNode {
    pub id: String,
    pub address: String,
    pub capabilities: Tier,
    pub status: WorkerStatus,
    pub load: f32,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
    pub cpu_cores: u32,
    pub gpu_available: bool,
    pub started_at: u64,
    pub last_seen: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerStatus {
    Idle,
    Busy,
    Starting,
    Stopping,
    Offline,
    Failed(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterTask {
    pub task_id: RequestId,
    pub expert_ids: Vec<ExpertId>,
    pub assigned_worker: Option<String>,
    pub status: TaskStatus,
    pub created_at: u64,
    pub started_at: Option<u64>,
    pub completed_at: Option<u64>,
    pub priority: TaskPriority,
    pub input_size: u64,
    pub retries: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Queued,
    Assigned,
    Running,
    Completed,
    Failed(String),
    Cancelled,
    TimedOut,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: RequestId,
    pub worker_id: String,
    pub output: Vec<String>,
    pub execution_time_ms: u64,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingTask {
    pub task: ClusterTask,
    pub priority_score: i64,
    pub queued_at: u64,
}

#[derive(Clone, Debug)]
pub struct GossipState {
    pub members: HashMap<String, WorkerNode>,
    pub member_versions: HashMap<String, u64>,
    pub local_version: u64,
}

impl GossipState {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            member_versions: HashMap::new(),
            local_version: 0,
        }
    }

    pub fn update_member(&mut self, node: WorkerNode) {
        let version = self.member_versions.entry(node.id.clone()).or_insert(0);
        *version += 1;
        self.local_version += 1;
        self.members.insert(node.id.clone(), node);
    }

    pub fn remove_member(&mut self, node_id: &str) {
        self.members.remove(node_id);
        self.member_versions.remove(node_id);
        self.local_version += 1;
    }
}

impl Default for GossipState {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterCoordinator {
    pub fn new(cluster_id: String) -> Self {
        Self {
            config: ClusterConfig {
                cluster_id: cluster_id.clone(),
                ..Default::default()
            },
            node_id: format!("{}-{}", cluster_id, uuid::Uuid::new_v4()),
            is_leader: Arc::new(RwLock::new(false)),
            leader_id: Arc::new(RwLock::new(None)),
            workers: Arc::new(RwLock::new(HashMap::new())),
            tasks: Arc::new(RwLock::new(VecDeque::new())),
            task_results: Arc::new(RwLock::new(HashMap::new())),
            expert_assignments: Arc::new(RwLock::new(HashMap::new())),
            pending_tasks: Arc::new(RwLock::new(Vec::new())),
            failed_tasks: Arc::new(RwLock::new(Vec::new())),
            gossip_state: Arc::new(RwLock::new(GossipState::new())),
            last_heartbeat: Arc::new(RwLock::new(HashMap::new())),
            raft_node: None,
            cluster_nodes: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub fn with_config(config: ClusterConfig) -> Self {
        let node_id = format!("{}-{}", config.cluster_id, uuid::Uuid::new_v4());
        Self {
            config,
            node_id,
            is_leader: Arc::new(RwLock::new(false)),
            leader_id: Arc::new(RwLock::new(None)),
            workers: Arc::new(RwLock::new(HashMap::new())),
            tasks: Arc::new(RwLock::new(VecDeque::new())),
            task_results: Arc::new(RwLock::new(HashMap::new())),
            expert_assignments: Arc::new(RwLock::new(HashMap::new())),
            pending_tasks: Arc::new(RwLock::new(Vec::new())),
            failed_tasks: Arc::new(RwLock::new(Vec::new())),
            gossip_state: Arc::new(RwLock::new(GossipState::new())),
            last_heartbeat: Arc::new(RwLock::new(HashMap::new())),
            raft_node: None,
            cluster_nodes: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub async fn init_raft(&mut self, peers: Vec<String>) -> AuriaResult<()> {
        let raft_config = RaftConfig {
            election_timeout_min_ms: self.config.election_timeout_ms / 2,
            election_timeout_max_ms: self.config.election_timeout_ms,
            heartbeat_interval_ms: self.config.heartbeat_interval_ms,
            min_election_quorum: (peers.len() + 1) / 2 + 1,
            max_log_entries_per_snapshot: 1000,
        };
        
        let node_id = self.node_id.clone();
        let raft = Arc::new(RaftNode::new(node_id.clone(), raft_config).with_peers(peers.clone()));
        
        raft.start().await?;
        
        let peers_count = peers.len();
        {
            let mut cluster_nodes = self.cluster_nodes.write().await;
            for peer in peers {
                cluster_nodes.insert(peer);
            }
        }
        
        self.raft_node = Some(raft);
        tracing::info!("Initialized Raft for node {} with {} peers", node_id, peers_count);
        Ok(())
    }

    pub async fn start_raft(&self) -> AuriaResult<()> {
        if let Some(ref raft) = self.raft_node {
            raft.start().await?;
        }
        Ok(())
    }

    pub async fn stop_raft(&self) {
        if let Some(ref raft) = self.raft_node {
            raft.stop().await;
        }
    }

    pub async fn sync_with_leader(&self) -> bool {
        if let Some(ref raft) = self.raft_node {
            if raft.is_leader().await {
                return true;
            }
            
            if let Some(leader_id) = raft.get_leader_id().await {
                tracing::debug!("Following leader: {}", leader_id);
                *self.leader_id.write().await = Some(leader_id);
                return false;
            }
        }
        false
    }

    pub async fn get_raft_info(&self) -> Option<ClusterInfo> {
        if let Some(ref raft) = self.raft_node {
            Some(raft.get_cluster_info().await)
        } else {
            None
        }
    }

    pub async fn propose_task(&self, task_data: Vec<u8>) -> AuriaResult<u64> {
        if let Some(ref raft) = self.raft_node {
            if !raft.is_leader().await {
                return Err(AuriaError::ClusterError("Not the leader, cannot propose".to_string()));
            }
            raft.propose(task_data).await
        } else {
            Err(AuriaError::ClusterError("Raft not initialized".to_string()))
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn become_leader(&self) -> bool {
        let mut is_leader = self.is_leader.write().await;
        *is_leader = true;
        *self.leader_id.write().await = Some(self.node_id.clone());
        true
    }

    pub async fn is_leader(&self) -> bool {
        *self.is_leader.read().await
    }

    pub async fn get_leader(&self) -> Option<String> {
        self.leader_id.read().await.clone()
    }

    pub async fn propose_leader(&self, candidate_id: String) -> AuriaResult<bool> {
        let current_leader = self.leader_id.read().await;
        
        if let Some(ref leader) = *current_leader {
            if leader > &candidate_id {
                return Ok(false);
            }
        }
        
        drop(current_leader);
        *self.leader_id.write().await = Some(candidate_id.clone());
        
        Ok(true)
    }

    pub async fn add_worker(&self, worker: WorkerNode) -> AuriaResult<()> {
        let mut workers = self.workers.write().await;
        
        if workers.len() >= self.config.max_workers {
            return Err(AuriaError::ClusterError("Max workers reached".to_string()));
        }
        
        if workers.contains_key(&worker.id) {
            return Err(AuriaError::ClusterError(
                format!("Worker {} already exists", worker.id),
            ));
        }
        
        let worker_clone = worker.clone();
        workers.insert(worker.id.clone(), worker);
        
        let mut gossip = self.gossip_state.write().await;
        gossip.update_member(worker_clone);
        
        Ok(())
    }

    pub async fn remove_worker(&self, worker_id: &str) -> AuriaResult<()> {
        let mut workers = self.workers.write().await;
        
        if workers.remove(worker_id).is_none() {
            return Err(AuriaError::ClusterError(
                format!("Worker {} not found", worker_id),
            ));
        }
        
        let mut gossip = self.gossip_state.write().await;
        gossip.remove_member(worker_id);
        
        Ok(())
    }

    pub async fn get_worker(&self, worker_id: &str) -> Option<WorkerNode> {
        self.workers.read().await.get(worker_id).cloned()
    }

    pub async fn get_available_workers(&self, min_tier: Tier) -> Vec<WorkerNode> {
        let workers = self.workers.read().await;
        
        workers
            .values()
            .filter(|w| {
                w.status == WorkerStatus::Idle 
                && Self::tier_meets_minimum(&w.capabilities, &min_tier)
                && w.load < 0.8
            })
            .cloned()
            .collect()
    }

    fn tier_meets_minimum(worker_tier: &Tier, min_tier: &Tier) -> bool {
        match (worker_tier, min_tier) {
            (Tier::Max, _) => true,
            (Tier::Pro, Tier::Max) => false,
            (Tier::Pro, _) => true,
            (Tier::Standard, Tier::Max) => false,
            (Tier::Standard, Tier::Pro) => false,
            (Tier::Standard, _) => true,
            (Tier::Nano, Tier::Max) => false,
            (Tier::Nano, Tier::Pro) => false,
            (Tier::Nano, Tier::Standard) => false,
            (Tier::Nano, Tier::Nano) => true,
        }
    }

    pub async fn distribute_experts(&self, expert_ids: &[ExpertId]) -> AuriaResult<Vec<(WorkerNode, Vec<ExpertId>)>> {
        let workers = self.workers.read().await;
        
        if workers.is_empty() {
            return Err(AuriaError::ClusterError("No workers available".to_string()));
        }

        let available: Vec<_> = workers
            .values()
            .filter(|w| w.capabilities == Tier::Max && w.status == WorkerStatus::Idle)
            .collect();

        if available.is_empty() {
            return Err(AuriaError::ClusterError(
                "No available workers with Max tier capability".to_string(),
            ));
        }

        let mut distribution = Vec::new();
        for (i, expert_id) in expert_ids.iter().enumerate() {
            let worker = available[i % available.len()].clone();
            distribution.push((worker, vec![expert_id.clone()]));
        }

        Ok(distribution)
    }

    pub async fn submit_task(
        &self,
        request_id: RequestId,
        expert_ids: Vec<ExpertId>,
        priority: TaskPriority,
        input_size: u64,
    ) -> AuriaResult<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let task = ClusterTask {
            task_id: request_id,
            expert_ids: expert_ids.clone(),
            assigned_worker: None,
            status: TaskStatus::Pending,
            created_at: now,
            started_at: None,
            completed_at: None,
            priority: priority.clone(),
            input_size,
            retries: 0,
        };

        let pending = PendingTask {
            task,
            priority_score: Self::compute_priority_score(priority, now),
            queued_at: now,
        };

        let mut pending_tasks = self.pending_tasks.write().await;
        pending_tasks.push(pending);
        pending_tasks.sort_by(|a, b| b.priority_score.cmp(&a.priority_score));

        Ok(())
    }

    fn compute_priority_score(priority: TaskPriority, timestamp: u64) -> i64 {
        let priority_val = match priority {
            TaskPriority::Low => 0,
            TaskPriority::Normal => 1000,
            TaskPriority::High => 2000,
            TaskPriority::Critical => 3000,
        };
        
        priority_val as i64 - (timestamp as i64 / 1000)
    }

    pub async fn assign_next_task(&self, worker_id: &str) -> Option<ClusterTask> {
        let mut pending_tasks = self.pending_tasks.write().await;
        
        if let Some(pending) = pending_tasks.pop() {
            let mut task = pending.task;
            task.status = TaskStatus::Assigned;
            task.assigned_worker = Some(worker_id.to_string());
            
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            task.started_at = Some(now);
            
            drop(pending_tasks);
            
            self.tasks.write().await.push_back(task.clone());
            
            return Some(task);
        }
        
        None
    }

    pub async fn complete_task(&self, result: TaskResult) -> AuriaResult<()> {
        let mut tasks = self.tasks.write().await;
        
        if let Some(task) = tasks.iter_mut().find(|t| t.task_id == result.task_id) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            task.status = if result.success {
                TaskStatus::Completed
            } else {
                TaskStatus::Failed(result.error_message.clone().unwrap_or_default())
            };
            task.completed_at = Some(now);
        }
        
        drop(tasks);
        self.task_results.write().await.insert(result.task_id, result);
        
        Ok(())
    }

    pub async fn get_task_status(&self, task_id: RequestId) -> Option<TaskStatus> {
        let tasks = self.tasks.read().await;
        
        tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .map(|t| t.status.clone())
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
        
        if let Some(worker) = workers.get_mut(worker_id) {
            worker.status = status;
            Ok(())
        } else {
            Err(AuriaError::ClusterError(
                format!("Worker {} not found", worker_id),
            ))
        }
    }

    pub async fn update_worker_load(&self, worker_id: &str, load: f32, memory_used: u64) -> AuriaResult<()> {
        let mut workers = self.workers.write().await;
        
        if let Some(worker) = workers.get_mut(worker_id) {
            worker.load = load;
            worker.memory_used_mb = memory_used;
            
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

    pub async fn receive_heartbeat(&self, worker_id: String) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let mut heartbeats = self.last_heartbeat.write().await;
        heartbeats.insert(worker_id.clone(), now);
        
        let mut workers = self.workers.write().await;
        if let Some(worker) = workers.get_mut(&worker_id) {
            worker.last_seen = now;
        }
    }

    pub async fn detect_failed_workers(&self) -> Vec<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let heartbeats = self.last_heartbeat.read().await;
        let mut failed = Vec::new();
        
        for (worker_id, last_seen) in heartbeats.iter() {
            if now - last_seen > self.config.election_timeout_ms / 1000 * self.config.failure_detection_threshold as u64 {
                failed.push(worker_id.clone());
            }
        }
        
        failed
    }

    pub async fn get_cluster_stats(&self) -> ClusterStats {
        let workers = self.workers.read().await;
        
        let mut idle = 0;
        let mut busy = 0;
        let mut offline = 0;
        
        for worker in workers.values() {
            match worker.status {
                WorkerStatus::Idle => idle += 1,
                WorkerStatus::Busy => busy += 1,
                WorkerStatus::Offline => offline += 1,
                _ => {}
            }
        }
        
        let tasks = self.tasks.read().await;
        let pending = self.pending_tasks.read().await;
        
        ClusterStats {
            total_workers: workers.len(),
            idle_workers: idle,
            busy_workers: busy,
            offline_workers: offline,
            pending_tasks: pending.len(),
            running_tasks: tasks.iter().filter(|t| matches!(t.status, TaskStatus::Running)).count(),
            completed_tasks: tasks.iter().filter(|t| matches!(t.status, TaskStatus::Completed)).count(),
            failed_tasks: self.failed_tasks.read().await.len(),
            is_leader: *self.is_leader.read().await,
            leader_id: self.leader_id.read().await.clone(),
        }
    }

    pub async fn get_worker_count(&self) -> usize {
        self.workers.read().await.len()
    }

    pub async fn health_check(&self) -> Vec<(String, bool, WorkerStatus)> {
        let workers = self.workers.read().await;
        
        workers
            .iter()
            .map(|(id, w)| {
                let healthy = w.status != WorkerStatus::Offline 
                    && w.status != WorkerStatus::Failed(String::new());
                (id.clone(), healthy, w.status.clone())
            })
            .collect()
    }

    pub async fn gossip_with_peer(&self, peer_id: &str, peer_state: GossipState) -> GossipState {
        let mut local_state = self.gossip_state.write().await;
        
        for (node_id, node) in peer_state.members {
            if let Some(peer_version) = peer_state.member_versions.get(&node_id) {
                if let Some(local_version) = local_state.member_versions.get(&node_id) {
                    if peer_version > local_version {
                        local_state.update_member(node);
                    }
                } else {
                    local_state.update_member(node);
                }
            }
        }
        
        local_state.clone()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterStats {
    pub total_workers: usize,
    pub idle_workers: usize,
    pub busy_workers: usize,
    pub offline_workers: usize,
    pub pending_tasks: usize,
    pub running_tasks: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub is_leader: bool,
    pub leader_id: Option<String>,
}

pub struct ClusterSession {
    coordinator: ClusterCoordinator,
    session_id: String,
}

impl ClusterSession {
    pub fn new(coordinator: ClusterCoordinator) -> Self {
        Self {
            coordinator,
            session_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    pub async fn submit_task(
        &self,
        request_id: RequestId,
        expert_ids: Vec<ExpertId>,
    ) -> AuriaResult<ClusterTask> {
        self.coordinator
            .submit_task(
                request_id,
                expert_ids.clone(),
                TaskPriority::Normal,
                0,
            )
            .await?;

        let status = self
            .coordinator
            .get_task_status(request_id)
            .await
            .unwrap_or(TaskStatus::Pending);

        Ok(ClusterTask {
            task_id: request_id,
            expert_ids,
            assigned_worker: None,
            status,
            created_at: 0,
            started_at: None,
            completed_at: None,
            priority: TaskPriority::Normal,
            input_size: 0,
            retries: 0,
        })
    }

    pub async fn get_task_result(&self, task_id: RequestId) -> Option<TaskResult> {
        self.coordinator.task_results.read().await.get(&task_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_coordinator() {
        let coordinator = ClusterCoordinator::new("test-cluster".to_string());
        
        coordinator.add_worker(WorkerNode {
            id: "worker-1".to_string(),
            address: "192.168.1.1:8080".to_string(),
            capabilities: Tier::Max,
            status: WorkerStatus::Idle,
            load: 0.0,
            memory_used_mb: 0,
            memory_total_mb: 8192,
            cpu_cores: 8,
            gpu_available: true,
            started_at: 0,
            last_seen: 0,
        }).await.unwrap();

        let workers = coordinator.get_available_workers(Tier::Max).await;
        assert_eq!(workers.len(), 1);
    }

    #[tokio::test]
    async fn test_task_submission() {
        let coordinator = ClusterCoordinator::new("test-cluster".to_string());
        
        let request_id = RequestId(uuid::Uuid::new_v4().into_bytes());
        
        coordinator.submit_task(
            request_id,
            vec![ExpertId([1u8; 32])],
            TaskPriority::High,
            1024,
        ).await.unwrap();

        let status = coordinator.get_task_status(request_id).await;
        assert!(status.is_some());
    }

    #[tokio::test]
    async fn test_leader_election() {
        let coordinator = ClusterCoordinator::new("test-cluster".to_string());
        
        coordinator.become_leader().await;
        
        assert!(coordinator.is_leader().await);
        assert_eq!(coordinator.get_leader().await, Some(coordinator.node_id().to_string()));
    }

    #[tokio::test]
    async fn test_worker_failure_detection() {
        let coordinator = ClusterCoordinator::new("test-cluster".to_string());
        
        coordinator.add_worker(WorkerNode {
            id: "worker-1".to_string(),
            address: "192.168.1.1:8080".to_string(),
            capabilities: Tier::Max,
            status: WorkerStatus::Idle,
            load: 0.0,
            memory_used_mb: 0,
            memory_total_mb: 8192,
            cpu_cores: 8,
            gpu_available: true,
            started_at: 0,
            last_seen: 0,
        }).await.unwrap();
        
        coordinator.receive_heartbeat("worker-1".to_string()).await;
        
        let failed = coordinator.detect_failed_workers().await;
        assert!(failed.is_empty());
    }
}
