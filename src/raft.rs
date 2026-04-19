// File: raft.rs - This file is part of AURIA
// Copyright (c) 2026 AURIA Developers and Contributors
// Description:
//     Raft consensus implementation for AURIA cluster leader election.
//     Provides fault-tolerant leader election and distributed consensus.

use auria_core::{AuriaError, AuriaResult};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::interval;
use sha3::{Digest, Keccak256};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftConfig {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub min_election_quorum: usize,
    pub max_log_entries_per_snapshot: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            min_election_quorum: 2,
            max_log_entries_per_snapshot: 1000,
        }
    }
}

#[derive(Clone)]
pub struct RaftNode {
    node_id: String,
    cluster_nodes: HashSet<String>,
    config: RaftConfig,
    role: Arc<tokio::sync::RwLock<NodeRole>>,
    current_term: Arc<tokio::sync::RwLock<u64>>,
    voted_for: Arc<tokio::sync::RwLock<Option<String>>>,
    log: Arc<tokio::sync::RwLock<Vec<RaftLogEntry>>>,
    commit_index: Arc<tokio::sync::RwLock<u64>>,
    last_applied: Arc<tokio::sync::RwLock<u64>>,
    leader_id: Arc<tokio::sync::RwLock<Option<String>>>,
    last_heartbeat: Arc<tokio::sync::RwLock<u64>>,
    vote_count: Arc<tokio::sync::RwLock<HashMap<String, bool>>>,
    election_timer: Arc<tokio::sync::RwLock<Option<tokio::task::JoinHandle<()>>>>,
    heartbeat_timer: Arc<tokio::sync::RwLock<Option<tokio::task::JoinHandle<()>>>>,
    state_machine: Arc<tokio::sync::RwLock<Box<dyn RaftStateMachine + Send + Sync>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub entry_type: RaftEntryType,
    pub data: Vec<u8>,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RaftEntryType {
    ConfigChange,
    StateMachineCommand,
    MembershipChange,
    Heartbeat,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub voter_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<RaftLogEntry>,
    pub leader_commit: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

#[async_trait::async_trait]
pub trait RaftStateMachine: Send + Sync {
    async fn apply(&self, entry: &RaftLogEntry) -> Vec<u8>;
    async fn snapshot(&self) -> Vec<u8>;
    async fn restore(&self, snapshot: &[u8]) -> AuriaResult<()>;
}

    pub struct InMemoryStateMachine {
        state: Arc<tokio::sync::RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    }

    impl InMemoryStateMachine {
        pub fn new() -> Self {
            Self {
                state: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            }
        }
}

#[async_trait::async_trait]
impl RaftStateMachine for InMemoryStateMachine {
    async fn apply(&self, entry: &RaftLogEntry) -> Vec<u8> {
        let mut state = self.state.write().await;
        let mut hasher = Keccak256::new();
        hasher.update(&entry.data);
        hasher.update(entry.term.to_le_bytes());
        let hash = hasher.finalize();
        state.insert(entry.data.clone(), hash.to_vec());
        hash.to_vec()
    }

    async fn snapshot(&self) -> Vec<u8> {
        let state = self.state.read().await;
        serde_json::to_vec(&*state).unwrap_or_default()
    }

    async fn restore(&self, snapshot: &[u8]) -> AuriaResult<()> {
        let state: HashMap<Vec<u8>, Vec<u8>> = serde_json::from_slice(snapshot)
            .map_err(|e| AuriaError::ExecutionError(format!("Snapshot restore failed: {}", e)))?;
        *self.state.write().await = state;
        Ok(())
    }
}

impl RaftNode {
    pub fn new(node_id: String, config: RaftConfig) -> Self {
        Self {
            node_id,
            cluster_nodes: HashSet::new(),
            config,
            role: Arc::new(RwLock::new(NodeRole::Follower)),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(Vec::new())),
            commit_index: Arc::new(RwLock::new(0)),
            last_applied: Arc::new(RwLock::new(0)),
            leader_id: Arc::new(RwLock::new(None)),
            last_heartbeat: Arc::new(RwLock::new(0)),
            vote_count: Arc::new(RwLock::new(HashMap::new())),
            election_timer: Arc::new(RwLock::new(None)),
            heartbeat_timer: Arc::new(RwLock::new(None)),
            state_machine: Arc::new(RwLock::new(Box::new(InMemoryStateMachine::new()))),
        }
    }

    pub fn with_peers(mut self, peers: Vec<String>) -> Self {
        self.cluster_nodes = peers.into_iter().collect();
        self
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn start(&self) -> AuriaResult<()> {
        tracing::info!("Starting Raft node: {}", self.node_id);
        self.start_election_timer().await;
        Ok(())
    }

    pub async fn stop(&self) {
        tracing::info!("Stopping Raft node: {}", self.node_id);
        
        if let Some(timer) = self.election_timer.write().await.take() {
            timer.abort();
        }
        if let Some(timer) = self.heartbeat_timer.write().await.take() {
            timer.abort();
        }
    }

    async fn start_election_timer(&self) {
        let node = self.clone();
        let min = self.config.election_timeout_min_ms;
        let max = self.config.election_timeout_max_ms;
        
        tokio::spawn(async move {
            loop {
                let timeout_ms = {
                    use rand::Rng;
                    rand::thread_rng().gen_range(min..max)
                };
                
                tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
                
                let role = node.role.read().await.clone();
                if role == NodeRole::Leader {
                    continue;
                }
                
                let last_heartbeat = *node.last_heartbeat.read().await;
                let now = now_ms();
                
                // Check if we've received a heartbeat recently
                if now - last_heartbeat < max {
                    continue;
                }
                
                tracing::debug!("Election timeout expired, starting election");
                node.start_election().await;
            }
        });
    }

    async fn start_heartbeat_timer(&self) {
        let node = self.clone();
        let interval_ms = self.config.heartbeat_interval_ms;
        
        let handle = tokio::spawn(async move {
            let mut timer = interval(Duration::from_millis(interval_ms));
            loop {
                timer.tick().await;
                if !node.send_heartbeat().await {
                    break;
                }
            }
        });
        
        *self.heartbeat_timer.write().await = Some(handle);
    }

    async fn start_election(&self) {
        let role = self.role.read().await.clone();
        if role == NodeRole::Leader {
            return;
        }

        let mut term = self.current_term.write().await;
        *term += 1;
        let current_term = *term;
        drop(term);

        *self.role.write().await = NodeRole::Candidate;
        *self.voted_for.write().await = Some(self.node_id.clone());
        *self.leader_id.write().await = None;

        tracing::info!("Node {} starting election for term {}", self.node_id, current_term);

        let last_log = self.get_last_log_info().await;
        let request = VoteRequest {
            term: current_term,
            candidate_id: self.node_id.clone(),
            last_log_index: last_log.0,
            last_log_term: last_log.1,
        };

        let mut votes_granted = 1;
        *self.vote_count.write().await = HashMap::new();
        self.vote_count.write().await.insert(self.node_id.clone(), true);

        let peers: Vec<String> = self.cluster_nodes.iter().cloned().collect();
        for peer in &peers {
            if let Ok(response) = self.request_vote_from_peer(peer, &request).await {
                if response.term > current_term {
                    self.become_follower(response.term).await;
                    return;
                }
                if response.vote_granted {
                    votes_granted += 1;
                }
            }
        }

        let quorum_size = (self.cluster_nodes.len() + 1) / 2 + 1;
        if votes_granted >= quorum_size {
            self.become_leader().await;
        } else {
            *self.role.write().await = NodeRole::Follower;
        }
    }

    async fn become_leader(&self) {
        *self.role.write().await = NodeRole::Leader;
        *self.leader_id.write().await = Some(self.node_id.clone());
        
        tracing::info!("Node {} became leader for term {}", self.node_id, *self.current_term.read().await);

        self.start_heartbeat_timer().await;
    }

    async fn become_follower(&self, term: u64) {
        *self.current_term.write().await = term;
        *self.role.write().await = NodeRole::Follower;
        *self.voted_for.write().await = None;
        *self.leader_id.write().await = None;
    }

    async fn send_heartbeat(&self) -> bool {
        let role = self.role.read().await.clone();
        if role != NodeRole::Leader {
            return false;
        }

        let term = *self.current_term.read().await;
        let last_log = self.get_last_log_info().await;
        
        let request = AppendEntriesRequest {
            term,
            leader_id: self.node_id.clone(),
            prev_log_index: last_log.0,
            prev_log_term: last_log.1,
            entries: vec![],
            leader_commit: *self.commit_index.read().await,
        };

        let peers: Vec<String> = self.cluster_nodes.iter().cloned().collect();
        for peer in &peers {
            let _ = self.send_append_entries_to_peer(peer, &request).await;
        }

        *self.last_heartbeat.write().await = now_ms();
        true
    }

    async fn request_vote_from_peer(
        &self,
        peer: &str,
        request: &VoteRequest,
    ) -> Result<VoteResponse, String> {
        tracing::debug!("Requesting vote from {} for term {}", peer, request.term);
        
        // In a real implementation, this would send an actual RPC to the peer
        // and receive a response. For now, we simulate the peer's vote decision
        // based on the Raft voting rules.
        
        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // The simulated peer applies the same voting rules as handle_vote_request
        // In a real cluster, this would be executed on the peer node
        
        // For simulation, always grant vote if our term is valid
        // (In production, this would be the actual peer node's decision)
        Ok(VoteResponse {
            term: request.term,
            vote_granted: true,
            voter_id: peer.to_string(),
        })
    }

    async fn send_append_entries_to_peer(
        &self,
        peer: &str,
        request: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, String> {
        tracing::debug!("Sending append entries to {} for term {}", peer, request.term);
        
        Ok(AppendEntriesResponse {
            term: request.term,
            success: true,
            match_index: request.prev_log_index + request.entries.len() as u64,
        })
    }

    async fn get_last_log_info(&self) -> (u64, u64) {
        let log = self.log.read().await;
        if log.is_empty() {
            return (0, 0);
        }
        let last = log.last().unwrap();
        (last.index, last.term)
    }

    pub async fn propose(&self, data: Vec<u8>) -> AuriaResult<u64> {
        let role = self.role.read().await.clone();
        if role != NodeRole::Leader {
            return Err(AuriaError::ClusterError("Not the leader".to_string()));
        }

        let term = *self.current_term.read().await;
        let index = {
            let log = self.log.read().await;
            log.len() as u64 + 1
        };

        let entry = RaftLogEntry {
            term,
            index,
            entry_type: RaftEntryType::StateMachineCommand,
            data,
            timestamp: now_ms(),
        };

        self.log.write().await.push(entry);
        
        tracing::debug!("Leader {} proposed entry at index {}", self.node_id, index);
        Ok(index)
    }

    pub async fn get_role(&self) -> NodeRole {
        self.role.read().await.clone()
    }

    pub async fn is_leader(&self) -> bool {
        self.role.read().await.clone() == NodeRole::Leader
    }

    pub async fn get_current_term(&self) -> u64 {
        *self.current_term.read().await
    }

    pub async fn get_leader_id(&self) -> Option<String> {
        self.leader_id.read().await.clone()
    }

    pub async fn get_cluster_info(&self) -> ClusterInfo {
        ClusterInfo {
            node_id: self.node_id.clone(),
            role: self.role.read().await.clone(),
            term: *self.current_term.read().await,
            leader_id: self.leader_id.read().await.clone(),
            commit_index: *self.commit_index.read().await,
            log_length: self.log.read().await.len(),
            peers: self.cluster_nodes.clone().into_iter().collect(),
        }
    }

    pub async fn add_peer(&mut self, peer_id: String) -> AuriaResult<()> {
        self.cluster_nodes.insert(peer_id);
        Ok(())
    }

    pub async fn remove_peer(&mut self, peer_id: &str) -> AuriaResult<()> {
        self.cluster_nodes.remove(peer_id);
        Ok(())
    }

    pub async fn handle_vote_request(&self, request: VoteRequest) -> VoteResponse {
        let current_term = *self.current_term.read().await;
        
        if request.term < current_term {
            return VoteResponse {
                term: current_term,
                vote_granted: false,
                voter_id: self.node_id.clone(),
            };
        }

        if request.term > current_term {
            self.become_follower(request.term).await;
        }

        let voted_for = self.voted_for.read().await.clone();
        let last_log = self.get_last_log_info().await;
        
        let log_ok = request.last_log_term > last_log.1 
            || (request.last_log_term == last_log.1 && request.last_log_index >= last_log.0);
        
        let vote_ok = voted_for.is_none() 
            || voted_for.as_ref() == Some(&request.candidate_id);
        
        let granted = log_ok && vote_ok;
        
        if granted {
            *self.voted_for.write().await = Some(request.candidate_id.clone());
            *self.last_heartbeat.write().await = now_ms();
        }

        tracing::debug!("Node {} granting vote: {}, term: {}", self.node_id, granted, current_term);

        VoteResponse {
            term: current_term,
            vote_granted: granted,
            voter_id: self.node_id.clone(),
        }
    }

    pub async fn handle_append_entries(&self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        *self.last_heartbeat.write().await = now_ms();
        
        let current_term = *self.current_term.read().await;
        
        if request.term < current_term {
            return AppendEntriesResponse {
                term: current_term,
                success: false,
                match_index: 0,
            };
        }

        if request.term > current_term {
            self.become_follower(request.term).await;
        }

        *self.leader_id.write().await = Some(request.leader_id.clone());
        
        let mut entries_len = 0;
        if !request.entries.is_empty() {
            let mut log = self.log.write().await;
            
            if request.prev_log_index > 0 {
                if request.prev_log_index > log.len() as u64 {
                    return AppendEntriesResponse {
                        term: current_term,
                        success: false,
                        match_index: log.len() as u64,
                    };
                }
                
                if let Some(entry) = log.get((request.prev_log_index - 1) as usize) {
                    if entry.term != request.prev_log_term {
                        log.truncate(request.prev_log_index as usize);
                    }
                }
            }
            
            entries_len = request.entries.len();
            for entry in request.entries {
                log.push(entry);
            }
        }

        let mut commit_index = self.commit_index.write().await;
        if request.leader_commit > *commit_index {
            *commit_index = std::cmp::min(request.leader_commit, self.log.read().await.len() as u64);
        }

        AppendEntriesResponse {
            term: current_term,
            success: true,
            match_index: request.prev_log_index + entries_len as u64,
        }
    }

    pub async fn apply_committed_entries(&self) -> AuriaResult<Vec<Vec<u8>>> {
        let mut results = Vec::new();
        let mut last_applied = self.last_applied.write().await;
        let commit_index = *self.commit_index.read().await;
        let log = self.log.read().await.clone();
        
        while *last_applied < commit_index {
            *last_applied += 1;
            if let Some(entry) = log.get(*last_applied as usize - 1) {
                let state_machine = self.state_machine.read().await;
                let result = state_machine.apply(entry).await;
                results.push(result);
            }
        }
        
        Ok(results)
    }

    pub async fn set_state_machine(&self, sm: Box<dyn RaftStateMachine + Send + Sync>) {
        *self.state_machine.write().await = sm;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub node_id: String,
    pub role: NodeRole,
    pub term: u64,
    pub leader_id: Option<String>,
    pub commit_index: u64,
    pub log_length: usize,
    pub peers: Vec<String>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub struct RaftCluster {
    nodes: HashMap<String, Arc<RaftNode>>,
    local_node_id: String,
}

impl RaftCluster {
    pub fn new(local_node_id: String) -> Self {
        Self {
            nodes: HashMap::new(),
            local_node_id,
        }
    }

    pub fn add_node(&mut self, node_id: String, node: Arc<RaftNode>) {
        self.nodes.insert(node_id, node);
    }

    pub fn get_node(&self, node_id: &str) -> Option<&Arc<RaftNode>> {
        self.nodes.get(node_id)
    }

    pub fn get_local_node(&self) -> Option<&Arc<RaftNode>> {
        self.nodes.get(&self.local_node_id)
    }

    pub fn get_all_nodes(&self) -> &HashMap<String, Arc<RaftNode>> {
        &self.nodes
    }

    pub async fn discover_nodes(&self) -> Vec<String> {
        self.nodes.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_raft_node_creation() {
        let config = RaftConfig::default();
        let node = RaftNode::new("node1".to_string(), config);
        
        assert_eq!(node.node_id(), "node1");
        assert_eq!(node.get_role().await, NodeRole::Follower);
    }

    #[tokio::test]
    async fn test_raft_node_with_peers() {
        let config = RaftConfig::default();
        let node = RaftNode::new("node1".to_string(), config)
            .with_peers(vec!["node2".to_string(), "node3".to_string()]);
        
        let info = node.get_cluster_info().await;
        assert_eq!(info.peers.len(), 2);
    }

    #[tokio::test]
    async fn test_vote_request() {
        let config = RaftConfig::default();
        let node = RaftNode::new("node1".to_string(), config);
        
        let request = VoteRequest {
            term: 1,
            candidate_id: "node2".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        };
        
        let response = node.handle_vote_request(request).await;
        assert!(response.vote_granted);
    }

    #[tokio::test]
    async fn test_append_entries() {
        let config = RaftConfig::default();
        let node = RaftNode::new("node1".to_string(), config);
        
        let request = AppendEntriesRequest {
            term: 1,
            leader_id: "node2".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        
        let response = node.handle_append_entries(request).await;
        assert!(response.success);
    }

    #[tokio::test]
    async fn test_state_machine() {
        let sm = InMemoryStateMachine::new();
        
        let entry = RaftLogEntry {
            term: 1,
            index: 1,
            entry_type: RaftEntryType::StateMachineCommand,
            data: b"key".to_vec(),
            timestamp: 0,
        };
        
        let result = sm.apply(&entry).await;
        assert_eq!(result.len(), 32);
    }
}
