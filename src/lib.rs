use auria_core::{AuriaResult, ExpertId, ShardId, Tier};

pub struct ClusterCoordinator {
    node_id: String,
    workers: Vec<WorkerNode>,
}

pub struct WorkerNode {
    pub id: String,
    pub address: String,
    pub capabilities: Tier,
}

impl ClusterCoordinator {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            workers: Vec::new(),
        }
    }

    pub fn add_worker(&mut self, worker: WorkerNode) {
        self.workers.push(worker);
    }

    pub fn distribute_experts(
        &self,
        expert_ids: &[ExpertId],
    ) -> AuriaResult<Vec<(WorkerNode, Vec<ExpertId>)>> {
        if self.workers.is_empty() {
            return Err(auria_core::AuriaError::ClusterError(
                "No workers available".to_string(),
            ));
        }
        let mut distribution = Vec::new();
        for (i, expert_id) in expert_ids.iter().enumerate() {
            let worker = &self.workers[i % self.workers.len()];
            distribution.push((worker.clone(), vec![*expert_id]));
        }
        Ok(distribution)
    }
}
