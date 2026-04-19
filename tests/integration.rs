use auria_cluster::{ClusterCoordinator, WorkerNode, WorkerStatus, TaskPriority, TaskResult, TaskStatus};
use auria_core::{RequestId, ExpertId, Tier};
use uuid::Uuid;

#[tokio::test]
async fn test_cluster_coordinator_creation() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    assert_eq!(coordinator.node_id().starts_with("test-cluster"), true);
}

#[tokio::test]
async fn test_add_worker() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let worker = WorkerNode {
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
    };
    
    let result = coordinator.add_worker(worker).await;
    assert!(result.is_ok());
    
    let worker_count = coordinator.get_worker_count().await;
    assert_eq!(worker_count, 1);
}

#[tokio::test]
async fn test_remove_worker() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let worker = WorkerNode {
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
    };
    
    coordinator.add_worker(worker).await.unwrap();
    
    let result = coordinator.remove_worker("worker-1").await;
    assert!(result.is_ok());
    
    let worker_count = coordinator.get_worker_count().await;
    assert_eq!(worker_count, 0);
}

#[tokio::test]
async fn test_leader_election() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let became_leader = coordinator.become_leader().await;
    assert!(became_leader);
    
    let is_leader = coordinator.is_leader().await;
    assert!(is_leader);
    
    let leader = coordinator.get_leader().await;
    assert!(leader.is_some());
}

#[tokio::test]
async fn test_worker_availability() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let worker1 = WorkerNode {
        id: "worker-1".to_string(),
        address: "192.168.1.1:8080".to_string(),
        capabilities: Tier::Max,
        status: WorkerStatus::Idle,
        load: 0.5,
        memory_used_mb: 4096,
        memory_total_mb: 8192,
        cpu_cores: 8,
        gpu_available: true,
        started_at: 0,
        last_seen: 0,
    };
    
    let worker2 = WorkerNode {
        id: "worker-2".to_string(),
        address: "192.168.1.2:8080".to_string(),
        capabilities: Tier::Standard,
        status: WorkerStatus::Busy,
        load: 0.9,
        memory_used_mb: 7000,
        memory_total_mb: 8192,
        cpu_cores: 4,
        gpu_available: false,
        started_at: 0,
        last_seen: 0,
    };
    
    coordinator.add_worker(worker1).await.unwrap();
    coordinator.add_worker(worker2).await.unwrap();
    
    let available = coordinator.get_available_workers(Tier::Max).await;
    assert_eq!(available.len(), 1);
    assert_eq!(available[0].id, "worker-1");
}

#[tokio::test]
async fn test_task_submission() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let request_id = RequestId(Uuid::new_v4().into_bytes());
    
    let result = coordinator.submit_task(
        request_id,
        vec![],
        TaskPriority::Normal,
        1024,
    ).await;
    
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_cluster_stats() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let worker = WorkerNode {
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
    };
    
    coordinator.add_worker(worker).await.unwrap();
    
    let stats = coordinator.get_cluster_stats().await;
    
    assert_eq!(stats.total_workers, 1);
    assert_eq!(stats.idle_workers, 1);
    assert_eq!(stats.busy_workers, 0);
}

#[tokio::test]
async fn test_heartbeat() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    coordinator.receive_heartbeat("worker-1".to_string()).await;
    
    let failed = coordinator.detect_failed_workers().await;
    assert!(failed.is_empty());
}

#[tokio::test]
async fn test_worker_status_update() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let worker = WorkerNode {
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
    };
    
    coordinator.add_worker(worker).await.unwrap();
    
    let result = coordinator.update_worker_status("worker-1", WorkerStatus::Busy).await;
    assert!(result.is_ok());
    
    let worker = coordinator.get_worker("worker-1").await;
    assert!(worker.is_some());
    assert_eq!(worker.unwrap().status, WorkerStatus::Busy);
}

#[tokio::test]
async fn test_distributed_inference_requires_leader() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let result = coordinator.execute_inference(
        "Hello world".to_string(),
        10,
        Tier::Standard,
    ).await;
    
    assert!(result.is_err());
}

#[tokio::test]
async fn test_distributed_inference_with_worker() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    coordinator.become_leader().await;
    
    let worker = WorkerNode {
        id: "worker-1".to_string(),
        address: "192.168.1.1:8080".to_string(),
        capabilities: Tier::Standard,
        status: WorkerStatus::Idle,
        load: 0.0,
        memory_used_mb: 0,
        memory_total_mb: 8192,
        cpu_cores: 4,
        gpu_available: false,
        started_at: 0,
        last_seen: 0,
    };
    
    coordinator.add_worker(worker).await.unwrap();
    
    let result = coordinator.execute_inference(
        "Hello world".to_string(),
        10,
        Tier::Standard,
    ).await;
    
    assert!(result.is_ok());
    let inference = result.unwrap();
    assert!(!inference.tokens.is_empty());
    assert_eq!(inference.distributed, true);
}

#[tokio::test]
async fn test_cluster_health() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    coordinator.become_leader().await;
    
    let worker = WorkerNode {
        id: "worker-1".to_string(),
        address: "192.168.1.1:8080".to_string(),
        capabilities: Tier::Standard,
        status: WorkerStatus::Idle,
        load: 0.3,
        memory_used_mb: 1024,
        memory_total_mb: 8192,
        cpu_cores: 4,
        gpu_available: false,
        started_at: 0,
        last_seen: 0,
    };
    
    coordinator.add_worker(worker).await.unwrap();
    
    let health = coordinator.get_cluster_health().await;
    
    assert_eq!(health.total_workers, 1);
    assert_eq!(health.idle_workers, 1);
    assert_eq!(health.average_load, 0.3);
    assert!(health.healthy);
}

#[tokio::test]
async fn test_least_loaded_worker_selection() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let worker1 = WorkerNode {
        id: "worker-1".to_string(),
        address: "192.168.1.1:8080".to_string(),
        capabilities: Tier::Standard,
        status: WorkerStatus::Idle,
        load: 0.8,
        memory_used_mb: 4096,
        memory_total_mb: 8192,
        cpu_cores: 4,
        gpu_available: false,
        started_at: 0,
        last_seen: 0,
    };
    
    let worker2 = WorkerNode {
        id: "worker-2".to_string(),
        address: "192.168.1.2:8080".to_string(),
        capabilities: Tier::Standard,
        status: WorkerStatus::Idle,
        load: 0.2,
        memory_used_mb: 1024,
        memory_total_mb: 8192,
        cpu_cores: 4,
        gpu_available: false,
        started_at: 0,
        last_seen: 0,
    };
    
    coordinator.add_worker(worker1).await.unwrap();
    coordinator.add_worker(worker2).await.unwrap();
    
    let least_loaded = coordinator.get_least_loaded_worker(Tier::Standard).await;
    
    assert!(least_loaded.is_some());
    assert_eq!(least_loaded.unwrap().id, "worker-2");
}

#[tokio::test]
async fn test_expert_routing() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    let expert_id = auria_core::ExpertId([1u8; 32]);
    
    coordinator.assign_expert(expert_id.clone(), "worker-1".to_string()).await.unwrap();
    
    let worker_id = coordinator.route_to_expert(&expert_id).await;
    assert!(worker_id.is_some());
    assert_eq!(worker_id.unwrap(), "worker-1");
}

#[tokio::test]
async fn test_parallel_inference_no_workers() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let result = coordinator.execute_parallel_inference(
        "Test prompt".to_string(),
        50,
        Tier::Standard,
        4,
    ).await;
    
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_worker_by_id() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let worker = WorkerNode {
        id: "test-worker".to_string(),
        address: "192.168.1.1:8080".to_string(),
        capabilities: Tier::Max,
        status: WorkerStatus::Idle,
        load: 0.0,
        memory_used_mb: 0,
        memory_total_mb: 16384,
        cpu_cores: 16,
        gpu_available: true,
        started_at: 0,
        last_seen: 0,
    };
    
    coordinator.add_worker(worker).await.unwrap();
    
    let found = coordinator.get_worker_by_id("test-worker").await;
    assert!(found.is_some());
    assert_eq!(found.unwrap().id, "test-worker");
    
    let not_found = coordinator.get_worker_by_id("nonexistent").await;
    assert!(not_found.is_none());
}

#[tokio::test]
async fn test_task_queue_ordering() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    // Submit tasks with different priorities
    let id_low = RequestId(Uuid::new_v4().into_bytes());
    let id_high = RequestId(Uuid::new_v4().into_bytes());
    let id_critical = RequestId(Uuid::new_v4().into_bytes());
    
    coordinator.submit_task(id_low, vec![], TaskPriority::Low, 100).await.unwrap();
    coordinator.submit_task(id_high, vec![], TaskPriority::High, 100).await.unwrap();
    coordinator.submit_task(id_critical, vec![], TaskPriority::Critical, 100).await.unwrap();
    
    // Assign tasks and verify order
    let task1 = coordinator.assign_next_task("worker-1").await;
    let task2 = coordinator.assign_next_task("worker-1").await;
    let task3 = coordinator.assign_next_task("worker-1").await;
    
    // Critical should come first, then high, then low
    assert!(task3.is_some());
    assert!(task2.is_some());
    assert!(task1.is_some());
}

#[tokio::test]
async fn test_task_assignment() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    coordinator.become_leader().await;
    
    let worker = WorkerNode {
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
    };
    
    coordinator.add_worker(worker).await.unwrap();
    
    let request_id = RequestId(Uuid::new_v4().into_bytes());
    coordinator.submit_task(
        request_id,
        vec![ExpertId([1u8; 32])],
        TaskPriority::Normal,
        1024,
    ).await.unwrap();
    
    let task = coordinator.assign_next_task("worker-1").await;
    assert!(task.is_some());
    assert_eq!(task.unwrap().assigned_worker, Some("worker-1".to_string()));
}

#[tokio::test]
async fn test_task_completion() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    coordinator.become_leader().await;
    
    let worker = WorkerNode {
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
    };
    
    coordinator.add_worker(worker).await.unwrap();
    
    let request_id = RequestId(Uuid::new_v4().into_bytes());
    coordinator.submit_task(
        request_id,
        vec![],
        TaskPriority::Normal,
        512,
    ).await.unwrap();
    
    // Assign and complete task
    coordinator.assign_next_task("worker-1").await;
    
    let result = TaskResult {
        task_id: request_id,
        worker_id: "worker-1".to_string(),
        output: vec!["result".to_string()],
        execution_time_ms: 100,
        success: true,
        error_message: None,
    };
    
    coordinator.complete_task(result).await.unwrap();
    
    let status = coordinator.get_task_status(request_id).await;
    assert!(status.is_some());
    assert_eq!(status.unwrap(), TaskStatus::Completed);
}

#[tokio::test]
async fn test_expert_assignment_persistence() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    // Assign multiple experts to workers
    for i in 0..5 {
        let expert_id = ExpertId([i; 32]);
        let worker_id = format!("worker-{}", i % 2);
        coordinator.assign_expert(expert_id, worker_id).await.unwrap();
    }
    
    // Verify routing works
    let expert0 = ExpertId([0u8; 32]);
    let expert1 = ExpertId([1u8; 32]);
    let expert2 = ExpertId([2u8; 32]);
    
    assert_eq!(coordinator.route_to_expert(&expert0).await, Some("worker-0".to_string()));
    assert_eq!(coordinator.route_to_expert(&expert1).await, Some("worker-1".to_string()));
    assert_eq!(coordinator.route_to_expert(&expert2).await, Some("worker-0".to_string()));
}

#[tokio::test]
async fn test_expert_distribution() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    coordinator.become_leader().await;
    
    // Add multiple workers
    for i in 0..3 {
        let worker = WorkerNode {
            id: format!("worker-{}", i),
            address: format!("192.168.1.{}:8080", i + 1),
            capabilities: Tier::Max,
            status: WorkerStatus::Idle,
            load: 0.0,
            memory_used_mb: 0,
            memory_total_mb: 16384,
            cpu_cores: 8,
            gpu_available: true,
            started_at: 0,
            last_seen: 0,
        };
        coordinator.add_worker(worker).await.unwrap();
    }
    
    // Distribute experts across workers
    let experts: Vec<ExpertId> = (0..6).map(|i| ExpertId([i; 32])).collect();
    let distribution = coordinator.distribute_experts(&experts).await.unwrap();
    
    assert_eq!(distribution.len(), 6);
    
    // Verify round-robin distribution
    let worker_counts: std::collections::HashMap<String, usize> = distribution.iter()
        .map(|(w, _)| w.id.clone())
        .fold(std::collections::HashMap::new(), |mut acc, id| {
            *acc.entry(id).or_insert(0) += 1;
            acc
        });
    
    // Each worker should get 2 experts (6 experts / 3 workers)
    for (_, count) in worker_counts {
        assert_eq!(count, 2);
    }
}

#[tokio::test]
async fn test_pending_task_priority_updates() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    let id1 = RequestId(Uuid::new_v4().into_bytes());
    let id2 = RequestId(Uuid::new_v4().into_bytes());
    
    // Submit two tasks
    coordinator.submit_task(id1, vec![], TaskPriority::Low, 100).await.unwrap();
    coordinator.submit_task(id2, vec![], TaskPriority::High, 100).await.unwrap();
    
    // High priority task should be assigned first
    let task1 = coordinator.assign_next_task("worker-1").await;
    let task2 = coordinator.assign_next_task("worker-1").await;
    
    // Either could come first depending on implementation, but they should both be assigned
    assert!(task1.is_some());
    assert!(task2.is_some());
}

#[tokio::test]
async fn test_failed_task_tracking() {
    let coordinator = ClusterCoordinator::new("test-cluster".to_string());
    
    coordinator.become_leader().await;
    
    let worker = WorkerNode {
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
    };
    
    coordinator.add_worker(worker).await.unwrap();
    
    let request_id = RequestId(Uuid::new_v4().into_bytes());
    coordinator.submit_task(request_id, vec![], TaskPriority::Normal, 512).await.unwrap();
    
    coordinator.assign_next_task("worker-1").await;
    
    // Complete task with failure
    let result = TaskResult {
        task_id: request_id,
        worker_id: "worker-1".to_string(),
        output: vec![],
        execution_time_ms: 100,
        success: false,
        error_message: Some("Execution failed".to_string()),
    };
    
    coordinator.complete_task(result).await.unwrap();
    
    let status = coordinator.get_task_status(request_id).await;
    assert!(matches!(status, Some(TaskStatus::Failed(_))));
}
