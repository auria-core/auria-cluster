use auria_cluster::{ClusterCoordinator, ClusterConfig, WorkerNode, WorkerStatus, Tier, TaskPriority};
use auria_core::RequestId;
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
