use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAX_INACTIVITY_DURATION: u64 = 10;

pub struct DataNodeInfo {
    pub is_active: bool,
    pub last_heartbeat: Instant,
}

pub struct NameNode {
    nodes: Arc<Mutex<HashMap<u32, DataNodeInfo>>>, // Protegido para acceso concurrente
}

impl NameNode {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register_node(&self, node_id: u32) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(
            node_id,
            DataNodeInfo {
                is_active: true,
                last_heartbeat: Instant::now(),
            },
        );
        println!("Node {} registered.", node_id);
    }

    pub fn receive_heartbeat(&self, node_id: u32) {
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(node) = nodes.get_mut(&node_id) {
            node.last_heartbeat = Instant::now();
            node.is_active = true;
            println!("Heartbeat received from Node {}.", node_id);
        } else {
            println!("Node {} not registered. Ignoring heartbeat.", node_id);
        }
    }

    pub fn check_nodes(&self) {
        let mut nodes = self.nodes.lock().unwrap();
        let now = Instant::now();
        for (node_id, node_info) in nodes.iter_mut() {
            if now.duration_since(node_info.last_heartbeat) > Duration::from_secs(MAX_INACTIVITY_DURATION) {
                node_info.is_active = false;
                println!("Node {} is marked as inactive.", node_id);
            }
        }
    }
}
