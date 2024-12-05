use std::sync::Arc;
use std::thread;
use std::time::Duration;
use crate::NameNode;

pub struct DataNode {
    node_id: u32,
    namenode: Arc<NameNode>, // El NameNode está embebido y es seguro para múltiples hilos
}

impl DataNode {
    pub fn new(node_id: u32, namenode: Arc<NameNode>) -> Self {
        Self { node_id, namenode }
    }

    pub fn send_heartbeat(&self) {
        loop {
            thread::sleep(Duration::from_secs(5));
            self.namenode.receive_heartbeat(self.node_id);
            println!("Node {} sent heartbeat.", self.node_id);
        }
    }
}
