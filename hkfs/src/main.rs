use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod namenode;
mod datanode;

use namenode::NameNode;
use datanode::DataNode;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod namenode;
mod datanode;

use namenode::NameNode;
use datanode::DataNode;

// Definición de constantes
const NUM_NODES: u32 = 3; // Número de nodos
const CHECK_INTERVAL_SECS: u64 = 15; // Intervalo de verificación en segundos

fn main() {
    let namenode = Arc::new(NameNode::new());

    // Registrar nodos con identificadores numéricos
    for node_id in 1..=NUM_NODES {
        namenode.register_node(node_id);
    }

    // Crear DataNodes y asignarles el NameNode
    let mut threads = Vec::new();
    for node_id in 1..=NUM_NODES {
        let namenode_clone = Arc::clone(&namenode);
        threads.push(thread::spawn(move || {
            let node = DataNode::new(node_id, namenode_clone);
            node.send_heartbeat();
        }));
    }

    // Verificar nodos periódicamente
    let namenode_monitor_thread = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(CHECK_INTERVAL_SECS));
            namenode.check_nodes();
        }
    });

    // Esperar a que los hilos de los nodos terminen
    for (i, t) in threads.into_iter().enumerate() {
        match t.join() {
            Ok(_) => println!("Thread {} completed successfully.", i),
            Err(err) => eprintln!("Thread {} encountered an error: {:?}", i, err),
        }
    }

    // Esperar a que el hilo del monitor termine
    match namenode_monitor_thread.join() {
        Ok(_) => println!("NameNode monitor thread completed successfully."),
        Err(err) => eprintln!("NameNode monitor thread encountered an error: {:?}", err),
    }
}
