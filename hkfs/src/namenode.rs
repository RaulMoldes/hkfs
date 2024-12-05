use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const MAX_INACTIVITY_DURATION: u64 = 10; // Máxima inactividad en segundos
const NAMENODE_PORT: &str = "127.0.0.1:7878";
const VERIFICATION_INTERVAL: u64 = 15;

#[derive(Debug)]
struct DataNodeInfo {
    is_active: bool,
    last_heartbeat: Instant,
}

#[derive(Debug)]
struct NameNode {
    nodes: Arc<Mutex<HashMap<u32, DataNodeInfo>>>,
    next_id: Arc<Mutex<u32>>, // Contador de IDs únicos
}

impl NameNode {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)), // El primer ID será 1
        }
    }

    pub fn handle_client(&self, mut stream: TcpStream) {
        let mut buffer = [0; 128];
        while let Ok(size) = stream.read(&mut buffer) {
            if size == 0 {
                break; // Conexión cerrada
            }

            let message = String::from_utf8_lossy(&buffer[..size]);
            let parts: Vec<&str> = message.trim().split(':').collect();

            if parts.len() == 2 && parts[0] == "register" {
                // El primer mensaje es un registro, asignar un ID
                let node_id = self.assign_id();
                self.register_node(node_id);

                // Enviar el ID al DataNode
                let response = format!("ack:{}\n", node_id);
                if let Err(e) = stream.write_all(response.as_bytes()) {
                    eprintln!("Error al enviar respuesta: {}", e);
                }
            } else if parts.len() == 2 && parts[0] == "heartbeat" {
                // Si el nodo ya está registrado, simplemente actualizamos el heartbeat
                if let Ok(node_id) = parts[1].parse::<u32>() {
                    self.receive_heartbeat(node_id);
                    stream
                        .write_all(b"ack\n")
                        .expect("Error al enviar la respuesta");
                }
            }
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
        println!("Node {} registrado.", node_id);
    }

    pub fn receive_heartbeat(&self, node_id: u32) {
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(node) = nodes.get_mut(&node_id) {
            node.last_heartbeat = Instant::now();
            node.is_active = true;
            println!("Heartbeat recibido de Node {}.", node_id);
        }
    }

    pub fn assign_id(&self) -> u32 {
        let mut next_id = self.next_id.lock().unwrap();
        let id = *next_id;
        *next_id += 1; // Incrementar el ID para el siguiente nodo
        id
    }

    pub fn check_nodes(&self) {
        let mut nodes = self.nodes.lock().unwrap();
        let now = Instant::now();

        for (node_id, node_info) in nodes.iter_mut() {
            if now.duration_since(node_info.last_heartbeat) > Duration::from_secs(MAX_INACTIVITY_DURATION) {
                node_info.is_active = false;
                println!("Node {} está marcado como inactivo.", node_id);
            }
        }
    }
}

fn main() {
    let namenode = Arc::new(NameNode::new());
    let namenode_clone = Arc::clone(&namenode);

    // Iniciar el servidor TCP
    let listener = TcpListener::bind(NAMENODE_PORT).expect("No se pudo iniciar el servidor");
    println!("NameNode está escuchando....");

    // Hilo para verificar nodos periódicamente
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(VERIFICATION_INTERVAL));
        namenode_clone.check_nodes();
    });

    // Aceptar conexiones de DataNodes
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let namenode_clone = Arc::clone(&namenode);
                thread::spawn(move || namenode_clone.handle_client(stream));
            }
            Err(e) => eprintln!("Error al aceptar conexión: {}", e),
        }
    }
}
