mod messages;


use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use messages::DataNodeMessage;

const MAX_INACTIVITY_DURATION: u64 = 4; // Máxima inactividad en segundos
const NAMENODE_PORT: &str = "127.0.0.1:7878";
const VERIFICATION_INTERVAL: u64 = 8;
// Lista predeterminada de claves válidas como constante global
const DEFAULT_KEYS: &[&str] = &["SBXBUSKANKLAKA"];

#[derive(Debug)]
struct DataNodeInfo {
    is_active: bool,
    last_heartbeat: Instant,
}

#[derive(Debug)]
struct NameNode {
    nodes: Arc<Mutex<HashMap<u32, DataNodeInfo>>>,
    next_id: Arc<Mutex<u32>>, // Contador de IDs únicos
    valid_keys: HashSet<String>
}

impl NameNode {
        pub fn new(valid_keys: Option<HashSet<String>>) -> Self {
            // Convertimos DEFAULT_KEYS en Vec<String> para el campo valid_keys
            let default_keys:HashSet<String>= DEFAULT_KEYS.iter().map(|&s| s.to_string()).collect();
    
            Self {
                nodes: Arc::new(Mutex::new(HashMap::new())),
                next_id: Arc::new(Mutex::new(1)), // El primer ID será 1
                valid_keys: valid_keys.unwrap_or_else(|| default_keys), // Usamos unwrap_or_else para usar la conversión
            }
        }
    
        pub fn handle_client(&self, mut stream: TcpStream) {
             // Buffer para leer los datos
            loop {
                let mut buffer = [0; 128];
                match stream.read(&mut buffer) {
                    Ok(size) if size > 0 => {
                        let message = String::from_utf8_lossy(&buffer[..size]);
                        match serde_json::from_str::<DataNodeMessage>(&message) {
                            Ok(DataNodeMessage::Register { key }) => {
                                let node_id = self.assign_id();
                                self.register_node(key, &mut stream, node_id);
                            }
                            Ok(DataNodeMessage::Heartbeat { node_id }) => {
                                self.receive_heartbeat(&mut stream, node_id);
                            }
                            Err(_) => eprintln!("Mensaje inválido recibido: {}", message),
                        }
                    }
                    Ok(_) => {
                        println!("No hay datos de entrada");
                        // Si `size == 0`, la conexión se cierra, así que salimos del loop
                        break;
                    }
                    Err(e) => {
                        eprintln!("Error al leer del stream: {}", e);
                        break; // En caso de error, salimos del loop
                    }
                }
            }
        }
    pub fn register_node(&self,key: String, stream: &mut TcpStream,  node_id: u32) {
        if !self.valid_keys.contains(&key) {
            eprintln!("Registro rechazado para {}: clave inválida", key);
            let _ = stream.write_all(b"register_rejected:invalid_key\n");
            return;
        }
        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(
            node_id,
            DataNodeInfo {
                is_active: true,
                last_heartbeat: Instant::now(),
            },
        );
        println!("Node {} registrado con éxito!", node_id);
        let response = format!("register_accepted:{}", node_id);
        let _ = stream.write_all(response.as_bytes());
    }

    pub fn receive_heartbeat(&self, stream: &mut TcpStream,  node_id: u32) {
        let mut nodes = self.nodes.lock().unwrap();
        if let Some(node) = nodes.get_mut(&node_id) {
            node.last_heartbeat = Instant::now();
            node.is_active = true;
            println!("Heartbeat recibido de Node {}.", node_id);
        }
        let response = format!("received_heartbeat:{}", node_id);
        let _ = stream.write_all(response.as_bytes());
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
    let namenode: Arc<NameNode> = Arc::new(NameNode::new(None));
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
