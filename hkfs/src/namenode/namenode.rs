mod messages;

use actix_web::{web, App, HttpResponse, HttpServer};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

use messages::{BlockReadRequest, BlockStoreRequest, DataMessage, DataNodeMessage, StatusResponse};

const MAX_INACTIVITY_DURATION: u64 = 4; // Máxima inactividad en segundos
const NAMENODE_PORT: usize = 7878;
const API_PORT: usize = 8080;
const VERIFICATION_INTERVAL: u64 = 8;
// Lista predeterminada de claves válidas como constante global
const DEFAULT_KEYS: &[&str] = &["SBXBUSKANKLAKA"];

#[derive(Debug)]
pub struct DataNodeInfo {
    pub is_active: bool,
    pub port: usize,
    pub last_heartbeat: Instant,
    pub block_ids: HashSet<String>,
}

#[derive(Debug)]
pub struct NameNode {
    pub nodes: Arc<Mutex<HashMap<u32, DataNodeInfo>>>,
    pub next_id: Arc<Mutex<u32>>, // Contador de IDs únicos
    pub valid_keys: HashSet<String>,
}

impl NameNode {
    pub fn new(valid_keys: Option<HashSet<String>>) -> Self {
        // Convertimos DEFAULT_KEYS en Vec<String> para el campo valid_keys
        let default_keys: HashSet<String> = DEFAULT_KEYS.iter().map(|&s| s.to_string()).collect();

        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)), // El primer ID será 1
            valid_keys: valid_keys.unwrap_or(default_keys), // Usamos unwrap_or para usar la conversión
        }
    }
    // Method to communicate with DataNodes
    pub async fn handle_datanodes(&self, mut stream: TcpStream) {
        loop {
            let mut buffer = vec![0; 128];
            match stream.read(&mut buffer).await {
                Ok(size) if size > 0 => {
                    let message = String::from_utf8_lossy(&buffer[..size]);
                    match serde_json::from_str::<DataNodeMessage>(&message) {
                        Ok(DataNodeMessage::Register { key }) => {
                            let node_id = self.assign_id().await;
                            self.register_node(key, &mut stream, node_id).await;
                        }
                        Ok(DataNodeMessage::Heartbeat { node_id }) => {
                            self.receive_heartbeat(&mut stream, node_id).await;
                        }
                        Ok(DataNodeMessage::BlockData { data }) => {
                            println!("Recibido  bloque {:?}", data);

                            // Implementar la lógica para leer el bloque del DataNode
                        }
                        Err(_) => eprintln!("Mensaje inválido recibido: {}", message),
                    }
                }
                Ok(_) => {
                    println!("No hay datos de entrada");
                    break;
                }
                Err(e) => {
                    eprintln!("Error al leer del stream: {}", e);
                    break;
                }
            }
        }
    }

    pub async fn register_node(&self, key: String, stream: &mut TcpStream, node_id: u32) {
        if !self.valid_keys.contains(&key) {
            eprintln!("Registro rechazado para {}: clave inválida", key);
            let _ = stream.write_all(b"register_rejected:invalid_key\n").await;
            return;
        }
        let mut nodes = self.nodes.lock().await;
        nodes.insert(
            node_id,
            DataNodeInfo {
                is_active: true,
                port: API_PORT + node_id as usize,
                last_heartbeat: Instant::now(),
                block_ids: HashSet::new(),
            },
        );
        println!("Node {} registrado con éxito!", node_id);
        let response = format!("register_accepted:{}", node_id);
        let _ = stream.write_all(response.as_bytes()).await;
    }

    pub async fn receive_heartbeat(&self, stream: &mut TcpStream, node_id: u32) {
        let mut nodes = self.nodes.lock().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.last_heartbeat = Instant::now();
            node.is_active = true;
            println!("Heartbeat recibido de Node {}.", node_id);
        }
        let response = format!("received_heartbeat:{}", node_id);
        let _ = stream.write_all(response.as_bytes()).await;
    }

    pub async fn assign_id(&self) -> u32 {
        let mut next_id = self.next_id.lock().await;
        let id = *next_id;
        *next_id += 1; // Incrementar el ID para el siguiente nodo
        id
    }

    pub async fn check_nodes(&self) {
        let mut nodes = self.nodes.lock().await;
        let now = Instant::now();

        for (node_id, node_info) in nodes.iter_mut() {
            if now.duration_since(node_info.last_heartbeat)
                > Duration::from_secs(MAX_INACTIVITY_DURATION)
            {
                node_info.is_active = false;
                println!("Node {} está marcado como inactivo.", node_id);
            }
        }
    }

    // API FUNCTIONS.
    // Communication with the client.
    pub async fn api_get_status(&self) -> HttpResponse {
        // Lock the nodes HashMap to read the data.
        let nodes = self.nodes.lock().await;
        let mut status = Vec::new();
        for (node_id, node_info) in nodes.iter() {
            let status_message = if node_info.is_active {
                "active"
            } else {
                "inactive"
            };
            // Push the status message to the vector.
            status.push(format!("Node {}: {}", node_id, status_message));
        }
        // Liberar el Mutex antes de devolver la respuesta
        drop(nodes);
        // Return the status as JSON.
        HttpResponse::Ok().json(StatusResponse { status })
    }

    // Método para almacenar un bloque en el primer DataNode disponible
    pub async fn api_storeblock(&self, block_id: String, message: String) -> HttpResponse {
        // Usamos un Mutex asíncrono para bloquear el acceso a la lista de nodos
        let mut nodes = self.nodes.lock().await;

        // Buscar el primer DataNode activo
        let node_id = nodes
            .iter()
            .find(|(_, node_info)| node_info.is_active)
            .map(|(node_id, _)| *node_id);

        // Cuando se encuentra un DataNode activo
        match node_id {
            Some(node_id) => {
                let response = format!("Bloque {} se almacena en Node {}", block_id, node_id);

                // Crear un nuevo stream y enviar un mensaje al DataNode para almacenar el bloque
                let datanode_port = nodes.get(&node_id).unwrap().port;
                println!("Conectando al DataNode en el puerto {}", &datanode_port);
                let datanode_address = format!("127.0.0.1:{}", datanode_port);
                match TcpStream::connect(datanode_address).await {
                    Ok(mut stream) => {
                        // Crear un mensaje de almacenamiento de bloque y enviarlo al DataNode
                        if let Some(node_info) = nodes.get_mut(&node_id) {
                            node_info.block_ids.insert(block_id.clone());
                        }
                        let data_message = DataMessage::StoreBlock {
                            block_id: block_id.clone(),
                            data: message.into_bytes(),
                        };
                        println!(
                            "Almacenando bloque con ID: {} en el nodo {}",
                            block_id, node_id
                        );
                        if let Some(node_info) = nodes.get_mut(&node_id) {
                            node_info.block_ids.insert(block_id.clone());
                        }
                        let message = serde_json::to_string(&data_message).unwrap();
                        if let Err(e) = stream.write_all(message.as_bytes()).await {
                            eprintln!("Error al enviar el mensaje al DataNode: {}", e);
                            return HttpResponse::InternalServerError()
                                .json("Error al comunicar con el DataNode");
                        }
                        HttpResponse::Ok().json(response)
                    }
                    Err(e) => {
                        eprintln!("Error al conectar con el DataNode: {}", e);
                        HttpResponse::InternalServerError()
                            .json("No se pudo conectar con el DataNode")
                    }
                }
            }
            None => HttpResponse::BadRequest()
                .json("No hay nodos activos disponibles para almacenar el bloque"),
        }
    }
    pub async fn api_readblock(&self, block_id: String) -> HttpResponse {
        // Usamos un Mutex asíncrono para bloquear el acceso a la lista de nodos
        let nodes = self.nodes.lock().await;

        // Buscar el DataNode que contiene el bloque
        let node_id = nodes
            .iter()
            .find(|(_, node_info)| node_info.block_ids.contains(&block_id))
            .map(|(node_id, _)| *node_id);

        match node_id {
            Some(node_id) => {
                println!(
                    "Recuperando bloque con ID: {} del nodo {}",
                    block_id, node_id
                );

                // Obtener la dirección del DataNode
                let datanode_port = nodes.get(&node_id).unwrap().port;
                let datanode_address = format!("127.0.0.1:{}", datanode_port);

                // Intentar conectar al DataNode de manera asíncrona
                match TcpStream::connect(datanode_address).await {
                    Ok(mut stream) => {
                        // Crear mensaje para leer el bloque
                        let data_message = DataMessage::ReadBlock {
                            block_id: block_id.clone(),
                        };

                        // Serializar el mensaje a JSON
                        let message = serde_json::to_string(&data_message).unwrap();

                        // Enviar el mensaje al DataNode
                        if let Err(e) = stream.write_all(message.as_bytes()).await {
                            eprintln!("Error al enviar el mensaje al DataNode: {}", e);
                            return HttpResponse::InternalServerError()
                                .json("Error al comunicar con el DataNode");
                        } else {
                            println!("Mensaje enviado al DataNode");
                        }

                        // Leer la respuesta del DataNode
                        let mut buffer = Vec::new();
                        if let Err(e) = stream.read_to_end(&mut buffer).await {
                            eprintln!("Error al leer los datos del DataNode: {}", e);
                            return HttpResponse::InternalServerError()
                                .json("Error al leer el bloque del DataNode");
                        } else {
                            println!("Datos leídos del DataNode");
                        }

                        // Intentamos deserializar el buffer como un valor JSON genérico
                        match serde_json::from_slice::<serde_json::Value>(&buffer) {
                            Ok(json) => {
                                // Acceder al campo "BlockData" dentro del JSON
                                if let Some(block_data) = json.get("BlockData") {
                                    // Acceder al campo "data" que es un arreglo de bytes
                                    if let Some(data) = block_data.get("data") {
                                        // Intentamos convertir el arreglo de bytes (Vec<u8>) en un String
                                        if let Some(bytes) = data.as_array() {
                                            let byte_values: Vec<u8> = bytes
                                                .iter()
                                                .filter_map(|v| v.as_u64())
                                                .map(|v| v as u8)
                                                .collect();

                                            match String::from_utf8(byte_values) {
                                                Ok(string_data) => {
                                                    // Responder con los datos convertidos a String
                                                    return HttpResponse::Ok().body(string_data);
                                                }
                                                Err(e) => {
                                                    eprintln!(
                                                        "Error al convertir los datos a String: {}",
                                                        e
                                                    );
                                                    return HttpResponse::InternalServerError()
                                                        .body(
                                                        "Error al convertir los datos   a String",
                                                    );
                                                }
                                            }
                                        } else {
                                            eprintln!(
                                                "El campo 'data' no es un arreglo de bytes válido."
                                            );
                                            return HttpResponse::BadRequest().body("El campo 'data' no es un arreglo de bytes   válido.");
                                        }
                                    } else {
                                        eprintln!("No se encontró el campo 'data' en BlockData.");
                                        return HttpResponse::BadRequest()
                                            .body("No se encontró el campo 'data' en BlockData.");
                                    }
                                } else {
                                    eprintln!("No se encontró 'BlockData' en el mensaje.");
                                    return HttpResponse::BadRequest()
                                        .body("No se encontró 'BlockData' en el mensaje.");
                                }
                            }
                            Err(e) => {
                                eprintln!("Error al deserializar el mensaje: {}", e);
                                return HttpResponse::InternalServerError()
                                    .body("Error al deserializar el mensaje.");
                            }
                        }
                    }
                    Err(e) => {
                        // Error al conectar con el DataNode
                        eprintln!("No se pudo conectar con el DataNode: {}", e);
                        HttpResponse::InternalServerError()
                            .json("No se pudo conectar con el DataNode")
                    }
                }
            }
            None => {
                // Si no se encontró un DataNode con el bloque
                println!("Bloque {} no encontrado en ningún DataNode", block_id);
                println!(
                    "Lista actual de bloques almacenados: {:?}",
                    nodes
                        .iter()
                        .map(|(_, node_info)| &node_info.block_ids)
                        .collect::<Vec<_>>()
                );

                // Responder con error 404
                HttpResponse::NotFound().json(format!(
                    "Bloque {} no encontrado en ningún DataNode",
                    block_id
                ))
            }
        }
    }
}

async fn start_namenode(namenode: web::Data<Arc<NameNode>>) {
    let namenode_clone = Arc::clone(&namenode);

    // Iniciar el servidor TCP utilizando Tokio en lugar de std::thread
    let listener = TcpListener::bind(format!("127.0.0.1:{}", NAMENODE_PORT))
        .await
        .expect("No se pudo iniciar el servidor");

    println!("NameNode está escuchando....");

    // Tarea asíncrona para verificar nodos periódicamente
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(VERIFICATION_INTERVAL)).await;
            namenode_clone.check_nodes().await;
        }
    });

    // Aceptar conexiones de DataNodes de manera asíncrona
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let namenode_clone = Arc::clone(&namenode);
                tokio::spawn(async move {
                    namenode_clone.handle_datanodes(stream).await;
                });
            }
            Err(e) => eprintln!("Error al aceptar conexión: {}", e),
        }
    }
}

async fn api_get_status(namenode: web::Data<Arc<NameNode>>) -> HttpResponse {
    namenode.api_get_status().await
}

async fn api_post_storeblock(
    namenode: web::Data<Arc<NameNode>>,
    payload: web::Json<BlockStoreRequest>,
) -> HttpResponse {
    // Parsear el ID del bloque desde el cuerpo de la solicitud
    let block_id = payload.block_id.clone();
    let message = payload.message.clone();
    print!("Almacenando bloque con ID: {}", block_id);
    namenode.api_storeblock(block_id.clone(), message).await;
    HttpResponse::Ok().json(format!("Bloque {} almacenado correctamente", block_id))
}

async fn api_get_readblock(
    namenode: web::Data<Arc<NameNode>>,
    query: web::Query<BlockReadRequest>,
) -> HttpResponse {
    let block_id = &query.block_id;

    // Imprimir el block_id recibido
    print!("Recuperando bloque con ID: {}", block_id);
    namenode.api_readblock(block_id.clone()).await
}

async fn run_api_server(namenode: web::Data<Arc<NameNode>>) {
    HttpServer::new(move || {
        App::new()
            .app_data(namenode.clone()) // Agrega el Arc<NameNode> a las rutas
            .route("/status", web::get().to(api_get_status))
            .route("/storeblock", web::post().to(api_post_storeblock)) // Agregar la ruta para almacenar bloques
            .route("/readblock", web::get().to(api_get_readblock)) // Agregar la ruta para recuperar el contenido de los bloques
    })
    .bind(format!("127.0.0.1:{}", API_PORT))
    .expect("Unable to start API server")
    .run()
    .await
    .expect("Failed to run API server");
}

#[tokio::main]
async fn main() {
    // Crear una instancia de NameNode
    let namenode = Arc::new(NameNode::new(None)); // Si es necesario, puedes pasar claves al constructor

    // Si solo quieres iniciar el servidor NameNode (sin API):
    let namenode_clone = Arc::clone(&namenode);

    // Iniciar servidor NameNode en un hilo separado usando Tokio
    tokio::spawn(async move {
        start_namenode(web::Data::new(namenode_clone)).await;
    });

    // Iniciar el servidor API (si es necesario junto con el servidor NameNode):
    let namenode_clone = Arc::clone(&namenode);

    // Ejecutar el servidor Actix Web
    run_api_server(web::Data::new(namenode_clone)).await;
}
