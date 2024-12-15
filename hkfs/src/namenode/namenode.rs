mod messages;

use actix_web::{web, App, HttpResponse, HttpServer};
use futures::future::join_all;
use messages::{DataMessage, DataNodeMessage, GetRequest, PostRequest, Response};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

const REPLICATION_FACTOR: usize = 3; // Factor de replicación
const MAX_INACTIVITY_DURATION: u64 = 4; // Máxima inactividad en segundos
const NAMENODE_PORT: usize = 7878;
const API_PORT: usize = 8080;
const PARTITION_SIZE: usize = 1024; // Tamaño máximo de la partición
const VERIFICATION_INTERVAL: u64 = 8;
// Lista predeterminada de claves válidas como constante global
const DEFAULT_KEYS: &[&str] = &["SBXBUSKANKLAKA"];

#[derive(Clone, Debug)]
pub struct PartitionInfo {
    pub node: u32,
    pub block: u32,     // Cambiado a u32
    pub partition: u32, // Cambiado a u32
}

#[derive(Debug)]
pub struct NameNode {
    pub nodes: Arc<Mutex<HashMap<u32, DataNodeInfo>>>,
    pub partitions: Arc<Mutex<HashMap<u32, PartitionInfo>>>, // Cambiado a u32 para partition_id
    pub next_id: Arc<Mutex<u32>>,
    pub next_node_id: Arc<Mutex<u32>>,
    pub valid_keys: HashSet<String>,
    pub replication_factor: usize,

}

#[derive(Debug, Clone)]
pub struct DataNodeInfo {
    pub is_active: bool,
    pub port: usize,
    pub last_heartbeat: Instant,
    pub block_ids: HashSet<u32>,
}

impl NameNode {
    pub fn new(valid_keys: Option<HashSet<String>>, replication_factor:Option<usize>) -> Self {
        // Convertimos DEFAULT_KEYS en Vec<String> para el campo valid_keys
        let default_keys: HashSet<String> = DEFAULT_KEYS.iter().map(|&s| s.to_string()).collect();
        let replication_factor = replication_factor.unwrap_or(REPLICATION_FACTOR);
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
            partitions: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)), // El primer ID será 1
            next_node_id: Arc::new(Mutex::new(1)), // El primer ID será 1
            valid_keys: valid_keys.unwrap_or(default_keys), // Usamos unwrap_or para usar la conversión
            replication_factor,
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

    pub async fn partition_block(data: Vec<u8>, partition_size: usize) -> Vec<Vec<u8>> {
        // Validación: Si el tamaño de la partición es 0, devolvemos un vector vacío.
        if partition_size == 0 {
            eprintln!("Error: El tamaño de la partición no puede ser 0.");
            return Vec::new();
        }

        // Si los datos están vacíos, devolvemos un vector vacío.
        if data.is_empty() {
            eprintln!("Advertencia: Los datos están vacíos, no se crearán particiones.");
            return Vec::new();
        }

        let mut partitions = Vec::new();
        let mut start = 0;

        while start < data.len() {
            let end = std::cmp::min(start + partition_size, data.len());
            partitions.push(data[start..end].to_vec());
            start = end;
        }

        partitions
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
        HttpResponse::Ok().json(Response::Status { status })
    }
    pub async fn api_storeblock(&self, block_id: u32, message: String) -> HttpResponse {
        let data = message.into_bytes(); // Convertir el mensaje a bytes
        println!("ENTRO A LA FUNCION api_storeblock");
        // Si el tamaño del bloque es mayor que 1 KB, particionamos
        let partitions = if data.len() > PARTITION_SIZE {
            println!("El bloque es demasiado grande, particionando...");
            NameNode::partition_block(data, PARTITION_SIZE).await
        } else {
            vec![data] // Si no es mayor que 1 KB, solo lo enviamos entero
        };
        for (i, partition) in partitions.iter().enumerate() {
            println!("Partición {}: tamaño {} bytes", i, partition.len());
        }


        // Round Robin para seleccionar los nodos
        // Se seleccionan los primeros N nodos activos
        let node_ids_and_ports: Vec<(u32, usize)> = {
            let nodes = self.nodes.lock().await;
            let next_node_id = *self.next_node_id.lock().await;
            // Buscar los primeros N nodos activos y obtener sus IDs y puertos
            let nodes_ports = nodes.iter()
            .filter(|(_node_id, node_info)| node_info.is_active)
        .map(|(node_id, node_info)| (*node_id, node_info.port))
        .skip_while(|(node_id, _)| *node_id < next_node_id)
        .take(self.replication_factor)
        .collect();

        //POner el next_node_id en el siguiente nodo
        *self.next_node_id.lock().await = next_node_id + self.replication_factor  as u32;
        if next_node_id + self.replication_factor as u32 > nodes.len() as u32 {
            *self.next_node_id.lock().await = 1;
        } else {
            *self.next_node_id.lock().await = next_node_id +self.replication_factor  as u32;
        }

        nodes_ports
        }; // El Mutex se desbloquea aquí

        // Verificamos si tenemos al menos REPLICATION_FACTOR nodos activos
        if node_ids_and_ports.len() < REPLICATION_FACTOR {
            return HttpResponse::BadRequest().json(Response::Error {
                message:
                    "No hay suficientes DataNodes activos disponibles para replicar la partición"
                        .to_string(),
            });
        }
        // Crear la lista de futuros
        let store_futures = partitions.iter().enumerate().flat_map(|(i, partition)| {
            // Generar un id único para cada partición
            let partition_id = block_id * 1000 + i as u32;
            println!(
                "Almacenando partición {} del bloque {}",
                partition_id, block_id
            );

            // Iterar sobre cada combinación de (node_id, port) con la partición
            node_ids_and_ports.iter().map(move |(node_id, port)| {
                // Clonar los valores para moverlos al bloque async
                let port = *port;
                println!(
                    "Almacenando partición {} en el nodo {}",
                    partition_id, node_id
                );
                // Llamar a store_partition con el nodo y la partición correspondiente
                self.store_partition(
                    *node_id,
                    "127.0.0.1".to_string(),
                    port,
                    block_id,
                    partition_id,
                    partition.clone(),
                )
            })
        });
        // Ejecutar todas las tareas en paralelo y esperar sus resultados
        let responses = join_all(store_futures).await;

        // Verificar si alguna de las respuestas contiene un error
        if responses
            .iter()
            .any(|response| response.status().is_server_error())
        {
            return HttpResponse::InternalServerError().json(Response::Error {
                message: "Error al almacenar particiones".to_string(),
            });
        }

        HttpResponse::Ok().json(Response::Ok {
            message: format!("Bloque {} almacenado y particionado.", block_id),
        })
    }

    pub async fn store_partition(
        &self,
        datanode_id: u32,     // Recibe el ID del DataNode
        datanode_ip: String,  // Recibe la IP del DataNode
        datanode_port: usize, // Recibe el puerto del DataNode
        block_id: u32,
        partition_id: u32,
        data: Vec<u8>,
    ) -> HttpResponse {
        println!(
            "Entrando a store_partition con IP: {}, Puerto: {}, block_id: {}, partition_id: {}",
            datanode_ip, datanode_port, block_id, partition_id
        );

        // Almacenar la información de la partición en el NameNode
        {
            let mut partitions = self.partitions.lock().await;
            partitions.insert(
                partition_id,
                PartitionInfo {
                    node: datanode_id, // Si es necesario, reemplaza con el ID del nodo o usa un valor adecuado
                    partition: partition_id,
                    block: block_id,
                },
            );
        }
        println!("Información de la partición almacenada en el NameNode");

        // Intentar conectar y enviar datos al DataNode seleccionado
        let datanode_address = format!("{}:{}", datanode_ip, datanode_port);
        println!("Conectando al DataNode en {}", datanode_address);

        match TcpStream::connect(&datanode_address).await {
            Ok(mut stream) => {
                let data_message = DataMessage::StoreBlock {
                    block_id: partition_id,
                    data: data.clone(),
                };
                println!("Conexión exitosa al DataNode {}", datanode_address);

                let message = serde_json::to_string(&data_message).unwrap();
                println!("Tamaño del mensaje: {} bytes", message.len());
                if let Err(e) = stream.write_all(message.as_bytes()).await {
                    eprintln!(
                        "Error al enviar la partición {} al DataNode {}: {}",
                        partition_id, datanode_address, e
                    );
                    return HttpResponse::InternalServerError().json(Response::Error {
                        message: format!("Error al comunicar con el DataNode {}", datanode_address),
                    });
                }

                println!(
                    "Partición {} del bloque {} replicada en el nodo {}",
                    partition_id, block_id, datanode_address
                );
                HttpResponse::Ok().json(Response::Ok {
                    message: format!(
                        "Partición {} del bloque {} almacenada en el nodo {}",
                        partition_id, block_id, datanode_address
                    ),
                })
            }
            Err(e) => {
                eprintln!(
                    "Error al conectar con el DataNode {}: {}",
                    datanode_address, e
                );
                HttpResponse::InternalServerError().json(Response::Error {
                    message: format!("No se pudo conectar con el DataNode {}", datanode_address),
                })
            }
        }
    }

    pub async fn api_readblock(&self, block_id: u32) -> HttpResponse {
        let block_partitions: Vec<PartitionInfo> = {
            // Usamos un Mutex asíncrono para bloquear el acceso a las particiones
            let partitions = self.partitions.lock().await;

            // Buscar todas las particiones del bloque solicitado (filtramos por block_id)
            partitions
                .values()
                .filter(|partition_info| partition_info.block == block_id)
                .cloned()
                .collect()
        }; // Liberar el Mutex antes de continuar

        if block_partitions.is_empty() {
            // Si no se encontraron particiones para el bloque
            return HttpResponse::NotFound().json(Response::Error {
                message: format!("Bloque {} no encontrado en ningún DataNode.", block_id),
            });
        }

        // Clonar la información de los nodos para liberar el Mutex antes de continuar
        let nodes_info: HashMap<u32, DataNodeInfo> = {
            let nodes = self.nodes.lock().await;
            nodes.clone()
        }; // Liberar el Mutex antes de continuar
        let futures: Vec<_> = block_partitions
            .into_iter()
            .enumerate() // Añadir índice para elegir cíclicamente el nodo
            .map(|(index, partition_info)| {
                // Selección cíclica de los nodos
                let node_info = nodes_info.values().nth(index % nodes_info.len()).cloned(); // Selecciona el nodo cíclicamente
                println!(
                    "Recuperando partición {} del bloque {} del nodo {}",
                    partition_info.partition, partition_info.block, partition_info.node
                );
                async move {
                    if let Some(node_info) = node_info {
                        let datanode_address = format!("127.0.0.1:{}", node_info.port);

                        match TcpStream::connect(datanode_address).await {
                            Ok(mut stream) => {
                                // Crear mensaje para leer la partición
                                let data_message = DataMessage::ReadBlock {
                                    block_id: partition_info.partition,
                                };

                                let message = match serde_json::to_string(&data_message) {
                                    Ok(msg) => msg,
                                    Err(e) => {
                                        eprintln!("Error al serializar el mensaje: {}", e);
                                        return Err("Error al serializar el mensaje".to_string());
                                    }
                                };

                                // Enviar el mensaje al DataNode
                                if let Err(e) = stream.write_all(message.as_bytes()).await {
                                    eprintln!("Error al enviar el mensaje al DataNode: {}", e);
                                    return Err("Error al comunicar con el DataNode".to_string());
                                }

                                let mut buffer = Vec::new();
                                let mut temp_buffer = vec![0; 1024];

                                loop {
                                    match stream.read(&mut temp_buffer).await {
                                        Ok(0) => break, // Fin de la conexión
                                        Ok(n) => {
                                            buffer.extend_from_slice(&temp_buffer[..n]);
                                            // Intentar deserializar el JSON con el buffer acumulado
                                            match serde_json::from_slice::<DataNodeMessage>(&buffer)
                                            {
                                                Ok(_) => {
                                                    // Si la deserialización es exitosa, hemos recibido el mensaje completo
                                                    break;
                                                }
                                                Err(_) => {
                                                    // Si la deserialización falla, significa que aún no tenemos un mensaje completo
                                                    // Continuar leyendo más datos
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("Error al leer del stream: {}", e);
                                            return Err("Error al leer del stream".to_string());
                                        }
                                    }
                                }

                                // Intentamos deserializar el buffer como un valor JSON
                                match serde_json::from_slice::<DataNodeMessage>(&buffer) {
                                    Ok(DataNodeMessage::BlockData { data }) => {
                                        // Si la partición se encuentra correctamente, devolverla
                                        println!(
                                            "Partición {} del bloque {} recuperada del nodo {}",
                                            partition_info.partition,
                                            partition_info.block,
                                            partition_info.node
                                        );
                                        Ok(data)
                                    }
                                    Ok(_) => {
                                        println!("Mensaje de DataNode ignorado.");
                                        Err("Mensaje de DataNode no esperado".to_string())
                                    }
                                    Err(e) => {
                                        eprintln!("Error al deserializar el mensaje: {}", e);
                                        Err("Error al deserializar los datos del DataNode"
                                            .to_string())
                                    }
                                }
                            }
                            Err(e) => {
                                // Error al conectar con el DataNode
                                eprintln!("No se pudo conectar con el DataNode: {}", e);
                                Err("No se pudo conectar con el DataNode".to_string())
                            }
                        }
                    } else {
                        eprintln!(
                            "No se encontró información del DataNode para la partición {}",
                            partition_info.partition
                        );
                        Err("No se encontró información del DataNode".to_string())
                    }
                }
            })
            .collect();

        // Ejecutar todas las lecturas de las particiones de manera concurrente
        let results = join_all(futures).await;

        // Recolectar los datos y manejar los errores
        let mut reconstructed_data = Vec::new();
        let mut errors = Vec::new();

        for result in results {
            match result {
                Ok(data) => {
                    reconstructed_data.extend(data);
                }
                Err(e) => {
                    errors.push(e);
                }
            }
        }

        // Si hubo errores, devolver un mensaje de error con detalles
        if !errors.is_empty() {
            return HttpResponse::InternalServerError().json(Response::Error {
                message: format!("Errores al recuperar las particiones: {:?}", errors),
            });
        }
        //Convert reconstructed_data to a string
        let reconstructed_data_str = String::from_utf8(reconstructed_data).unwrap();
        // Si la reconstrucción fue exitosa, devolver los datos reconstruidos
        HttpResponse::Ok().json(Response::BlockData {
            data: reconstructed_data_str,
        })
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

// La función api_post ajustada
pub async fn api_post(namenode: web::Data<Arc<NameNode>>, req_body: String) -> HttpResponse {
    println!("Raw JSON body received: {}", req_body);

    match serde_json::from_str::<PostRequest>(&req_body) {
        Ok(temp_payload) => match temp_payload {
            PostRequest::StoreBlock { block_id, data } => {
                println!("Block ID (String) received: {}", block_id);

                // Intentar convertir el block_id de String a u32
                let block_id_u32: u32 = match block_id.parse() {
                    Ok(id) => id,
                    Err(_) => {
                        eprintln!("Error: El block_id proporcionado no es un número válido.");
                        return HttpResponse::BadRequest().json(Response::Error {
                            message: "El block_id proporcionado no es un número válido."
                                .to_string(),
                        });
                    }
                };

                println!("Almacenando bloque con ID: {}", block_id_u32);

                // Llamar a api_storeblock con el block_id convertido y los datos
                namenode.api_storeblock(block_id_u32, data).await
            }
        },
        Err(e) => {
            eprintln!("Error al deserializar el JSON: {}", e);
            HttpResponse::BadRequest().json(Response::Error {
                message: "Error al deserializar el JSON".to_string(),
            })
        }
    }
}

// La función API para manejar GET requests
async fn api_get(
    namenode: web::Data<Arc<NameNode>>,
    query: web::Query<GetRequest>, // Deserializamos en la estructura intermedia
) -> HttpResponse {
    match query.into_inner() {
        GetRequest { r#type, block_id } if r#type == "ReadBlock" => {
            if let Some(block_id) = block_id {
                println!("Recuperando bloque con ID: {}", block_id);

                // Intentar convertir el block_id de String a u32
                let block_id_u32: u32 = match block_id.parse() {
                    Ok(id) => id,
                    Err(_) => {
                        eprintln!("Error: El block_id proporcionado no es un número válido.");
                        return HttpResponse::BadRequest().json(Response::Error {
                            message: "El block_id proporcionado no es un número válido."
                                .to_string(),
                        });
                    }
                };

                namenode.api_readblock(block_id_u32).await
            } else {
                HttpResponse::BadRequest().json(Response::Error {
                    message: "Falta el parámetro 'block_id'".to_string(),
                })
            }
        }
        GetRequest { r#type, .. } if r#type == "GetStatus" => {
            println!("Obteniendo el estado del NameNode");
            namenode.api_get_status().await
        }
        _ => HttpResponse::BadRequest().json(Response::Error {
            message: "Solicitud no válida".to_string(),
        }),
    }
}

async fn run_api_server(namenode: web::Data<Arc<NameNode>>) {
    HttpServer::new(move || {
        App::new()
            .app_data(namenode.clone()) // Agrega el Arc<NameNode> a las rutas
            .route("/status", web::get().to(api_get))
            .route("/storeblock", web::post().to(api_post)) // Agregar la ruta para almacenar bloques
            .route("/readblock", web::get().to(api_get)) // Agregar la ruta para recuperar el contenido de los bloques
    })
    .bind(format!("127.0.0.1:{}", API_PORT))
    .expect("Unable to start API server")
    .run()
    .await
    .expect("Failed to run API server");
}

#[tokio::main]
async fn main() {
    // Obtener el factor de replicación desde los argumentos de la línea de comandos
    let args: Vec<String> = std::env::args().collect();
    let replication_factor = if args.len() > 1 {
        args[1].parse::<usize>().unwrap_or(REPLICATION_FACTOR)
    } else {
        REPLICATION_FACTOR
    };

    // Crear una instancia de NameNode con el factor de replicación especificado
    let namenode = Arc::new(NameNode::new(None, Some(replication_factor)));

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
