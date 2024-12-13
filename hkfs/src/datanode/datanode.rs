mod blockdata;
mod messages;

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};

use blockdata::BlockData;
use messages::{DataMessage, DataNodeMessage};

const PARTITION_SIZE: usize = 24;
const DATA_PATH: &str = "data"; // Directorio donde se almacenarán los bloques
const NAMENODE_PORT: usize = 7878; // Puerto del NameNode
const DEFAULT_KEY: &str = "SBXBUSKANKLAKA";
const API_PORT: usize = 8080;

#[derive(Debug, Clone)]
struct BlockDataInfo {
    block_id: String,
    path: String,
}

#[derive(Debug, Clone)]
struct DataNode {
    id: Option<u32>, // El ID se asignará después del registro
    server_address: String,
    key: String,
    port: Option<usize>,
    blocks: Option<Vec<BlockDataInfo>>,
}

impl DataNode {
    fn new(server_address: String, key: String) -> Self {
        Self {
            id: None, // Sin ID al inicio
            server_address,
            key,
            port: None,
            blocks: None,
        }
    }

    async fn register(&mut self) -> Result<(), String> {
        let register_message = DataNodeMessage::Register {
            key: self.key.clone(),
        };

        // Intentar conectar al NameNode de manera asíncrona
        let mut stream = match TcpStream::connect(&self.server_address).await {
            Ok(stream) => stream,
            Err(e) => return Err(format!("Error al conectarse al NameNode: {}", e)),
        };

        // Serializar el mensaje de registro
        let serialized = match serde_json::to_string(&register_message) {
            Ok(msg) => msg,
            Err(e) => return Err(format!("Error al serializar el mensaje de registro: {}", e)),
        };

        // Enviar el mensaje al NameNode
        if let Err(e) = stream.write_all(serialized.as_bytes()).await {
            return Err(format!("Error al enviar el mensaje de registro: {}", e));
        }

        // Leer la respuesta del NameNode
        let mut buffer = [0; 128];
        let size = match stream.read(&mut buffer).await {
            Ok(size) => size,
            Err(e) => return Err(format!("Error al leer la respuesta del NameNode: {}", e)),
        };

        // Convertir la respuesta en una cadena
        let response = String::from_utf8_lossy(&buffer[..size]);

        // Verificar la respuesta y extraer el ID
        if let Some(id_str) = response.strip_prefix("register_accepted:") {
            match id_str.parse::<u32>() {
                Ok(id) => {
                    let port = API_PORT + id as usize;
                    println!("Registro exitoso, ID asignado: {}", id);
                    self.id = Some(id);
                    self.port = Some(port);
                    Ok(())
                }
                Err(_) => Err("Error al parsear el ID recibido".to_string()),
            }
        } else {
            Err(format!("Respuesta inesperada del NameNode: {}", response))
        }
    }

    async fn start_listening(&mut self) -> Option<TcpListener> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port.unwrap()))
            .await
            .unwrap();
        println!(
            "Escuchando en el puerto {} para recibir bloques...",
            self.port.unwrap()
        );

        Some(listener)
    }

    async fn handle_connection(&mut self, mut stream: tokio::net::TcpStream) {
        let mut buffer = vec![0; 2048];

        // Intentamos leer los datos del stream
        match stream.read(&mut buffer).await {
            Ok(n @ 1..) => {
                println!("Se leyeron {} bytes del stream", n);
                let data = &buffer[..n]; // Recortamos el buffer a los datos válidos

                // Intentamos deserializar el mensaje recibido
                match serde_json::from_slice::<DataMessage>(data) {
                    Ok(DataMessage::StoreBlock { block_id, data }) => {
                        println!(
                            "Bloque recibido: ID: {}, Tamaño de los datos: {} bytes",
                            block_id,
                            data.len()
                        );
                        // Almacenar el bloque
                        self.store_block(block_id, &data).await;
                    }
                    Ok(DataMessage::ReadBlock { block_id }) => {
                        println!("Se solicita el bloque con ID: {}", block_id);

                        // Enviar el bloque solicitado
                        match self.send_block(block_id).await {
                            Ok(data) => {
                                let response_message = DataNodeMessage::BlockData { data };
                                match serde_json::to_string(&response_message) {
                                    Ok(serialized) => {
                                        if let Err(e) =
                                            stream.write_all(serialized.as_bytes()).await
                                        {
                                            eprintln!(
                                                "Error al enviar el mensaje de respuesta: {}",
                                                e
                                            );
                                        } else if let Err(e) = stream.flush().await {
                                            eprintln!(
                                                "Error al vaciar el stream después del envío: {}",
                                                e
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "Error al serializar el mensaje de respuesta: {}",
                                            e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Error al enviar el bloque: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error al deserializar el mensaje: {}", e);
                    }
                }
            }
            Ok(0) => {
                eprintln!("La conexión se cerró por el cliente");
            }
            Err(e) => {
                eprintln!("Error al leer datos del stream: {}", e);
            }
        }
    }
    // Método para simular el almacenamiento de un bloque
    async fn store_block(&mut self, block_id: String, data: &[u8]) {
        println!("Almacenando bloque con ID {}. Datos: {:?}", block_id, data);

        let path = format!("{}/{}/", DATA_PATH, block_id);
        let block = BlockData::new(block_id.clone(), path.clone());
        let _ = block.store_block(PARTITION_SIZE, data.to_vec()).await;

        let block_info = BlockDataInfo {
            block_id: block_id.clone(),
            path: path.clone(),
        };

        // Si la lista de bloques es None, inicializarla
        if let Some(ref mut blocks) = self.blocks {
            blocks.push(block_info);
        } else {
            self.blocks = Some(vec![block_info]);
        }
        println!("Bloque almacenado correctamente.");
    }

    async fn send_heartbeat(&mut self) {
        // Comprobamos si el DataNode está registrado (si tiene un ID)

        // Creamos el mensaje de Heartbeat con el node_id
        let node_id = self.id.unwrap();
        let heartbeat_message = DataNodeMessage::Heartbeat { node_id };

        // Intentamos conectar de manera asincrónica al NameNode
        match TcpStream::connect(&self.server_address).await {
            Ok(mut stream) => {
                // Serializamos el mensaje
                let serialized = match serde_json::to_string(&heartbeat_message) {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!("Error al serializar el mensaje de heartbeat: {}", e);
                        return;
                    }
                };

                // Enviamos el mensaje de heartbeat de manera asincrónica
                if let Err(e) = stream.write_all(serialized.as_bytes()).await {
                    eprintln!("Error al enviar el mensaje de heartbeat: {}", e);
                    return;
                }

                // Leemos la respuesta del NameNode
                let mut buffer = [0; 128];
                let size = match stream.read(&mut buffer).await {
                    Ok(size) => size,
                    Err(e) => {
                        eprintln!("Error al leer la respuesta del NameNode: {}", e);
                        return;
                    }
                };

                // Convertimos la respuesta a un string
                let response = String::from_utf8_lossy(&buffer[..size]);

                // Verificamos la respuesta
                if response.starts_with("received_heartbeat") {
                    println!("Se envió el heartbeat correctamente. Sigo activo.");
                } else {
                    eprintln!("Error: {}", response);
                }
            }
            Err(e) => {
                eprintln!("Error al conectarse al NameNode: {}", e);
            }
        }
    }

    async fn send_block(&mut self, block_id: String) -> Result<Vec<u8>, String> {
        println!("Checando bloques...");

        if let Some(blocks) = &self.blocks {
            // Buscar el bloque con el ID proporcionado
            if let Some(block_info) = blocks.iter().find(|b| b.block_id == block_id) {
                let mut block = BlockData {
                    block_id: block_info.block_id.clone(),
                    path: block_info.path.clone(),
                };

                println!("Bloque encontrado: {}", block_info.block_id);

                // Intentar cargar los datos del bloque con un timeout
                match timeout(Duration::from_secs(10), block.load_block()).await {
                    Ok(Ok(data)) => {
                        println!("Bloque cargado correctamente.");
                        Ok(data)
                    }
                    Ok(Err(e)) => {
                        eprintln!("Error al cargar el bloque: {}", e);
                        Err("Error al cargar el bloque.".into())
                    }
                    Err(_) => {
                        eprintln!("Timeout al cargar el bloque.");
                        Err("Timeout al cargar el bloque.".into())
                    }
                }
            } else {
                let err_msg = format!("Bloque con ID {} no encontrado.", block_id);
                eprintln!("{}", err_msg);
                Err(err_msg)
            }
        } else {
            let err_msg = "No hay bloques almacenados.".to_string();
            eprintln!("{}", err_msg);
            Err(err_msg)
        }
    }
}

#[tokio::main]
async fn main() {
    let datanode = Arc::new(Mutex::new(DataNode::new(
        format!("127.0.0.1:{}", NAMENODE_PORT),
        DEFAULT_KEY.to_string(),
    )));

    // Registrar el DataNode
    {
        let mut datanode_guard = datanode.lock().await;
        if let Err(e) = datanode_guard.register().await {
            eprintln!("Error al registrar el DataNode: {:?}", e);
            return;
        }
    }

    // Empezar a escuchar conexiones:
    let listener = {
        let mut datanode_guard = datanode.lock().await;
        datanode_guard.start_listening().await
    };

    // Tarea asíncrona para enviar el heartbeat
    let datanode_clone_heartbeat = Arc::clone(&datanode);
    let heartbeat_handler = tokio::spawn(async move {
        loop {
            let mut datanode_guard = datanode_clone_heartbeat.lock().await;
            datanode_guard.send_heartbeat().await;
            sleep(Duration::from_secs(1)).await; // Espera 1 segundo antes de enviar el siguiente heartbeat
        }
    });

    // Tarea asíncrona para escuchar conexiones
    let datanode_clone_connections = Arc::clone(&datanode);
    let connection_handle = tokio::spawn(async move {
        loop {
            if let Some(ref listener) = listener {
                // Esperamos a que lleguen conexiones
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let mut datanode_guard = datanode_clone_connections.lock().await;
                        datanode_guard.handle_connection(stream).await;
                    }
                    Err(e) => {
                        eprintln!("Error al aceptar conexión: {}", e);
                    }
                }
            } else {
                eprintln!("No se pudo obtener el listener");
            }

            sleep(Duration::from_secs(1)).await; // Espera antes de aceptar la siguiente conexión
        }
    });
    let _ = tokio::join!(heartbeat_handler, connection_handle);
}
