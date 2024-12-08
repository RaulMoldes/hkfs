mod blockdata;
mod messages;


use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use tokio::time::{sleep, Duration};

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
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port.unwrap())).await.unwrap();
        println!(
            "Escuchando en el puerto {} para recibir bloques...",
            self.port.unwrap()
        );

        Some(listener)
      
    }

    // Método para escuchar el puerto asignado para recibir StoreBlock
    async fn handle_connection(&mut self, mut stream: tokio::net::TcpStream) {
        let mut buffer = Vec::new();
        if let Err(e) = stream.read_to_end(&mut buffer).await {
            eprintln!("Error al leer datos del stream: {}", e);
            return;
        }

        match serde_json::from_slice::<DataMessage>(&buffer) {
            Ok(DataMessage::StoreBlock { block_id, data }) => {
                println!(
                    "Bloque recibido: ID: {}, Tamaño de los datos: {} bytes",
                    block_id,
                    data.len()
                );
                self.store_block(block_id, &data).await;
            }
            Ok(DataMessage::ReadBlock { block_id }) => {
                println!("Se solicita el bloque con ID: {}", block_id);
                self.send_block(block_id).await;
            }
            Err(e) => {
                eprintln!("Error al deserializar el mensaje: {}", e);
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

    pub async fn send_block(&mut self, block_id: String) {
        // Verificamos si los bloques están disponibles
        if let Some(blocks) = &self.blocks {
            // Buscamos el bloque con el ID proporcionado
            if let Some(block_info) = blocks.iter().find(|b| b.block_id == block_id) {
                let mut block = BlockData {
                    block_id: block_info.block_id.clone(),
                    path: block_info.path.clone(),
                };

                // Intentamos cargar los datos del bloque de manera asincrónica
                match block.load_block().await {
                    Ok(data) => {
                        let send_block_message = DataMessage::StoreBlock {
                            block_id: block_info.block_id.clone(),
                            data,
                        };

                        // Intentamos conectar de manera asincrónica al NameNode
                        if let Err(e) = self.send_to_namenode(send_block_message).await {
                            eprintln!("Error al enviar el bloque: {}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error al cargar el bloque: {}", e);
                    }
                }
            } else {
                println!("Bloque con ID {} no encontrado.", block_id);
            }
        } else {
            println!("No hay bloques almacenados.");
        }
    }

    // Función auxiliar para enviar el mensaje al NameNode
    async fn send_to_namenode(&self, send_block_message: DataMessage) -> Result<(), String> {
        match TcpStream::connect(&self.server_address).await {
            Ok(mut stream) => {
                let serialized = serde_json::to_string(&send_block_message)
                    .map_err(|e| format!("Error al serializar el mensaje del bloque: {}", e))?;

                stream
                    .write_all(serialized.as_bytes())
                    .await
                    .map_err(|e| format!("Error al enviar el bloque: {}", e))?;

                let mut buffer = [0; 128];
                let size = stream
                    .read(&mut buffer)
                    .await
                    .map_err(|e| format!("Error al leer la respuesta del NameNode: {}", e))?;

                let response = String::from_utf8_lossy(&buffer[..size]);

                if response.starts_with("received_block") {
                    println!("Bloque enviado correctamente.");
                    Ok(())
                } else {
                    Err(format!("Error al recibir respuesta: {}", response))
                }
            }
            Err(e) => Err(format!("Error al conectarse al NameNode: {}", e)),
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
                let (stream, _) = listener.accept().await.unwrap();
                // Al llegar una nueva conexión, la manejamos
                let mut datanode_guard = datanode_clone_connections.lock().await;
                datanode_guard.handle_connection(stream).await;
            } else {
                eprintln!("No se pudo obtener el listener");
            }

            sleep(Duration::from_secs(1)).await; // Espera antes de aceptar la siguiente conexión
        }
    });
    // Deja que ambas tareas corran indefinidamente

    // Usa `tokio::join!` para esperar a que ambas tareas se ejecuten en paralelo.
    let _ = tokio::join!(heartbeat_handler, connection_handle); // Dormir para que las tareas continúen ejecutándose
}
