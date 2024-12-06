mod messages;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use messages::{DataMessage, DataNodeMessage};

const NAMENODE_PORT: &str = "127.0.0.1:7878"; // Puerto del NameNode
const DEFAULT_KEY: &str = "SBXBUSKANKLAKA";
const API_PORT: usize = 8080;

#[derive(Debug, Clone)]
struct DataNode {
    id: Option<u32>, // El ID se asignará después del registro
    server_address: String,
    key: String,
    port: Option<usize>,
}

impl DataNode {
    pub fn new(server_address: String, key: String) -> Self {
        Self {
            id: None, // Sin ID al inicio
            server_address,
            key,
            port: None,
        }
    }

    // Método para enviar el "registro" y recibir el ID
    pub fn register(&mut self) -> Result<(), String> {
        let register_message = DataNodeMessage::Register {
            key: self.key.clone(),
        };

        match TcpStream::connect(&self.server_address) {
            Ok(mut stream) => {
                let serialized = serde_json::to_string(&register_message).unwrap();
                stream.write_all(serialized.as_bytes()).unwrap();

                let mut buffer = [0; 128];
                let size = stream.read(&mut buffer).unwrap();
                let response = String::from_utf8_lossy(&buffer[..size]);

                if response.starts_with("register_accepted:") {
                    let id: u32 = response
                        .strip_prefix("register_accepted:")
                        .unwrap()
                        .parse()
                        .unwrap();
                    let port = API_PORT + id as usize;
                    println!("Registro exitoso, ID asignado: {}", id);
                    self.id = Some(id);
                    self.port = Some(port);
                    Ok(())
                } else {
                    Err(response.to_string())
                }
            }
            Err(e) => Err(format!("Error al conectarse al NameNode: {}", e)),
        }
    }

    // Método para escuchar el puerto asignado para recibir StoreBlock
    pub fn listen(&self) {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port.unwrap())).unwrap();
        println!(
            "Escuchando en el puerto {} para recibir bloques...",
            self.port.unwrap()
        );

        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    let mut buffer = Vec::new();
                    stream.read_to_end(&mut buffer).unwrap(); // Lee todo el mensaje

                    // Deserializa el mensaje recibido
                    let received_message: Result<DataMessage, _> = serde_json::from_slice(&buffer);

                    match received_message {
                        Ok(DataMessage::StoreBlock { block_id, data }) => {
                            // Si es un comando StoreBlock, lo procesamos
                            println!(
                                "Bloque recibido: ID: {}, Tamaño de los datos: {} bytes",
                                block_id,
                                data.len()
                            );
                            self.store_block(block_id, &data);
                        }
                        _ => {
                            println!("Mensaje no reconocido o error al deserializar");
                        }
                    }
                }
                Err(e) => eprintln!("Error en la conexión: {}", e),
            }
        }
    }

    // Método para simular el almacenamiento de un bloque
    pub fn store_block(&self, block_id: String, data: &[u8]) {
        println!("Almacenando bloque con ID {}. Datos: {:?}", block_id, data);
        println!("Bloque almacenado correctamente.");
    }

    pub fn send_heartbeat(&mut self) {
        if let Some(_node_id) = self.id {
            let heartbeat_message = DataNodeMessage::Heartbeat {
                node_id: self
                    .id
                    .expect("No se puede enviar el heartbeat si el datanode no esta registrado"),
            };
            match TcpStream::connect(&self.server_address) {
                Ok(mut stream) => {
                    let serialized = serde_json::to_string(&heartbeat_message).unwrap();
                    stream.write_all(serialized.as_bytes()).unwrap();

                    let mut buffer = [0; 128];
                    let size = stream.read(&mut buffer).unwrap();
                    let response = String::from_utf8_lossy(&buffer[..size]);

                    if response.starts_with("received_heartbeat") {
                        println!("Se envió el heartbeat correctamente. Sigo activo.");
                    } else {
                        eprintln!("Error: {}", response);
                    }
                }
                Err(e) => eprintln!("Error al conectarse al NameNode: {}", e),
            }
        } else {
            println!("Id no asignado. No se puede enviar heartbeat");
        }
    }
}

fn main() {
    let mut datanode = DataNode::new(NAMENODE_PORT.to_string(), DEFAULT_KEY.to_string());

    // Registrar el DataNode
    let _ = datanode.register();
    let datanode_clone = datanode.clone();
    // Crear un hilo para enviar heartbeats
    let heartbeat_handle = thread::spawn(move || {
        loop {
            println!("Intentando enviar heartbeat");
            datanode.send_heartbeat(); // Enviar heartbeats después de registrar
            thread::sleep(Duration::from_secs(1));
        }
    });

    // Crear un hilo para escuchar conexiones en el puerto
    let listen_handle = thread::spawn(move || {
        datanode_clone.listen(); // Escuchar el puerto asignado para recibir bloques
    });

    // Esperar a que ambos hilos terminen (en este caso, deberían ejecutarse indefinidamente)
    let _ = heartbeat_handle.join();
    let _ = listen_handle.join();
}
