use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

const NAMENODE_PORT: &str = "127.0.0.1:7878"; // Puerto del NameNode

struct DataNode {
    id: Option<u32>, // El ID se asignará después del registro
    server_address: String,
}

impl DataNode {
    pub fn new(server_address: String) -> Self {
        Self {
            id: None, // Sin ID al inicio
            server_address,
        }
    }

    // Método para enviar el "registro" y recibir el ID
    pub fn register(&mut self) {
        match TcpStream::connect(&self.server_address) {
            Ok(mut stream) => {
                let message = "register:\n"; // Enviar mensaje de registro
                if let Err(e) = stream.write_all(message.as_bytes()) {
                    eprintln!("Error al enviar mensaje de registro: {}", e);
                } else {
                    let mut buffer = [0; 128];
                    if let Ok(size) = stream.read(&mut buffer) {
                        let response = String::from_utf8_lossy(&buffer[..size]);

                        // Obtener el ID del DataNode
                        if let Some(start) = response.find("ack:") {
                            let id_str = &response[start + 4..];
                            if let Ok(id) = id_str.trim().parse::<u32>() {
                                self.id = Some(id); // Asignar ID recibido
                                println!("DataNode registrado con ID: {}", self.id.unwrap());
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("Error al conectarse al NameNode: {}", e),
        }
    }

    // Método para enviar el heartbeat con el ID ya asignado
    pub fn send_heartbeat(&self) {
        if let Some(id) = self.id {
            loop {
                match TcpStream::connect(&self.server_address) {
                    Ok(mut stream) => {
                        let message = format!("heartbeat:{}\n", id); // Enviar heartbeat con ID
                        if let Err(e) = stream.write_all(message.as_bytes()) {
                            eprintln!("Error al enviar heartbeat: {}", e);
                        } else {
                            let mut buffer = [0; 128];
                            if let Ok(size) = stream.read(&mut buffer) {
                                let response = String::from_utf8_lossy(&buffer[..size]);
                                println!("Node {} received: {}", id, response);
                            }
                        }
                    }
                    Err(e) => eprintln!("Error al conectarse al NameNode: {}", e),
                }

                thread::sleep(Duration::from_secs(5)); // Enviar cada 5 segundos
            }
        }
    }
}

fn main() {
    let mut datanode = DataNode::new(NAMENODE_PORT.to_string());
    datanode.register(); // Registrar el DataNode y obtener el ID
    datanode.send_heartbeat(); // Enviar heartbeats después de registrar
}
