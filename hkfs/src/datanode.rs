mod messages;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use messages::DataNodeMessage;

const NAMENODE_PORT: &str = "127.0.0.1:7878"; // Puerto del NameNode
const DEFAULT_KEY: &str = "SBXBUSKANKLAKA";

struct DataNode {
    id: Option<u32>, // El ID se asignará después del registro
    server_address: String,
    key: String
}

impl DataNode {
    pub fn new(server_address: String, key: String) -> Self {
        Self {
            id: None, // Sin ID al inicio
            server_address,
            key,
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
                    let id: u32 = response[18..].parse().unwrap();
                    println!("Registro exitoso, ID asignado: {}", id);
                    self.id = Some(id);
                    Ok(())
                } else {
                    Err(response.to_string())
                }
            }
            Err(e) => Err(format!("Error al conectarse al NameNode: {}", e)),
        }
    }

    pub fn send_heartbeat(&mut self) {
        if let Some(_node_id) = self.id {
            
            let heartbeat_message = DataNodeMessage::Heartbeat{
                node_id: self.id.expect("No se puede enviar el heartbeat si el datanode no esta registrado"),
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
                     eprintln!("Error: {}", response.to_string());
                 }
                }
                Err(e) => eprintln!("Error al conectarse al NameNode: {}", e),
            }

        }else{
            println!("Id no asignado. No se puede enviar heartbeat");
        }
    }
    
}


fn main() {
    let mut datanode = DataNode::new(NAMENODE_PORT.to_string(), DEFAULT_KEY.to_string());
    let _  = datanode.register(); // Registrar el DataNode y obtener el ID
    loop  {
        println!("Intentando enviar heartbeat");
        datanode.send_heartbeat(); // Enviar heartbeats después de registrar
        thread::sleep(Duration::from_secs(1));
    }
 
}
