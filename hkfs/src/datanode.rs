use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

struct DataNode {
    id: u32,
    server_address: String,
}

impl DataNode {
    pub fn new(id: u32, server_address: String) -> Self {
        Self { id, server_address }
    }

    pub fn send_heartbeat(&self) {
        loop {
            match TcpStream::connect(&self.server_address) {
                Ok(mut stream) => {
                    let message = format!("heartbeat:{}\n", self.id);
                    if let Err(e) = stream.write_all(message.as_bytes()) {
                        eprintln!("Error al enviar heartbeat: {}", e);
                    } else {
                        let mut buffer = [0; 128];
                        if let Ok(size) = stream.read(&mut buffer) {
                            let response = String::from_utf8_lossy(&buffer[..size]);
                            println!("Node {} received: {}", self.id, response);
                        }
                    }
                }
                Err(e) => eprintln!("Error al conectarse al NameNode: {}", e),
            }

            thread::sleep(Duration::from_secs(5)); // Enviar cada 5 segundos
        }
    }
}

fn main() {
    let datanode = DataNode::new(1, "127.0.0.1:7878".to_string());
    datanode.send_heartbeat();
}
