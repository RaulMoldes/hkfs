use::serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum  DataNodeMessage {
    Heartbeat { node_id: u32 }, 
    Register {key: String },
}