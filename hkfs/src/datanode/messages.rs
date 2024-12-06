use ::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum DataNodeMessage {
    Heartbeat { node_id: u32 },
    Register { key: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DataMessage {
    StoreBlock { block_id: String, data: Vec<u8> },
}
