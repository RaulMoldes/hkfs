use ::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum DataNodeMessage {
    Heartbeat { node_id: u32 },
    Register { key: String },
    BlockData { data: Vec<u8> },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DataMessage {
    StoreBlock { block_id: String, data: Vec<u8> },
    ReadBlock { block_id: String },
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub status: Vec<String>,
}

#[derive(Deserialize)]
pub struct BlockRequest {
    pub block_id: String,
}
