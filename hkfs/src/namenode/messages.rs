use ::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum DataNodeMessage {
    Heartbeat { node_id: u32 },
    Register { key: String },
    BlockData { data: Vec<u8> },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DataMessage {
    StoreBlock { block_id: u32, data: Vec<u8> },
    ReadBlock { block_id: u32 },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "data")]
pub enum PostRequest {
    StoreBlock { block_id: String, data: String },
}

#[derive(Deserialize, Debug)]
pub struct GetRequest {
    pub r#type: String, // `type` es una palabra reservada en Rust, por eso se usa `r#type`
    pub block_id: Option<String>, // Opcional porque no todas las variantes necesitan `block_id`
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Ok { message: String },
    Error { message: String },
    BlockData { data: String },
    Status { status: Vec<String> },
}
