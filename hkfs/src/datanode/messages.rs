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
