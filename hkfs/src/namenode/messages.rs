use ::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum DataNodeMessage {
    Heartbeat { node_id: u32 },
    Register { key: String },
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub status: Vec<String>,
}
