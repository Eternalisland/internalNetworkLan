use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ControlMessage {
    
    #[serde(rename = "hello")]
    Hello {
        token: String
    },

    #[serde(rename = "new_conn")]
    NewConn {
        id : u64
    },
    #[serde(rename = "work")]
     Work {
        id: u64,
    },
}