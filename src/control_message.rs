use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForwardRegistration {
    pub proxy_port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ControlMessage {
    #[serde(rename = "hello")]
    Hello {
        token: String,
        #[serde(default)]
        forwards: Vec<ForwardRegistration>,
        #[serde(default)]
        proxy_port: Option<u16>,
    },

    #[serde(rename = "hello_ack")]
    HelloAck { session_id: u64, work_token: String },

    #[serde(rename = "ping")]
    Ping { session_id: u64 },

    #[serde(rename = "pong")]
    Pong { session_id: u64 },

    #[serde(rename = "new_conn")]
    NewConn { id: u64, proxy_port: u16 },

    #[serde(rename = "work")]
    Work {
        id: u64,
        session_id: u64,
        work_token: String,
    },
}
