// 更新内容：增加 ready_work 和传输协议字段，支持 TCP/UDP/BOTH 端口转发并兼容旧 TCP 配置。
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportProtocol {
    Tcp,
    Udp,
    Both,
}

impl Default for TransportProtocol {
    fn default() -> Self {
        Self::Tcp
    }
}

impl TransportProtocol {
    pub fn expands_to(self) -> Vec<Self> {
        match self {
            Self::Tcp => vec![Self::Tcp],
            Self::Udp => vec![Self::Udp],
            Self::Both => vec![Self::Tcp, Self::Udp],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForwardRegistration {
    pub proxy_port: u16,
    #[serde(default)]
    pub protocol: TransportProtocol,
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
    NewConn {
        id: u64,
        proxy_port: u16,
        #[serde(default)]
        protocol: TransportProtocol,
    },

    #[serde(rename = "work")]
    Work {
        id: u64,
        session_id: u64,
        work_token: String,
    },

    #[serde(rename = "ready_work")]
    ReadyWork {
        session_id: u64,
        work_token: String,
        proxy_port: u16,
    },
}
