// 更新内容：扩展服务端/客户端稳定性配置，支持协议选择、超时、并发限制和预热 work 连接池参数。
use anyhow::{Result, anyhow};
use serde::Deserialize;

use crate::control_message::TransportProtocol;

#[derive(Deserialize, Debug)]
pub struct ServerConfig {
    pub control_addr: String,
    #[serde(default)]
    pub proxy_ports: Vec<u16>,
    #[serde(default)]
    pub proxy_forwards: Vec<ServerForwardConfig>,
    pub token: String,
    #[serde(default)]
    pub control_channel_capacity: Option<usize>,
    #[serde(default)]
    pub control_read_timeout_secs: Option<u64>,
    #[serde(default)]
    pub control_send_timeout_secs: Option<u64>,
    #[serde(default)]
    pub work_connect_timeout_secs: Option<u64>,
    #[serde(default)]
    pub pending_work_ttl_secs: Option<u64>,
    #[serde(default)]
    pub pending_sweep_interval_secs: Option<u64>,
    #[serde(default)]
    pub max_connections_per_port: Option<usize>,
    #[serde(default)]
    pub ready_work_pool_max_per_port: Option<usize>,
    #[serde(default)]
    pub ready_work_ttl_secs: Option<u64>,
}

#[derive(Clone, Deserialize, Debug)]
pub struct ServerForwardConfig {
    pub proxy_port: u16,
    #[serde(default)]
    pub protocol: TransportProtocol,
}

#[derive(Deserialize, Debug)]
pub struct ClientConfig {
    pub server_addr: String,
    pub token: String,
    #[serde(default)]
    pub forwards: Vec<ClientForwardConfig>,
    #[serde(default)]
    pub local_addr: Option<String>,
    #[serde(default)]
    pub proxy_port: Option<u16>,
    #[serde(default)]
    pub control_heartbeat_interval_secs: Option<u64>,
    #[serde(default)]
    pub control_connect_timeout_secs: Option<u64>,
    #[serde(default)]
    pub control_read_timeout_secs: Option<u64>,
    #[serde(default)]
    pub control_write_timeout_secs: Option<u64>,
    #[serde(default)]
    pub server_work_connect_timeout_secs: Option<u64>,
    #[serde(default)]
    pub max_concurrent_work_connections: Option<usize>,
    #[serde(default)]
    pub local_connect_attempts: Option<usize>,
    #[serde(default)]
    pub local_connect_timeout_ms: Option<u64>,
    #[serde(default)]
    pub local_retry_delay_min_ms: Option<u64>,
    #[serde(default)]
    pub local_retry_delay_max_ms: Option<u64>,
    #[serde(default)]
    pub ready_work_pool_size_per_port: Option<usize>,
    #[serde(default)]
    pub ready_work_reconnect_delay_ms: Option<u64>,
}

#[derive(Clone, Deserialize, Debug)]
pub struct ClientForwardConfig {
    pub proxy_port: u16,
    pub local_addr: String,
    #[serde(default)]
    pub protocol: TransportProtocol,
}

impl ServerConfig {
    pub fn normalized_forwards(&self) -> Result<Vec<ServerForwardConfig>> {
        if !self.proxy_forwards.is_empty() {
            return Ok(self.proxy_forwards.clone());
        }

        if !self.proxy_ports.is_empty() {
            return Ok(self
                .proxy_ports
                .iter()
                .map(|proxy_port| ServerForwardConfig {
                    proxy_port: *proxy_port,
                    protocol: TransportProtocol::Tcp,
                })
                .collect());
        }

        Err(anyhow!("server config requires proxy_forwards or proxy_ports"))
    }
}

impl ClientConfig {
    pub fn normalized_forwards(&self) -> Result<Vec<ClientForwardConfig>> {
        if !self.forwards.is_empty() {
            return Ok(self.forwards.clone());
        }

        match (self.proxy_port, self.local_addr.as_ref()) {
            (Some(proxy_port), Some(local_addr)) => Ok(vec![ClientForwardConfig {
                proxy_port,
                local_addr: local_addr.clone(),
                protocol: TransportProtocol::Tcp,
            }]),
            _ => Err(anyhow!(
                "client config requires forwards, or both proxy_port and local_addr"
            )),
        }
    }
}

pub fn load_config_path() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == "-f")
        .and_then(|i| args.get(i + 1).cloned())
}
