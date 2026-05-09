use anyhow::{Result, anyhow};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct ServerConfig {
    pub control_addr: String,
    #[serde(default)]
    pub proxy_ports: Vec<u16>,
    pub token: String,
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
}

#[derive(Clone, Deserialize, Debug)]
pub struct ClientForwardConfig {
    pub proxy_port: u16,
    pub local_addr: String,
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
