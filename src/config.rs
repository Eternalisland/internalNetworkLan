use serde::Deserialize;

#[derive(Deserialize)]
pub struct ServerConfig {
    pub control_addr: String,
    pub proxy_addr: String,
    pub token: String,
}

#[derive(Deserialize)]
pub struct ClientConfig {
    pub server_addr: String,
    pub local_addr: String,
    pub token: String,
}

/// 从命令行参数 `-f <file>` 中提取配置文件路径。
/// 如果没有 `-f` 参数，返回 None。
pub fn load_config_path() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == "-f")
        .and_then(|i| args.get(i + 1).cloned())
}
