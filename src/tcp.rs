// 更新内容：统一配置隧道 TCP 连接，开启 nodelay 和 keepalive，减少弱网/半开连接导致的转发不稳定。
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;

const TCP_KEEPALIVE_TIME: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

pub fn configure_tunnel_stream(stream: &TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)?;

    let sock = SockRef::from(stream);
    let keepalive = TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_TIME)
        .with_interval(TCP_KEEPALIVE_INTERVAL);
    sock.set_tcp_keepalive(&keepalive)?;

    Ok(())
}
