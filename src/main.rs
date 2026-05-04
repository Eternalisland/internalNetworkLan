mod control_message;
mod frame;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
// 引入读写扩展 Trait


#[tokio::main]
async fn main() {
    run().await;
    // if let Err(r) = run() {
    //   println!("error: {}", r);
    // }
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    println!("服务端已启动，监听内网客户端连接，端口: 8080");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("accepted client: {:?}", addr);

        let target_addr = "121.43.231.91:18080";
        // TODO
        // 处理客户端请求
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            // 读到的字节大小
            println!("connected to target: {}", target_addr);
            match connect_with_timeout(target_addr,10).await {
                Ok(mut out_bound) => {
                    println!("connected to target: {}", target_addr);
                    match tokio::io::copy_bidirectional(&mut socket, &mut out_bound).await {
                        Ok((client_to_server, server_to_client)) => {
                            println!(
                                "connection closed, client->server={} bytes, server->client={} bytes",
                                client_to_server, server_to_client
                            );
                        }
                        Err(r) => {
                            println!("error: {:?}", r);
                        }
                    };
                }
                Err(e) => {
                    println!("failed to connect to target: {}", e);
                }
            }
        });
    }
}
use tokio::time::timeout;
pub async fn connect_with_timeout(addr : &str, timeout_secs : u64) -> std::io::Result<TcpStream> {
    timeout(Duration::from_secs(timeout_secs), TcpStream::connect(addr))
        .await
        .map_err( |_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("connect timeout: {}", addr),
            )
        })?
}