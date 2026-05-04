use anyhow::Result;
use internalNetworkLan::{ControlMessage, read_json_line, write_json_line};
use tokio::io::{ BufReader};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let server_addr = "localhost:10000";

    let local_addr = "localhost:63306";

    let token = "dev-token".to_string();

    run_client(server_addr, local_addr, token).await;
}

async fn run_client(server_addr: &str, local_addr: &str, token: String) {
    // 连接服务端
    match TcpStream::connect(server_addr).await {
        Ok(mut stream) => {
            // 只是获取服务端的连接
            let mut reader = BufReader::new(&mut stream);

            let result = write_json_line(
                &mut reader,
                &ControlMessage::Hello {
                    token: token.clone(),
                },
            )
            .await;

            match result {
                Ok(_) => {
                    match read_json_line(&mut reader).await {
                        Ok(msg) => {
                            // 成功读取到消息时的操作
                            match msg {
                                ControlMessage::NewConn { id } => {
                                    println!("new connection request from server, id = {}", id);

                                    let server_addr = server_addr.to_string();
                                    let local_addr = local_addr.to_string();

                                    tokio::spawn(async move {

                                        match handle_work_connection(server_addr, local_addr, id).await {
                                            Ok(()) => {

                                            }
                                            Err(e) => {

                                            }
                                        };
                                        
                                    });
                                }
                                other => {
                                    println!("unexpected control msg: {:?}", other);
                                }
                            }
                        }
                        Err(error) => {
                            // 处理读取消息时可能出现的错误
                            eprintln!("Failed to read JSON line: {}", error);
                        }
                    };
                }
                Err(error) => {
                    eprintln!("write_json_line error");
                }
            }
        }
        Err(e) => {
            println!("connect error: {:?}", e);
        }
    };
}

async fn handle_work_connection(
    server_addr: String,
    local_addr: String,
    conn_id: u64,
) -> Result<()> {
    let mut server_stream = TcpStream::connect(server_addr).await? ;
    println!("connected work connection to server, id = {}", conn_id);

    let msg = ControlMessage::Work { id: conn_id };
     if let Err(e) = write_json_line::<TcpStream,ControlMessage>(&mut server_stream, &msg).await {
        eprintln!("write_json_line error, id={}, error={:?}", conn_id, e);
        return Err(e.into());
     };

    let mut local_stream = TcpStream::connect(local_addr).await?;

    let result =
        tokio::io::copy_bidirectional(&mut server_stream, &mut local_stream).await;

    match result {
        Ok((server_to_local, local_to_server)) => {
            println!(
                "work forward finished, id={}, server->local={} bytes, local->server={} bytes",
                conn_id, server_to_local, local_to_server
            );
        }
        Err(e) => {
            eprintln!("work forward error, id={}, error={:?}", conn_id, e);
        }
    }

    Ok(())

}
