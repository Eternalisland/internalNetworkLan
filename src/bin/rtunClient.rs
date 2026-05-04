use anyhow::Result;
use internalNetworkLan::config::{self, ClientConfig};
use internalNetworkLan::{ControlMessage, read_json_line, write_json_line};
use tokio::io::BufReader;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let config = load_client_config();

    run_client(&config.server_addr, &config.local_addr, config.token).await;
}

fn load_client_config() -> ClientConfig {
    config::load_config_path()
        .and_then(|path| {
            std::fs::read_to_string(&path)
                .ok()
                .and_then(|s| serde_json::from_str::<ClientConfig>(&s).ok())
                .map(|c| {
                    println!("loaded client config from {}", path);
                    c
                })
        })
        .unwrap_or_else(|| {
            println!("no config file, using defaults");
            ClientConfig {
                server_addr: "localhost:7000".into(),
                local_addr: "localhost:63306".into(),
                token: "dev-token".into(),
            }
        })
}

async fn run_client(server_addr: &str, local_addr: &str, token: String) {
    let mut stream = match TcpStream::connect(server_addr).await {
        Ok(stream) => {
            println!("connected to server control at {}", server_addr);
            stream
        }
        Err(e) => {
            dbg!("connect error: {:?}", e);
            // eprintln!("connect error: {:?}", e);
            return;
        }
    };

    let mut reader = BufReader::new(&mut stream);

    if let Err(e) = write_json_line(
        &mut reader,
        &ControlMessage::Hello {
            token: token.clone(),
        },
    )
    .await
    {
        eprintln!("write_json_line error: {:?}", e);
        return;
    }

    println!("sent Hello, waiting for commands...");

    loop {
        match read_json_line::<_, ControlMessage>(&mut reader).await {
            Ok(ControlMessage::NewConn { id }) => {
                println!("new connection request from server, id = {}", id);

                let server_addr = server_addr.to_string();
                let local_addr = local_addr.to_string();

                tokio::spawn(async move {
                    match handle_work_connection(server_addr, local_addr, id).await {
                        Ok(()) => {
                            println!("work connection completed, id={}", id);
                        }
                        Err(e) => {
                            eprintln!("work connection failed, id={}, error={:?}", id, e);
                        }
                    }
                });
            }
            Ok(other) => {
                println!("unexpected control msg: {:?}", other);
            }
            Err(error) => {
                eprintln!("control connection error: {}, exiting", error);
                break;
            }
        }
    }
}

async fn handle_work_connection(
    server_addr: String,
    local_addr: String,
    conn_id: u64,
) -> Result<()> {
    println!("opening work connection to server, id={}", conn_id);
    let mut server_stream = TcpStream::connect(&server_addr).await?;
    println!("work connection established to server, id={}", conn_id);

    let msg = ControlMessage::Work { id: conn_id };
    if let Err(e) = write_json_line::<TcpStream, ControlMessage>(&mut server_stream, &msg).await {
        eprintln!("write_json_line error, id={}, error={:?}", conn_id, e);
        return Err(e.into());
    }

    println!("connecting to local service at {}, id={}", local_addr, conn_id);
    let mut local_stream = TcpStream::connect(&local_addr).await?;
    println!("connected to local service, id={}", conn_id);

    println!("forwarding started, id={}", conn_id);
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
