use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::io;
use internalNetworkLan::control_message::ControlMessage;
pub use internalNetworkLan::frame::{read_json_line, write_json_line};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};

#[tokio::main]
async fn main() {

    let control_addr = "0.0.0.0:7000";
    let proxy_addr = "0.0.0.0:10000";
    let token = "dev-token".to_string();

    run_server(control_addr, proxy_addr, token).await.expect("TODO: panic message");
}

async fn run_server(control_addr: &str, proxy_addr: &str, token: String) -> Result<()> {
    // 控制服务
    let control_listener = TcpListener::bind(control_addr).await?;
    // 代理服务
    let proxy_listener = TcpListener::bind(proxy_addr).await?;

    println!("server control listening on {}", control_addr);
    println!("server proxy listening on {}", proxy_addr);

    let conn_id_gen = Arc::new(AtomicU64::new(1));

    let pending_map = Arc::new(Mutex::new(HashMap::<u64, oneshot::Sender<TcpStream>>::new()));

    let control_sender: Arc<Mutex<Option<mpsc::Sender<ControlMessage>>>> =
        Arc::new(Mutex::new(None));

    {
        let pending_map = Arc::clone(&pending_map);
        let control_sender = Arc::clone(&control_sender);
        let token = token.clone();
        let conn_id_gen = Arc::clone(&conn_id_gen);

        tokio::spawn(async move {
            loop {
                match control_listener.accept().await {
                    Ok((socket, addr)) => {
                        let pending_map = pending_map.clone();
                        let conn_id =
                            conn_id_gen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let control_sender = control_sender.clone();
                        let token = token.clone();
                        println!("got a connection from {}", addr);
                        
                        tokio::spawn( async move {
                            if let Err(e) =  handle_client_connection(
                                socket,
                                pending_map,
                                control_sender,
                                token
                            ).await {
                                eprintln!("error handling client connection: {:?}", e);
                            };
                        });
                    }
                    Err(e) => {
                        println!("error accepting connection: {}", e);
                    }
                }
            }
        });
    }

    loop {
        let (user_stream, user_addr) = proxy_listener.accept().await?;
        println!("user connected: {:?}", user_addr);
        let padding_map = pending_map.clone();
        let conn_id = conn_id_gen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let control_sender = Arc::clone(&control_sender);
        tokio::spawn(async move {
            if let Err(e) =
                handle_user_connection(user_stream, conn_id, control_sender, padding_map).await
            {
                println!("error handling user connection: {}", e);
            }
        });
    }
    Ok(())
}
/**
 *
 */
pub async fn handle_client_connection(
    stream_tcp: TcpStream,
    pending_map: Arc<Mutex<HashMap<u64, oneshot::Sender<TcpStream>>>>,
    control_sender: Arc<Mutex<Option<mpsc::Sender<ControlMessage>>>>,
    expected_token: String,
) -> Result<()> {
    let mut reader = BufReader::new(stream_tcp);
    let data: Result<ControlMessage, _> = read_json_line(&mut reader).await;

    match data {
        Ok(data) => match data {
            ControlMessage::Hello { token } => {
                if token != expected_token {
                    return Err(anyhow!("invalidate token!"));
                    // Err::new( "invalidate token!".into())
                }
                let stream = reader.into_inner();
                println!("client connected from {}", stream.peer_addr()?);

                handle_control_connection(stream, control_sender)
                    .await
                    .expect("TODO: panic message");
            }
            ControlMessage::Work { id } => {
                println!("work connection arrived, id = {}", id);

                let stream = reader.into_inner();
                let sender = {
                    let mut mutex_guard = pending_map.lock().await;
                    mutex_guard.remove(&id)
                };

                match sender {
                    Some(sender) => {
                        let _ = sender.send(stream);
                    }
                    None => {
                        return Err(anyhow!("work connection arrived, id = {}", id));
                    }
                }
            }

            ControlMessage::NewConn { id } => {}
            _ => {}
        },
        Err(e) => {
            println!("error reading json line: {}", e);
        }
    }

    Ok(())
}

/**
 *
 */
pub async fn handle_control_connection(
    stream: TcpStream,
    control_sender: Arc<Mutex<Option<mpsc::Sender<ControlMessage>>>>,
) -> Result<()> {
    let (read, mut write) = stream.into_split();

    let (tx, mut rx) = mpsc::channel::<ControlMessage>(100);

    {
        let mut guard = control_sender.lock().await;
        *guard = Some(tx);
    }

    println!("control connection registered");

    let w = tokio::spawn(async move {
        // 处理任务队列的消息
        while let Some(msg) = rx.recv().await {
            if let Err(e) = write_json_line(&mut write, &msg).await {
                println!("error writing json line: {}", e);
            }
        }
    });

    let r = tokio::spawn(async move {
        let mut reader = BufReader::new(read);
        loop {
            match read_json_line::<_, ControlMessage>(&mut reader).await {
                Ok(msg) => {
                    println!("received a control message: {:?}", msg);
                }
                Err(e) => {
                    println!("error reading json line: {}", e);
                }
            }
        }
    });

    let _ = tokio::join!(w, r);

    println!("control connection closed");

    {
        let mut guard = control_sender.lock().await;
        *guard = None;
    }
    Ok(())
}

pub async fn handle_user_connection(
    mut user_stream: TcpStream,
    conn_id: u64,
    control_sender: Arc<Mutex<Option<mpsc::Sender<ControlMessage>>>>,
    pending_map: Arc<Mutex<HashMap<u64, oneshot::Sender<TcpStream>>>>,
) -> Result<()> {
    // 读取 tcp 连接流
    let tx = {
        let mutex_guard = control_sender.lock().await;
        mutex_guard
            .clone()
            .ok_or_else(|| anyhow!("no client control connection"))?
    };

    let (work_tx, work_rx) = oneshot::channel::<TcpStream>();

    // 将连接信息插入到 map 里
    {
        let mut map = pending_map.lock().await;
        map.insert(conn_id, work_tx);
    }

    // 往队列发送一条连接信息
    tx.send(ControlMessage::NewConn { id: conn_id }).await?;

    println!("sent NewConn to client, id = {}", conn_id);

    let mut work_stream =
        match tokio::time::timeout(std::time::Duration::from_secs(10), work_rx).await {
            Ok(Ok(work_stream)) => work_stream,
            Ok(Err(_)) => return Err(anyhow!("work timed out")),
            Err(_) => {
                let mut map = pending_map.lock().await;
                map.remove(&conn_id);
                return Err(anyhow!("work timed out"));
            }
        };

    let result = io::copy_bidirectional(&mut user_stream, &mut work_stream).await;

    match result {
        Ok( (user_to_client,client_to_user) ) => {
            println!(
                "forward finished, id={}, user->client={} bytes, client->user={} bytes",
                conn_id, user_to_client, client_to_user
            );
        },
        Err(e) => {
            println!("error copying bidirectional stream: {}", e);
        }
    }
    Ok(())
}
