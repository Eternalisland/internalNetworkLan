use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::io;
use internalNetworkLan::config::{self, ServerConfig};
use internalNetworkLan::control_message::ControlMessage;
pub use internalNetworkLan::frame::{read_json_line, write_json_line};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};

#[tokio::main]
async fn main() {

    let config = load_server_config();

    run_server(&config.control_addr, &config.proxy_addr, config.token)
        .await
        .expect("TODO: panic message");
}

fn load_server_config() -> ServerConfig {
    config::load_config_path()
        .and_then(|path| {
            std::fs::read_to_string(&path)
                .ok()
                .and_then(|s| serde_json::from_str::<ServerConfig>(&s).ok())
                .map(|c| {
                    println!("loaded server config from {}", path);
                    c
                })
        })
        .unwrap_or_else(|| {
            println!("no config file, using defaults");
            ServerConfig {
                control_addr: "0.0.0.0:7000".into(),
                proxy_addr: "0.0.0.0:7001".into(),
                token: "dev-token".into(),
            }
        })
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

        tokio::spawn(async move {
            loop {
                match control_listener.accept().await {
                    Ok((socket, addr)) => {
                        let pending_map = pending_map.clone();
                        // let conn_id =
                        //     conn_id_gen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let control_sender = control_sender.clone();
                        let token = token.clone();
                        println!("get a connection from {}", addr);
                        
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

    println!("proxy loop: wating for accept...");
    loop {

        match proxy_listener.accept().await {
            Ok((user_stream, user_addr)) => {
                println!("proxy: accept from {}", user_addr);
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
            },
            Err(e) => {
                println!("proxy: accept error {:?}", proxy_listener);
            }
        }
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
                    println!("control read error: {}, closing", e);
                    break;
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

/// 处理外部用户连接到代理端口的请求。
///
/// 这是整个反向隧道系统的核心转发逻辑。当外部用户连接到服务器的代理端口时，
/// 服务器需要通知内部网络的客户端建立一条"工作连接"(work connection)，
/// 然后把用户流量和客户端流量双向桥接起来。
///
/// ## 参数
/// - `user_stream`: 外部用户的 TCP 连接
/// - `conn_id`: 本次连接的唯一 ID
/// - `control_sender`: 到客户端控制连接的 mpsc 发送端（可能为 None，表示客户端不在线）
/// - `pending_map`: 等待中的连接映射表，key=连接ID，value=oneshot发送端
///
/// ## 流程
/// 1. 取出控制连接的 mpsc sender（客户端不在线则报错）
/// 2. 创建一个 oneshot 通道，把发送端插入 pending_map
/// 3. 通过 mpsc 发送 NewConn 消息给客户端
/// 4. 等待客户端通过 oneshot 返回工作连接（超时 10 秒）
/// 5. 双向桥接用户流量和工作连接流量
pub async fn handle_user_connection(
    mut user_stream: TcpStream,
    conn_id: u64,
    control_sender: Arc<Mutex<Option<mpsc::Sender<ControlMessage>>>>,
    pending_map: Arc<Mutex<HashMap<u64, oneshot::Sender<TcpStream>>>>,
) -> Result<()> {
    // 第一步：从共享状态中取出 mpsc::Sender。
    // control_sender 是 Arc<Mutex<Option<Sender>>>，存在三种状态：
    //   - Some(tx): 客户端已连接，控制通道可用
    //   - None: 客户端未连接或已断开
    // 这里 clone() 出一个 Sender（mpsc 支持多生产者），然后释放锁。
    let tx = {
        let mutex_guard = control_sender.lock().await;
        mutex_guard
            .clone()
            .ok_or_else(|| anyhow!("no client control connection"))?
    }; // 锁在此处释放，避免长时间持有

    // 第二步：创建 oneshot 通道。
    // work_tx 被插入 pending_map，等待客户端的工作连接到来。
    // work_rx 由当前函数持有，用于阻塞等待客户端的工作连接。
    let (work_tx, work_rx) = oneshot::channel::<TcpStream>();

    // 第三步：将 oneshot 发送端注册到 pending_map。
    // 当客户端的工作连接到达服务器时，handle_client_connection 会根据 conn_id
    // 找到对应的 oneshot sender，并把工作连接的 TcpStream 通过 oneshot 发送过来。
    {
        let mut map = pending_map.lock().await;
        map.insert(conn_id, work_tx);
    }

    // 第四步：通过 mpsc 通道通知客户端"有新的用户连接，请建立工作连接"。
    // 这条消息会由 handle_control_connection 中的 w 任务读取，并通过
    // JSON-line 协议发送给客户端。
    tx.send(ControlMessage::NewConn { id: conn_id }).await?;
    println!("sent NewConn to client, id = {}", conn_id);

    // 第五步：等待客户端的工作连接到达，超时时间为 10 秒。
    // oneshot 通道只传一个值，客户端的工作连接到达后 work_rx 就会被触发。
    // 超时说明客户端可能：
    //   - 未能连接到服务器（网络问题）
    //   - 连接到了但发送 Work 消息失败
    //   - 客户端进程已退出
    // 超时时需要清理 pending_map，避免内存泄漏。
    let mut work_stream =
        match tokio::time::timeout(std::time::Duration::from_secs(10), work_rx).await {
            // 超时未触发，work_rx 正常接收到工作连接
            Ok(Ok(work_stream)) => work_stream,
            // oneshot 的发送端被 drop 了（work_tx 被从 map 中移除但没有 send）
            Ok(Err(_)) => return Err(anyhow!("work connection sender dropped, id={}", conn_id)),
            // 真正的超时
            Err(_) => {
                // 清理 pending_map 中对应的条目，防止内存泄漏
                let mut map = pending_map.lock().await;
                map.remove(&conn_id);
                return Err(anyhow!("work connection timeout, id={}", conn_id));
            }
        };

    // 第六步：双向桥接。
    // copy_bidirectional 同时拷贝两个方向的数据：
    //   - 从 user_stream 读取，写入 work_stream（外部用户 → 客户端内网服务）
    //   - 从 work_stream 读取，写入 user_stream（客户端内网服务 → 外部用户）
    // 任一方向关闭或出错时返回。
    //
    // 注意：这一步会阻塞直到连接关闭（长时间运行）。
    let result = io::copy_bidirectional(&mut user_stream, &mut work_stream).await;

    match result {
        Ok((user_to_client, client_to_user)) => {
            println!(
                "forward finished, id={}, user->client={} bytes, client->user={} bytes",
                conn_id, user_to_client, client_to_user
            );
        }
        Err(e) => {
            eprintln!("forward error, id={}, error={:?}", conn_id, e);
        }
    }

    Ok(())
}
