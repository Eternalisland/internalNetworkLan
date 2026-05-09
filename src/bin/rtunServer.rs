use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use internalNetworkLan::config::{self, ServerConfig};
use internalNetworkLan::control_message::{ControlMessage, ForwardRegistration};
pub use internalNetworkLan::frame::{read_json_line, write_json_line};
use tokio::io;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc, oneshot};

const CONTROL_CHANNEL_CAPACITY: usize = 100;
const WORK_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PENDING_WORK_TTL: Duration = Duration::from_secs(30);
const PENDING_SWEEP_INTERVAL: Duration = Duration::from_secs(15);

type SharedPendingMap = Arc<Mutex<HashMap<u64, PendingWork>>>;
type SharedSessionMap = Arc<Mutex<HashMap<u16, ControlSession>>>;

#[derive(Clone)]
struct ControlSession {
    session_id: u64,
    work_token: String,
    sender: mpsc::Sender<ControlMessage>,
}

struct PendingWork {
    session_id: u64,
    work_token: String,
    created_at: Instant,
    sender: oneshot::Sender<TcpStream>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_server_config()?;
    run_server(&config.control_addr, &config.proxy_ports, &config.token).await
}

fn load_server_config() -> Result<ServerConfig> {
    if let Some(path) = config::load_config_path() {
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read server config from {path}"))?;
        let config = serde_json::from_str::<ServerConfig>(&raw)
            .with_context(|| format!("failed to parse server config from {path}"))?;
        log_info("server", "config_loaded", &[("path", format!("{path:?}"))]);
        Ok(config)
    } else {
        log_info("server", "config_default", &[]);
        Ok(ServerConfig {
            control_addr: "0.0.0.0:7000".into(),
            proxy_ports: vec![7001],
            token: "dev-token".into(),
        })
    }
}

async fn run_server(control_addr: &str, proxy_ports: &[u16], token: &str) -> Result<()> {
    let control_listener = TcpListener::bind(control_addr).await?;
    log_info(
        "server",
        "control_listening",
        &[("addr", format!("{control_addr:?}"))],
    );

    if proxy_ports.is_empty() {
        anyhow::bail!("proxy_ports must not be empty");
    }

    let conn_id_gen = Arc::new(AtomicU64::new(1));
    let session_id_gen = Arc::new(AtomicU64::new(1));
    let pending_map: SharedPendingMap = Arc::new(Mutex::new(HashMap::new()));
    let session_map: SharedSessionMap = Arc::new(Mutex::new(HashMap::new()));
    let allowed_proxy_ports = Arc::new(proxy_ports.iter().copied().collect::<HashSet<_>>());

    {
        let pending_map = Arc::clone(&pending_map);
        tokio::spawn(async move {
            pending_cleanup_loop(pending_map).await;
        });
    }

    // Spawn control listener
    {
        let pending_map = Arc::clone(&pending_map);
        let session_map = Arc::clone(&session_map);
        let token = token.to_string();
        let session_id_gen = Arc::clone(&session_id_gen);
        let allowed_proxy_ports = Arc::clone(&allowed_proxy_ports);

        tokio::spawn(async move {
            loop {
                match control_listener.accept().await {
                    Ok((socket, addr)) => {
                        let pending_map = Arc::clone(&pending_map);
                        let session_map = Arc::clone(&session_map);
                        let token = token.clone();
                        let session_id_gen = Arc::clone(&session_id_gen);
                        let allowed_proxy_ports = Arc::clone(&allowed_proxy_ports);
                        log_info("control", "accepted", &[("peer_addr", format!("{addr:?}"))]);

                        tokio::spawn(async move {
                            if let Err(error) = handle_client_connection(
                                socket,
                                pending_map,
                                session_map,
                                token,
                                session_id_gen,
                                allowed_proxy_ports,
                            )
                            .await
                            {
                                log_error(
                                    "control",
                                    "connection_failed",
                                    &[("error", format!("{error:#?}"))],
                                );
                            }
                        });
                    }
                    Err(error) => {
                        log_error(
                            "control",
                            "accept_failed",
                            &[("error", format!("{error:?}"))],
                        );
                    }
                }
            }
        });
    }

    // Spawn one proxy listener per port
    for &port in proxy_ports {
        let proxy_addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&proxy_addr).await?;
        log_info(
            "proxy",
            "listening",
            &[
                ("proxy_port", port.to_string()),
                ("addr", format!("{proxy_addr:?}")),
            ],
        );

        let pending_map = Arc::clone(&pending_map);
        let session_map = Arc::clone(&session_map);
        let conn_id_gen = Arc::clone(&conn_id_gen);

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((user_stream, user_addr)) => {
                        log_info(
                            "proxy",
                            "accepted",
                            &[
                                ("proxy_port", port.to_string()),
                                ("user_addr", format!("{user_addr:?}")),
                            ],
                        );
                        let pending_map = Arc::clone(&pending_map);
                        let session_map = Arc::clone(&session_map);
                        let conn_id = conn_id_gen.fetch_add(1, Ordering::Relaxed);

                        tokio::spawn(async move {
                            if let Err(error) = handle_user_connection(
                                user_stream,
                                conn_id,
                                port,
                                session_map,
                                pending_map,
                            )
                            .await
                            {
                                log_error(
                                    "proxy",
                                    "connection_failed",
                                    &[
                                        ("proxy_port", port.to_string()),
                                        ("conn_id", conn_id.to_string()),
                                        ("error", format!("{error:#?}")),
                                    ],
                                );
                            }
                        });
                    }
                    Err(error) => {
                        log_error(
                            "proxy",
                            "accept_failed",
                            &[
                                ("proxy_port", port.to_string()),
                                ("error", format!("{error:?}")),
                            ],
                        );
                    }
                }
            }
        });
    }

    // Keep main task alive
    std::future::pending::<()>().await;
    Ok(())
}

async fn handle_client_connection(
    stream_tcp: TcpStream,
    pending_map: SharedPendingMap,
    session_map: SharedSessionMap,
    expected_token: String,
    session_id_gen: Arc<AtomicU64>,
    allowed_proxy_ports: Arc<HashSet<u16>>,
) -> Result<()> {
    let mut reader = BufReader::new(stream_tcp);
    let message: ControlMessage = read_json_line(&mut reader).await?;

    match message {
        ControlMessage::Hello {
            token,
            forwards,
            proxy_port,
        } => {
            if token != expected_token {
                anyhow::bail!("invalid token");
            }

            let proxy_ports = normalize_registered_ports(forwards, proxy_port)?;
            for port in &proxy_ports {
                if !allowed_proxy_ports.contains(port) {
                    anyhow::bail!("proxy port {} is not listened by server", port);
                }
            }

            let session_id = session_id_gen.fetch_add(1, Ordering::Relaxed);
            let work_token = generate_work_token(session_id);
            let stream = reader.into_inner();

            log_info(
                "control",
                "authenticated",
                &[
                    ("peer_addr", format!("{:?}", stream.peer_addr()?)),
                    ("session_id", session_id.to_string()),
                    ("proxy_ports", format!("{proxy_ports:?}")),
                ],
            );

            handle_control_connection(stream, session_map, proxy_ports, session_id, work_token)
                .await
        }
        ControlMessage::Work {
            id,
            session_id,
            work_token,
        } => {
            log_info(
                "work",
                "arrived",
                &[
                    ("conn_id", id.to_string()),
                    ("session_id", session_id.to_string()),
                ],
            );
            let stream = reader.into_inner();
            complete_work_connection(id, session_id, &work_token, stream, pending_map).await
        }
        other => Err(anyhow!("unexpected initial message: {:?}", other)),
    }
}

async fn handle_control_connection(
    mut stream: TcpStream,
    session_map: SharedSessionMap,
    proxy_ports: Vec<u16>,
    session_id: u64,
    work_token: String,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<ControlMessage>(CONTROL_CHANNEL_CAPACITY);

    // 端口注册必须是一个原子动作：先在同一把锁里检查所有端口是否空闲，
    // 再一次性插入当前 session。这样可以避免两个客户端并发连接时都认为
    // 端口可用，最后出现后注册的客户端覆盖先注册客户端的问题。
    register_control_session(
        &session_map,
        &proxy_ports,
        session_id,
        &work_token,
        tx.clone(),
    )
    .await?;

    if let Err(error) = write_json_line(
        &mut stream,
        &ControlMessage::HelloAck {
            session_id,
            work_token: work_token.clone(),
        },
    )
    .await
    {
        clear_control_session(&session_map, &proxy_ports, session_id).await;
        return Err(error).context("failed to send hello_ack");
    }

    log_info(
        "control",
        "registered",
        &[
            ("session_id", session_id.to_string()),
            ("proxy_ports", format!("{proxy_ports:?}")),
        ],
    );

    let (read, mut write) = stream.into_split();
    let reader_tx = tx.clone();
    let mut writer_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            write_json_line(&mut write, &message).await?;
        }
        Ok::<(), anyhow::Error>(())
    });

    let mut reader_task = tokio::spawn(async move {
        let mut reader = BufReader::new(read);

        loop {
            match read_json_line::<_, ControlMessage>(&mut reader).await {
                Ok(ControlMessage::Ping {
                    session_id: ping_session_id,
                }) if ping_session_id == session_id => {
                    reader_tx
                        .send(ControlMessage::Pong { session_id })
                        .await
                        .map_err(|_| anyhow!("control writer dropped"))?;
                }
                Ok(ControlMessage::Pong {
                    session_id: pong_session_id,
                }) if pong_session_id == session_id => {}
                Ok(other) => {
                    log_warn(
                        "control",
                        "unexpected_message",
                        &[
                            ("session_id", session_id.to_string()),
                            ("message", format!("{other:?}")),
                        ],
                    );
                }
                Err(error) => {
                    return Err(anyhow!("control read error: {}", error));
                }
            }
        }
    });

    let session_result = tokio::select! {
        result = (&mut writer_task) => {
            reader_task.abort();
            result.context("control writer task join error")?
        }
        result = (&mut reader_task) => {
            writer_task.abort();
            result.context("control reader task join error")?
        }
    };

    if let Err(error) = session_result {
        log_error(
            "control",
            "session_closed_with_error",
            &[
                ("session_id", session_id.to_string()),
                ("error", format!("{error:#?}")),
            ],
        );
    }

    clear_control_session(&session_map, &proxy_ports, session_id).await;
    log_info(
        "control",
        "closed",
        &[
            ("session_id", session_id.to_string()),
            ("proxy_ports", format!("{proxy_ports:?}")),
        ],
    );
    Ok(())
}

async fn handle_user_connection(
    mut user_stream: TcpStream,
    conn_id: u64,
    proxy_port: u16,
    session_map: SharedSessionMap,
    pending_map: SharedPendingMap,
) -> Result<()> {
    let control = {
        let guard = session_map.lock().await;
        guard
            .get(&proxy_port)
            .cloned()
            .ok_or_else(|| anyhow!("no active client for proxy port {}", proxy_port))?
    };

    let (work_tx, work_rx) = oneshot::channel::<TcpStream>();
    {
        let mut map = pending_map.lock().await;
        let previous = map.insert(
            conn_id,
            PendingWork {
                session_id: control.session_id,
                work_token: control.work_token.clone(),
                created_at: Instant::now(),
                sender: work_tx,
            },
        );

        if previous.is_some() {
            log_warn(
                "pending",
                "conn_id_replaced",
                &[("conn_id", conn_id.to_string())],
            );
        }
    }

    if let Err(error) = control
        .sender
        .send(ControlMessage::NewConn {
            id: conn_id,
            proxy_port,
        })
        .await
    {
        remove_pending_connection(&pending_map, conn_id).await;
        return Err(error.into());
    }
    log_info(
        "proxy",
        "new_conn_sent",
        &[
            ("conn_id", conn_id.to_string()),
            ("session_id", control.session_id.to_string()),
            ("proxy_port", proxy_port.to_string()),
        ],
    );

    let mut work_stream = match tokio::time::timeout(WORK_CONNECT_TIMEOUT, work_rx).await {
        Ok(Ok(work_stream)) => work_stream,
        Ok(Err(_)) => {
            remove_pending_connection(&pending_map, conn_id).await;
            return Err(anyhow!("work connection sender dropped, id={}", conn_id));
        }
        Err(_) => {
            remove_pending_connection(&pending_map, conn_id).await;
            return Err(anyhow!("work connection timeout, id={}", conn_id));
        }
    };

    let result = io::copy_bidirectional(&mut user_stream, &mut work_stream).await;
    match result {
        Ok((user_to_client, client_to_user)) => {
            log_info(
                "proxy",
                "forward_finished",
                &[
                    ("conn_id", conn_id.to_string()),
                    ("user_to_client_bytes", user_to_client.to_string()),
                    ("client_to_user_bytes", client_to_user.to_string()),
                ],
            );
        }
        Err(error) => {
            log_error(
                "proxy",
                "forward_failed",
                &[
                    ("conn_id", conn_id.to_string()),
                    ("error", format!("{error:?}")),
                ],
            );
        }
    }

    Ok(())
}

async fn complete_work_connection(
    id: u64,
    session_id: u64,
    work_token: &str,
    stream: TcpStream,
    pending_map: SharedPendingMap,
) -> Result<()> {
    let sender = {
        let mut map = pending_map.lock().await;
        match map.entry(id) {
            Entry::Occupied(entry) => {
                let pending = entry.get();
                if pending.session_id != session_id || pending.work_token != work_token {
                    return Err(anyhow!(
                        "work connection auth mismatch, id={}, session_id={}",
                        id,
                        session_id
                    ));
                }

                entry.remove().sender
            }
            Entry::Vacant(_) => {
                return Err(anyhow!(
                    "unexpected work connection, id={}, session_id={}",
                    id,
                    session_id
                ));
            }
        }
    };

    sender
        .send(stream)
        .map_err(|_| anyhow!("work receiver dropped, id={}", id))
}

async fn remove_pending_connection(pending_map: &SharedPendingMap, conn_id: u64) {
    let mut map = pending_map.lock().await;
    map.remove(&conn_id);
}

async fn register_control_session(
    session_map: &SharedSessionMap,
    proxy_ports: &[u16],
    session_id: u64,
    work_token: &str,
    sender: mpsc::Sender<ControlMessage>,
) -> Result<()> {
    let mut guard = session_map.lock().await;

    for proxy_port in proxy_ports {
        if let Some(existing) = guard.get(proxy_port) {
            anyhow::bail!(
                "proxy port {} already registered by active session {}",
                proxy_port,
                existing.session_id
            );
        }
    }

    for proxy_port in proxy_ports {
        guard.insert(
            *proxy_port,
            ControlSession {
                session_id,
                work_token: work_token.to_string(),
                sender: sender.clone(),
            },
        );
    }

    Ok(())
}

async fn clear_control_session(
    session_map: &SharedSessionMap,
    proxy_ports: &[u16],
    session_id: u64,
) {
    let mut guard = session_map.lock().await;
    for proxy_port in proxy_ports {
        let should_remove = guard
            .get(proxy_port)
            .map(|s| s.session_id == session_id)
            .unwrap_or(false);
        if should_remove {
            guard.remove(proxy_port);
        }
    }
}

async fn pending_cleanup_loop(pending_map: SharedPendingMap) {
    let mut interval = tokio::time::interval(PENDING_SWEEP_INTERVAL);

    loop {
        interval.tick().await;
        let removed = cleanup_stale_pending_connections(&pending_map, PENDING_WORK_TTL).await;

        if removed > 0 {
            log_warn(
                "pending",
                "stale_batch_removed",
                &[
                    ("removed_count", removed.to_string()),
                    ("ttl_secs", PENDING_WORK_TTL.as_secs().to_string()),
                ],
            );
        }
    }
}

async fn cleanup_stale_pending_connections(pending_map: &SharedPendingMap, ttl: Duration) -> usize {
    let now = Instant::now();
    let mut map = pending_map.lock().await;

    // 正常情况下，单个外部连接会在 WORK_CONNECT_TIMEOUT 后自行清理 pending。
    // 这里的批量清理是兜底：如果等待任务被取消、逻辑提前退出，或者后续扩展
    // 引入了新的分支没有主动 remove，后台扫描仍然可以把超期 oneshot sender
    // 一次性 drop 掉，防止 pending_map 长期增长。
    let stale_ids = map
        .iter()
        .filter_map(|(conn_id, pending)| {
            if now.duration_since(pending.created_at) > ttl {
                Some(*conn_id)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    for conn_id in &stale_ids {
        map.remove(conn_id);
    }

    stale_ids.len()
}

fn normalize_registered_ports(
    forwards: Vec<ForwardRegistration>,
    proxy_port: Option<u16>,
) -> Result<Vec<u16>> {
    let ports = if forwards.is_empty() {
        proxy_port.into_iter().collect::<Vec<_>>()
    } else {
        forwards
            .into_iter()
            .map(|forward| forward.proxy_port)
            .collect::<Vec<_>>()
    };

    if ports.is_empty() {
        anyhow::bail!("client hello requires at least one proxy port");
    }

    let mut seen = HashSet::new();
    for port in &ports {
        if !seen.insert(*port) {
            anyhow::bail!("duplicate proxy port in client hello: {}", port);
        }
    }

    Ok(ports)
}

fn generate_work_token(session_id: u64) -> String {
    let mut bytes = [0u8; 32];
    if let Err(error) = fill_random_bytes(&mut bytes) {
        log_warn(
            "control",
            "secure_random_unavailable",
            &[
                ("session_id", session_id.to_string()),
                ("error", format!("{error:?}")),
            ],
        );
        fill_fallback_bytes(&mut bytes, session_id);
    }

    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut token, "{byte:02x}");
    }
    token
}

#[cfg(unix)]
fn fill_random_bytes(buffer: &mut [u8]) -> std::io::Result<()> {
    let mut file = std::fs::File::open("/dev/urandom")?;
    std::io::Read::read_exact(&mut file, buffer)
}

#[cfg(not(unix))]
fn fill_random_bytes(_buffer: &mut [u8]) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "secure random source unavailable",
    ))
}

fn fill_fallback_bytes(buffer: &mut [u8], session_id: u64) {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut state = timestamp ^ ((session_id as u128) << 64) ^ u128::from(std::process::id());

    for byte in buffer.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
}

fn log_info(component: &str, event: &str, fields: &[(&str, String)]) {
    log_line("INFO", component, event, fields);
}

fn log_warn(component: &str, event: &str, fields: &[(&str, String)]) {
    log_line("WARN", component, event, fields);
}

fn log_error(component: &str, event: &str, fields: &[(&str, String)]) {
    log_line("ERROR", component, event, fields);
}

fn log_line(level: &str, component: &str, event: &str, fields: &[(&str, String)]) {
    let mut line = format!(
        "ts={} level={} component={} event={}",
        unix_timestamp_millis(),
        level,
        component,
        event
    );

    for (key, value) in fields {
        let _ = write!(&mut line, " {}={}", key, value);
    }

    if level == "ERROR" {
        eprintln!("{line}");
    } else {
        println!("{line}");
    }
}

fn unix_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
