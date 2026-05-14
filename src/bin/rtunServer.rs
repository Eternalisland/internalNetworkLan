// 更新内容：增强多客户端多端口转发稳定性，支持配置化超时、端口级限流和预热 work 连接池。
use std::collections::{HashMap, HashSet, VecDeque, hash_map::Entry};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use chrono::Local;
use internalNetworkLan::config::{self, ServerConfig};
use internalNetworkLan::configure_tunnel_stream;
use internalNetworkLan::control_message::{ControlMessage, ForwardRegistration, TransportProtocol};
use internalNetworkLan::udp::{UdpPacket, build_udp_data, parse_udp_packet};
pub use internalNetworkLan::frame::{read_json_line, write_json_line};
use tokio::io;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time;

const DEFAULT_CONTROL_CHANNEL_CAPACITY: usize = 1024;
const DEFAULT_CONTROL_READ_TIMEOUT_SECS: u64 = 45;
const DEFAULT_CONTROL_SEND_TIMEOUT_SECS: u64 = 3;
const DEFAULT_WORK_CONNECT_TIMEOUT_SECS: u64 = 15;
const DEFAULT_PENDING_WORK_TTL_SECS: u64 = 30;
const DEFAULT_PENDING_SWEEP_INTERVAL_SECS: u64 = 15;
const DEFAULT_MAX_CONNECTIONS_PER_PORT: usize = 1024;
const DEFAULT_READY_WORK_POOL_MAX_PER_PORT: usize = 16;
const DEFAULT_READY_WORK_TTL_SECS: u64 = 300;
const DEFAULT_UDP_SESSION_TTL_SECS: u64 = 120;
const UDP_MAX_DATAGRAM_SIZE: usize = 64 * 1024;

type SharedPendingMap = Arc<Mutex<HashMap<u64, PendingWork>>>;
type SessionKey = (u16, TransportProtocol);
type SharedSessionMap = Arc<Mutex<HashMap<SessionKey, ControlSession>>>;
type SharedReadyWorkPool = Arc<Mutex<HashMap<SessionKey, VecDeque<ReadyWork>>>>;
type SharedUdpClientMap = Arc<Mutex<HashMap<u16, UdpClient>>>;
type SharedUdpSessionMap = Arc<Mutex<UdpSessionMap>>;

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

struct ReadyWork {
    session_id: u64,
    created_at: Instant,
    stream: TcpStream,
}

struct UdpClient {
    session_id: u64,
    addr: std::net::SocketAddr,
    last_seen: Instant,
}

struct UdpUserSession {
    conn_id: u64,
    last_seen: Instant,
}

#[derive(Default)]
struct UdpSessionMap {
    user_to_session: HashMap<(u16, std::net::SocketAddr), UdpUserSession>,
    id_to_user: HashMap<(u16, u64), std::net::SocketAddr>,
}

#[derive(Clone)]
struct ServerRuntimeConfig {
    control_channel_capacity: usize,
    control_read_timeout: Duration,
    control_send_timeout: Duration,
    work_connect_timeout: Duration,
    pending_work_ttl: Duration,
    pending_sweep_interval: Duration,
    max_connections_per_port: usize,
    ready_work_pool_max_per_port: usize,
    ready_work_ttl: Duration,
    udp_session_ttl: Duration,
}

impl From<&ServerConfig> for ServerRuntimeConfig {
    fn from(config: &ServerConfig) -> Self {
        Self {
            control_channel_capacity: config
                .control_channel_capacity
                .unwrap_or(DEFAULT_CONTROL_CHANNEL_CAPACITY),
            control_read_timeout: Duration::from_secs(
                config
                    .control_read_timeout_secs
                    .unwrap_or(DEFAULT_CONTROL_READ_TIMEOUT_SECS),
            ),
            control_send_timeout: Duration::from_secs(
                config
                    .control_send_timeout_secs
                    .unwrap_or(DEFAULT_CONTROL_SEND_TIMEOUT_SECS),
            ),
            work_connect_timeout: Duration::from_secs(
                config
                    .work_connect_timeout_secs
                    .unwrap_or(DEFAULT_WORK_CONNECT_TIMEOUT_SECS),
            ),
            pending_work_ttl: Duration::from_secs(
                config
                    .pending_work_ttl_secs
                    .unwrap_or(DEFAULT_PENDING_WORK_TTL_SECS),
            ),
            pending_sweep_interval: Duration::from_secs(
                config
                    .pending_sweep_interval_secs
                    .unwrap_or(DEFAULT_PENDING_SWEEP_INTERVAL_SECS),
            ),
            max_connections_per_port: config
                .max_connections_per_port
                .unwrap_or(DEFAULT_MAX_CONNECTIONS_PER_PORT),
            ready_work_pool_max_per_port: config
                .ready_work_pool_max_per_port
                .unwrap_or(DEFAULT_READY_WORK_POOL_MAX_PER_PORT),
            ready_work_ttl: Duration::from_secs(
                config
                    .ready_work_ttl_secs
                    .unwrap_or(DEFAULT_READY_WORK_TTL_SECS),
            ),
            udp_session_ttl: Duration::from_secs(DEFAULT_UDP_SESSION_TTL_SECS),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_server_config()?;
    run_server(&config).await
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
            proxy_forwards: Vec::new(),
            token: "dev-token".into(),
            control_channel_capacity: None,
            control_read_timeout_secs: None,
            control_send_timeout_secs: None,
            work_connect_timeout_secs: None,
            pending_work_ttl_secs: None,
            pending_sweep_interval_secs: None,
            max_connections_per_port: None,
            ready_work_pool_max_per_port: None,
            ready_work_ttl_secs: None,
        })
    }
}

async fn run_server(config: &ServerConfig) -> Result<()> {
    let runtime = Arc::new(ServerRuntimeConfig::from(config));
    let forwards = config.normalized_forwards()?;
    let forward_keys = expand_server_forward_keys(&forwards)?;
    let control_listener = TcpListener::bind(&config.control_addr).await?;
    log_info(
        "server",
        "control_listening",
        &[("addr", format!("{:?}", config.control_addr))],
    );

    if forward_keys.is_empty() {
        anyhow::bail!("proxy_forwards must not be empty");
    }

    let conn_id_gen = Arc::new(AtomicU64::new(1));
    let session_id_gen = Arc::new(AtomicU64::new(1));
    let pending_map: SharedPendingMap = Arc::new(Mutex::new(HashMap::new()));
    let session_map: SharedSessionMap = Arc::new(Mutex::new(HashMap::new()));
    let ready_pool: SharedReadyWorkPool = Arc::new(Mutex::new(HashMap::new()));
    let udp_clients: SharedUdpClientMap = Arc::new(Mutex::new(HashMap::new()));
    let udp_sessions: SharedUdpSessionMap = Arc::new(Mutex::new(UdpSessionMap::default()));
    let allowed_forwards = Arc::new(forward_keys.iter().copied().collect::<HashSet<_>>());
    let port_limiters = Arc::new(
        forward_keys
            .iter()
            .map(|key| (*key, Arc::new(Semaphore::new(runtime.max_connections_per_port))))
            .collect::<HashMap<_, _>>(),
    );

    {
        let pending_map = Arc::clone(&pending_map);
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            pending_cleanup_loop(pending_map, runtime).await;
        });
    }

    {
        let ready_pool = Arc::clone(&ready_pool);
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            ready_work_cleanup_loop(ready_pool, runtime).await;
        });
    }

    // Spawn control listener
    {
        let pending_map = Arc::clone(&pending_map);
        let session_map = Arc::clone(&session_map);
        let ready_pool = Arc::clone(&ready_pool);
        let udp_clients = Arc::clone(&udp_clients);
        let token = config.token.clone();
        let session_id_gen = Arc::clone(&session_id_gen);
        let allowed_forwards = Arc::clone(&allowed_forwards);
        let runtime = Arc::clone(&runtime);

        tokio::spawn(async move {
            loop {
                match control_listener.accept().await {
                    Ok((socket, addr)) => {
                        if let Err(error) = configure_tunnel_stream(&socket) {
                            log_warn(
                                "control",
                                "tcp_config_failed",
                                &[
                                    ("peer_addr", format!("{addr:?}")),
                                    ("error", format!("{error:?}")),
                                ],
                            );
                        }
                        let pending_map = Arc::clone(&pending_map);
                        let session_map = Arc::clone(&session_map);
                        let ready_pool = Arc::clone(&ready_pool);
                        let udp_clients = Arc::clone(&udp_clients);
                        let token = token.clone();
                        let session_id_gen = Arc::clone(&session_id_gen);
                        let allowed_forwards = Arc::clone(&allowed_forwards);
                        let runtime = Arc::clone(&runtime);
                        log_info("control", "accepted", &[("peer_addr", format!("{addr:?}"))]);

                        tokio::spawn(async move {
                            if let Err(error) = handle_client_connection(
                                socket,
                                pending_map,
                                session_map,
                                ready_pool,
                                udp_clients,
                                token,
                                session_id_gen,
                                allowed_forwards,
                                runtime,
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

    // Spawn one TCP proxy listener per TCP-capable port.
    let tcp_ports = forward_keys
        .iter()
        .filter_map(|(port, protocol)| (*protocol == TransportProtocol::Tcp).then_some(*port))
        .collect::<HashSet<_>>();
    for port in tcp_ports {
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
        let ready_pool = Arc::clone(&ready_pool);
        let conn_id_gen = Arc::clone(&conn_id_gen);
        let tcp_key = (port, TransportProtocol::Tcp);
        let limiter = port_limiters
            .get(&tcp_key)
            .cloned()
            .ok_or_else(|| anyhow!("missing limiter for proxy port {}", port))?;
        let runtime = Arc::clone(&runtime);

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((user_stream, user_addr)) => {
                        if let Err(error) = configure_tunnel_stream(&user_stream) {
                            log_warn(
                                "proxy",
                                "tcp_config_failed",
                                &[
                                    ("proxy_port", port.to_string()),
                                    ("user_addr", format!("{user_addr:?}")),
                                    ("error", format!("{error:?}")),
                                ],
                            );
                        }
                        log_info(
                            "proxy",
                            "accepted",
                            &[
                                ("proxy_port", port.to_string()),
                                ("user_addr", format!("{user_addr:?}")),
                            ],
                        );
                        let permit = match Arc::clone(&limiter).try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                log_warn(
                                    "proxy",
                                    "port_connection_limit_reached",
                                    &[
                                        ("proxy_port", port.to_string()),
                                        ("user_addr", format!("{user_addr:?}")),
                                    ],
                                );
                                continue;
                            }
                        };
                        let pending_map = Arc::clone(&pending_map);
                        let session_map = Arc::clone(&session_map);
                        let ready_pool = Arc::clone(&ready_pool);
                        let runtime = Arc::clone(&runtime);
                        let conn_id = conn_id_gen.fetch_add(1, Ordering::Relaxed);

                        tokio::spawn(async move {
                            if let Err(error) = handle_user_connection(
                                user_stream,
                                conn_id,
                                port,
                                TransportProtocol::Tcp,
                                session_map,
                                ready_pool,
                                pending_map,
                                runtime,
                                permit,
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

    // Spawn one UDP proxy listener per UDP-capable port.
    let udp_ports = forward_keys
        .iter()
        .filter_map(|(port, protocol)| (*protocol == TransportProtocol::Udp).then_some(*port))
        .collect::<HashSet<_>>();
    for port in udp_ports {
        let proxy_addr = format!("0.0.0.0:{}", port);
        let socket = Arc::new(UdpSocket::bind(&proxy_addr).await?);
        log_info(
            "udp",
            "listening",
            &[
                ("proxy_port", port.to_string()),
                ("addr", format!("{proxy_addr:?}")),
            ],
        );

        let session_map = Arc::clone(&session_map);
        let udp_clients = Arc::clone(&udp_clients);
        let udp_sessions = Arc::clone(&udp_sessions);
        let conn_id_gen = Arc::clone(&conn_id_gen);
        let runtime = Arc::clone(&runtime);

        tokio::spawn(async move {
            if let Err(error) = udp_proxy_loop(
                socket,
                port,
                session_map,
                udp_clients,
                udp_sessions,
                conn_id_gen,
                runtime,
            )
            .await
            {
                log_error(
                    "udp",
                    "listener_failed",
                    &[
                        ("proxy_port", port.to_string()),
                        ("error", format!("{error:#?}")),
                    ],
                );
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
    ready_pool: SharedReadyWorkPool,
    udp_clients: SharedUdpClientMap,
    expected_token: String,
    session_id_gen: Arc<AtomicU64>,
    allowed_forwards: Arc<HashSet<SessionKey>>,
    runtime: Arc<ServerRuntimeConfig>,
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

            let registered_forwards = normalize_registered_forwards(forwards, proxy_port)?;
            for key in &registered_forwards {
                if !allowed_forwards.contains(key) {
                    anyhow::bail!(
                        "proxy port {} protocol {:?} is not listened by server",
                        key.0,
                        key.1
                    );
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
                    ("forwards", format!("{registered_forwards:?}")),
                ],
            );

            handle_control_connection(
                stream,
                session_map,
                ready_pool,
                udp_clients,
                registered_forwards,
                session_id,
                work_token,
                runtime,
            )
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
        ControlMessage::ReadyWork {
            session_id,
            work_token,
            proxy_port,
        } => {
            if !allowed_forwards.contains(&(proxy_port, TransportProtocol::Tcp)) {
                anyhow::bail!("ready work port {} is not listened by server", proxy_port);
            }

            log_info(
                "work",
                "ready_arrived",
                &[
                    ("session_id", session_id.to_string()),
                    ("proxy_port", proxy_port.to_string()),
                ],
            );
            let stream = reader.into_inner();
            register_ready_work_connection(
                proxy_port,
                session_id,
                &work_token,
                stream,
                session_map,
                ready_pool,
                runtime,
            )
            .await
        }
        other => Err(anyhow!("unexpected initial message: {:?}", other)),
    }
}

async fn handle_control_connection(
    mut stream: TcpStream,
    session_map: SharedSessionMap,
    ready_pool: SharedReadyWorkPool,
    udp_clients: SharedUdpClientMap,
    registered_forwards: Vec<SessionKey>,
    session_id: u64,
    work_token: String,
    runtime: Arc<ServerRuntimeConfig>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<ControlMessage>(runtime.control_channel_capacity);

    // 端口注册必须是一个原子动作：先在同一把锁里检查所有端口是否空闲，
    // 再一次性插入当前 session。这样可以避免两个客户端并发连接时都认为
    // 端口可用，最后出现后注册的客户端覆盖先注册客户端的问题。
    register_control_session(
        &session_map,
        &registered_forwards,
        session_id,
        &work_token,
        tx.clone(),
    )
    .await?;

    match time::timeout(
        runtime.control_send_timeout,
        write_json_line(
            &mut stream,
            &ControlMessage::HelloAck {
                session_id,
                work_token: work_token.clone(),
            },
        ),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            clear_control_session(&session_map, &registered_forwards, session_id).await;
            return Err(error).context("failed to send hello_ack");
        }
        Err(error) => {
            clear_control_session(&session_map, &registered_forwards, session_id).await;
            return Err(error).context("hello_ack write timeout");
        }
    }

    log_info(
        "control",
        "registered",
        &[
            ("session_id", session_id.to_string()),
            ("forwards", format!("{registered_forwards:?}")),
        ],
    );

    let (read, mut write) = stream.into_split();
    let reader_tx = tx.clone();
    let writer_runtime = Arc::clone(&runtime);
    let mut writer_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            time::timeout(
                writer_runtime.control_send_timeout,
                write_json_line(&mut write, &message),
            )
            .await
            .context("control write timeout")??;
        }
        Ok::<(), anyhow::Error>(())
    });

    let reader_runtime = Arc::clone(&runtime);
    let mut reader_task = tokio::spawn(async move {
        let mut reader = BufReader::new(read);

        loop {
            match time::timeout(
                reader_runtime.control_read_timeout,
                read_json_line::<_, ControlMessage>(&mut reader),
            )
            .await
            {
                Ok(Ok(ControlMessage::Ping {
                    session_id: ping_session_id,
                })) if ping_session_id == session_id => {
                    time::timeout(
                        reader_runtime.control_send_timeout,
                        reader_tx.send(ControlMessage::Pong { session_id }),
                    )
                    .await
                    .context("control pong enqueue timeout")?
                    .map_err(|_| anyhow!("control writer dropped"))?;
                }
                Ok(Ok(ControlMessage::Pong {
                    session_id: pong_session_id,
                })) if pong_session_id == session_id => {}
                Ok(Ok(other)) => {
                    log_warn(
                        "control",
                        "unexpected_message",
                        &[
                            ("session_id", session_id.to_string()),
                            ("message", format!("{other:?}")),
                        ],
                    );
                }
                Ok(Err(error)) => {
                    return Err(anyhow!("control read error: {}", error));
                }
                Err(_) => {
                    return Err(anyhow!(
                        "control read timeout, session_id={}",
                        session_id
                    ));
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

    clear_control_session(&session_map, &registered_forwards, session_id).await;
    clear_ready_work_for_session(&ready_pool, session_id).await;
    clear_udp_client_for_session(&udp_clients, session_id).await;
    log_info(
        "control",
        "closed",
        &[
            ("session_id", session_id.to_string()),
            ("forwards", format!("{registered_forwards:?}")),
        ],
    );
    Ok(())
}

async fn handle_user_connection(
    mut user_stream: TcpStream,
    conn_id: u64,
    proxy_port: u16,
    protocol: TransportProtocol,
    session_map: SharedSessionMap,
    ready_pool: SharedReadyWorkPool,
    pending_map: SharedPendingMap,
    runtime: Arc<ServerRuntimeConfig>,
    _permit: OwnedSemaphorePermit,
) -> Result<()> {
    let control = {
        let guard = session_map.lock().await;
        guard
            .get(&(proxy_port, protocol))
            .cloned()
            .ok_or_else(|| anyhow!("no active client for proxy port {}", proxy_port))?
    };

    if let Some(mut work_stream) =
        take_ready_work_connection(&ready_pool, proxy_port, protocol, control.session_id).await
    {
        log_info(
            "proxy",
            "ready_work_used",
            &[
                ("conn_id", conn_id.to_string()),
                ("session_id", control.session_id.to_string()),
                ("proxy_port", proxy_port.to_string()),
            ],
        );
        return bridge_streams(conn_id, &mut user_stream, &mut work_stream).await;
    }

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

    match time::timeout(
        runtime.control_send_timeout,
        control.sender.send(ControlMessage::NewConn {
            id: conn_id,
            proxy_port,
            protocol,
        }),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            remove_pending_connection(&pending_map, conn_id).await;
            return Err(anyhow!("failed to send new_conn, id={}, error={:?}", conn_id, error));
        }
        Err(error) => {
            remove_pending_connection(&pending_map, conn_id).await;
            return Err(anyhow!(
                "failed to send new_conn within {:?}, id={}, error={:?}",
                runtime.control_send_timeout,
                conn_id,
                error
            ));
        }
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

    let mut work_stream = match tokio::time::timeout(runtime.work_connect_timeout, work_rx).await {
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

    bridge_streams(conn_id, &mut user_stream, &mut work_stream).await
}

async fn bridge_streams(
    conn_id: u64,
    user_stream: &mut TcpStream,
    work_stream: &mut TcpStream,
) -> Result<()> {
    let result = io::copy_bidirectional(user_stream, work_stream).await;
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

async fn udp_proxy_loop(
    socket: Arc<UdpSocket>,
    proxy_port: u16,
    session_map: SharedSessionMap,
    udp_clients: SharedUdpClientMap,
    udp_sessions: SharedUdpSessionMap,
    conn_id_gen: Arc<AtomicU64>,
    runtime: Arc<ServerRuntimeConfig>,
) -> Result<()> {
    let mut buf = vec![0u8; UDP_MAX_DATAGRAM_SIZE];

    loop {
        let (n, peer_addr) = socket.recv_from(&mut buf).await?;
        let bytes = &buf[..n];

        match parse_udp_packet(bytes) {
            Ok(Some(packet)) => {
                handle_udp_tunnel_packet(
                    &socket,
                    proxy_port,
                    peer_addr,
                    packet,
                    &session_map,
                    &udp_clients,
                    &udp_sessions,
                )
                .await;
            }
            Ok(None) => {
                handle_udp_user_packet(
                    &socket,
                    proxy_port,
                    peer_addr,
                    bytes,
                    &udp_clients,
                    &udp_sessions,
                    &conn_id_gen,
                    &runtime,
                )
                .await;
            }
            Err(error) => {
                log_warn(
                    "udp",
                    "packet_parse_failed",
                    &[
                        ("proxy_port", proxy_port.to_string()),
                        ("peer_addr", format!("{peer_addr:?}")),
                        ("error", format!("{error:?}")),
                    ],
                );
            }
        }
    }
}

async fn handle_udp_tunnel_packet(
    socket: &UdpSocket,
    listener_port: u16,
    peer_addr: std::net::SocketAddr,
    packet: UdpPacket,
    session_map: &SharedSessionMap,
    udp_clients: &SharedUdpClientMap,
    udp_sessions: &SharedUdpSessionMap,
) {
    match packet {
        UdpPacket::Register {
            session_id,
            proxy_port,
            work_token,
        } => {
            if proxy_port != listener_port {
                log_warn(
                    "udp",
                    "register_port_mismatch",
                    &[
                        ("listener_port", listener_port.to_string()),
                        ("proxy_port", proxy_port.to_string()),
                    ],
                );
                return;
            }

            let valid = {
                let guard = session_map.lock().await;
                guard
                    .get(&(proxy_port, TransportProtocol::Udp))
                    .map(|control| {
                        control.session_id == session_id && control.work_token == work_token
                    })
                    .unwrap_or(false)
            };

            if !valid {
                log_warn(
                    "udp",
                    "register_auth_failed",
                    &[
                        ("proxy_port", proxy_port.to_string()),
                        ("session_id", session_id.to_string()),
                    ],
                );
                return;
            }

            let mut clients = udp_clients.lock().await;
            clients.insert(
                proxy_port,
                UdpClient {
                    session_id,
                    addr: peer_addr,
                    last_seen: Instant::now(),
                },
            );
            log_info(
                "udp",
                "client_registered",
                &[
                    ("proxy_port", proxy_port.to_string()),
                    ("session_id", session_id.to_string()),
                    ("client_addr", format!("{peer_addr:?}")),
                ],
            );
        }
        UdpPacket::Data {
            session_id,
            proxy_port,
            conn_id,
            payload,
        } => {
            if proxy_port != listener_port {
                return;
            }

            let user_addr = {
                let sessions = udp_sessions.lock().await;
                sessions.id_to_user.get(&(proxy_port, conn_id)).copied()
            };
            let Some(user_addr) = user_addr else {
                log_warn(
                    "udp",
                    "user_session_missing",
                    &[
                        ("proxy_port", proxy_port.to_string()),
                        ("conn_id", conn_id.to_string()),
                    ],
                );
                return;
            };

            let client_matches = {
                let clients = udp_clients.lock().await;
                clients
                    .get(&proxy_port)
                    .map(|client| client.session_id == session_id && client.addr == peer_addr)
                    .unwrap_or(false)
            };
            if !client_matches {
                log_warn(
                    "udp",
                    "client_data_auth_failed",
                    &[
                        ("proxy_port", proxy_port.to_string()),
                        ("conn_id", conn_id.to_string()),
                    ],
                );
                return;
            }

            if let Err(error) = socket.send_to(&payload, user_addr).await {
                log_warn(
                    "udp",
                    "send_to_user_failed",
                    &[
                        ("proxy_port", proxy_port.to_string()),
                        ("conn_id", conn_id.to_string()),
                        ("error", format!("{error:?}")),
                    ],
                );
            }
        }
    }
}

async fn handle_udp_user_packet(
    socket: &UdpSocket,
    proxy_port: u16,
    user_addr: std::net::SocketAddr,
    payload: &[u8],
    udp_clients: &SharedUdpClientMap,
    udp_sessions: &SharedUdpSessionMap,
    conn_id_gen: &Arc<AtomicU64>,
    runtime: &ServerRuntimeConfig,
) {
    let client = {
        let clients = udp_clients.lock().await;
        clients.get(&proxy_port).map(|client| {
            (
                client.session_id,
                client.addr,
                Instant::now().duration_since(client.last_seen),
            )
        })
    };
    let Some((session_id, client_addr, _idle)) = client else {
        log_warn(
            "udp",
            "client_missing",
            &[
                ("proxy_port", proxy_port.to_string()),
                ("user_addr", format!("{user_addr:?}")),
            ],
        );
        return;
    };

    let conn_id = {
        let mut sessions = udp_sessions.lock().await;
        cleanup_udp_sessions_locked(&mut sessions, runtime.udp_session_ttl);
        let active_for_port = sessions
            .user_to_session
            .keys()
            .filter(|(port, _)| *port == proxy_port)
            .count();
        let key = (proxy_port, user_addr);

        if let Some(session) = sessions.user_to_session.get_mut(&key) {
            session.last_seen = Instant::now();
            session.conn_id
        } else if active_for_port >= runtime.max_connections_per_port {
            log_warn(
                "udp",
                "port_session_limit_reached",
                &[
                    ("proxy_port", proxy_port.to_string()),
                    ("user_addr", format!("{user_addr:?}")),
                ],
            );
            return;
        } else {
            let conn_id = conn_id_gen.fetch_add(1, Ordering::Relaxed);
            sessions.user_to_session.insert(
                key,
                UdpUserSession {
                    conn_id,
                    last_seen: Instant::now(),
                },
            );
            sessions.id_to_user.insert((proxy_port, conn_id), user_addr);
            conn_id
        }
    };

    let frame = build_udp_data(session_id, proxy_port, conn_id, payload);
    if let Err(error) = socket.send_to(&frame, client_addr).await {
        log_warn(
            "udp",
            "send_to_client_failed",
            &[
                ("proxy_port", proxy_port.to_string()),
                ("conn_id", conn_id.to_string()),
                ("error", format!("{error:?}")),
            ],
        );
    }
}

fn cleanup_udp_sessions_locked(sessions: &mut UdpSessionMap, ttl: Duration) {
    let now = Instant::now();
    let stale = sessions
        .user_to_session
        .iter()
        .filter_map(|(key, session)| {
            (now.duration_since(session.last_seen) > ttl).then_some((*key, session.conn_id))
        })
        .collect::<Vec<_>>();

    for (key, conn_id) in stale {
        sessions.user_to_session.remove(&key);
        sessions.id_to_user.remove(&(key.0, conn_id));
    }
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
    registered_forwards: &[SessionKey],
    session_id: u64,
    work_token: &str,
    sender: mpsc::Sender<ControlMessage>,
) -> Result<()> {
    let mut guard = session_map.lock().await;

    for key in registered_forwards {
        if let Some(existing) = guard.get(key) {
            anyhow::bail!(
                "proxy port {} protocol {:?} already registered by active session {}",
                key.0,
                key.1,
                existing.session_id
            );
        }
    }

    for key in registered_forwards {
        guard.insert(
            *key,
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
    registered_forwards: &[SessionKey],
    session_id: u64,
) {
    let mut guard = session_map.lock().await;
    for key in registered_forwards {
        let should_remove = guard
            .get(key)
            .map(|s| s.session_id == session_id)
            .unwrap_or(false);
        if should_remove {
            guard.remove(key);
        }
    }
}

async fn register_ready_work_connection(
    proxy_port: u16,
    session_id: u64,
    work_token: &str,
    stream: TcpStream,
    session_map: SharedSessionMap,
    ready_pool: SharedReadyWorkPool,
    runtime: Arc<ServerRuntimeConfig>,
) -> Result<()> {
    let control = {
        let guard = session_map.lock().await;
        guard
            .get(&(proxy_port, TransportProtocol::Tcp))
            .cloned()
            .ok_or_else(|| anyhow!("ready work has no active session for port {}", proxy_port))?
    };

    if control.session_id != session_id || control.work_token != work_token {
        anyhow::bail!(
            "ready work auth mismatch, proxy_port={}, session_id={}",
            proxy_port,
            session_id
        );
    }

    let mut pool = ready_pool.lock().await;
    let queue = pool
        .entry((proxy_port, TransportProtocol::Tcp))
        .or_default();
    if queue.len() >= runtime.ready_work_pool_max_per_port {
        anyhow::bail!(
            "ready work pool full, proxy_port={}, size={}, limit={}",
            proxy_port,
            queue.len(),
            runtime.ready_work_pool_max_per_port
        );
    }

    queue.push_back(ReadyWork {
        session_id,
        created_at: Instant::now(),
        stream,
    });
    log_info(
        "work",
        "ready_registered",
        &[
            ("session_id", session_id.to_string()),
            ("proxy_port", proxy_port.to_string()),
            ("pool_size", queue.len().to_string()),
        ],
    );

    Ok(())
}

async fn take_ready_work_connection(
    ready_pool: &SharedReadyWorkPool,
    proxy_port: u16,
    protocol: TransportProtocol,
    session_id: u64,
) -> Option<TcpStream> {
    let mut pool = ready_pool.lock().await;
    let queue = pool.get_mut(&(proxy_port, protocol))?;

    while let Some(ready) = queue.pop_front() {
        if ready.session_id == session_id {
            return Some(ready.stream);
        }

        log_warn(
            "work",
            "stale_ready_work_dropped",
            &[
                ("proxy_port", proxy_port.to_string()),
                ("expected_session_id", session_id.to_string()),
                ("ready_session_id", ready.session_id.to_string()),
            ],
        );
    }

    None
}

async fn clear_ready_work_for_session(ready_pool: &SharedReadyWorkPool, session_id: u64) {
    let mut pool = ready_pool.lock().await;
    for queue in pool.values_mut() {
        queue.retain(|ready| ready.session_id != session_id);
    }
}

async fn clear_udp_client_for_session(udp_clients: &SharedUdpClientMap, session_id: u64) {
    let mut clients = udp_clients.lock().await;
    clients.retain(|_, client| client.session_id != session_id);
}

async fn ready_work_cleanup_loop(
    ready_pool: SharedReadyWorkPool,
    runtime: Arc<ServerRuntimeConfig>,
) {
    let mut interval = tokio::time::interval(runtime.pending_sweep_interval);

    loop {
        interval.tick().await;
        let removed = cleanup_stale_ready_work_connections(&ready_pool, runtime.ready_work_ttl).await;

        if removed > 0 {
            log_warn(
                "work",
                "stale_ready_batch_removed",
                &[
                    ("removed_count", removed.to_string()),
                    ("ttl_secs", runtime.ready_work_ttl.as_secs().to_string()),
                ],
            );
        }
    }
}

async fn cleanup_stale_ready_work_connections(
    ready_pool: &SharedReadyWorkPool,
    ttl: Duration,
) -> usize {
    let now = Instant::now();
    let mut removed = 0;
    let mut pool = ready_pool.lock().await;

    for queue in pool.values_mut() {
        let before = queue.len();
        queue.retain(|ready| now.duration_since(ready.created_at) <= ttl);
        removed += before.saturating_sub(queue.len());
    }

    removed
}

async fn pending_cleanup_loop(
    pending_map: SharedPendingMap,
    runtime: Arc<ServerRuntimeConfig>,
) {
    let mut interval = tokio::time::interval(runtime.pending_sweep_interval);

    loop {
        interval.tick().await;
        let removed =
            cleanup_stale_pending_connections(&pending_map, runtime.pending_work_ttl).await;

        if removed > 0 {
            log_warn(
                "pending",
                "stale_batch_removed",
                &[
                    ("removed_count", removed.to_string()),
                    ("ttl_secs", runtime.pending_work_ttl.as_secs().to_string()),
                ],
            );
        }
    }
}

async fn cleanup_stale_pending_connections(pending_map: &SharedPendingMap, ttl: Duration) -> usize {
    let now = Instant::now();
    let mut map = pending_map.lock().await;

    // 正常情况下，单个外部连接会在 work_connect_timeout_secs 后自行清理 pending。
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

fn normalize_registered_forwards(
    forwards: Vec<ForwardRegistration>,
    proxy_port: Option<u16>,
) -> Result<Vec<SessionKey>> {
    let keys = if forwards.is_empty() {
        proxy_port
            .into_iter()
            .map(|port| (port, TransportProtocol::Tcp))
            .collect::<Vec<_>>()
    } else {
        forwards
            .into_iter()
            .flat_map(|forward| {
                forward
                    .protocol
                    .expands_to()
                    .into_iter()
                    .map(move |protocol| (forward.proxy_port, protocol))
            })
            .collect::<Vec<_>>()
    };

    if keys.is_empty() {
        anyhow::bail!("client hello requires at least one proxy port");
    }

    let mut seen = HashSet::new();
    for key in &keys {
        if !seen.insert(*key) {
            anyhow::bail!(
                "duplicate proxy port/protocol in client hello: {} {:?}",
                key.0,
                key.1
            );
        }
    }

    Ok(keys)
}

fn expand_server_forward_keys(
    forwards: &[internalNetworkLan::config::ServerForwardConfig],
) -> Result<Vec<SessionKey>> {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();

    for forward in forwards {
        for protocol in forward.protocol.expands_to() {
            let key = (forward.proxy_port, protocol);
            if !seen.insert(key) {
                anyhow::bail!(
                    "duplicate server proxy port/protocol: {} {:?}",
                    key.0,
                    key.1
                );
            }
            keys.push(key);
        }
    }

    Ok(keys)
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
        format_current_time(),
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

fn format_current_time() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}
