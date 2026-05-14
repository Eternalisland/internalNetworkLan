// 更新内容：增强客户端弱网稳定性，支持配置化超时、工作连接并发保护和预热 work 连接池。
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use internalNetworkLan::config::{self, ClientConfig, ClientForwardConfig};
use internalNetworkLan::configure_tunnel_stream;
use internalNetworkLan::control_message::TransportProtocol;
use internalNetworkLan::udp::{UdpPacket, build_udp_data, build_udp_register, parse_udp_packet};
use internalNetworkLan::{ControlMessage, ForwardRegistration, read_json_line, write_json_line};
use tokio::io::BufReader;
use tokio::net::{TcpStream, UdpSocket, lookup_host};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{self, Duration};

const CONTROL_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const DEFAULT_CONTROL_CONNECT_TIMEOUT_SECS: u64 = 10;
const DEFAULT_CONTROL_READ_TIMEOUT_SECS: u64 = 45;
const DEFAULT_CONTROL_WRITE_TIMEOUT_SECS: u64 = 3;
const RECONNECT_DELAY_MIN: Duration = Duration::from_secs(1);
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(30);
const DEFAULT_LOCAL_CONNECT_ATTEMPTS: usize = 4;
const DEFAULT_LOCAL_CONNECT_TIMEOUT_MS: u64 = 1000;
const DEFAULT_LOCAL_RETRY_DELAY_MIN_MS: u64 = 200;
const DEFAULT_LOCAL_RETRY_DELAY_MAX_MS: u64 = 1000;
const DEFAULT_SERVER_WORK_CONNECT_TIMEOUT_SECS: u64 = 10;
const DEFAULT_MAX_CONCURRENT_WORK_CONNECTIONS: usize = 512;
const DEFAULT_READY_WORK_POOL_SIZE_PER_PORT: usize = 2;
const DEFAULT_READY_WORK_RECONNECT_DELAY_MS: u64 = 500;
const UDP_MAX_DATAGRAM_SIZE: usize = 64 * 1024;
const UDP_LOCAL_SESSION_TTL: Duration = Duration::from_secs(120);

struct ClientControlSession {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    session_id: u64,
    work_token: String,
}

#[derive(Clone)]
struct ClientRuntimeConfig {
    control_heartbeat_interval: Duration,
    control_connect_timeout: Duration,
    control_read_timeout: Duration,
    control_write_timeout: Duration,
    server_work_connect_timeout: Duration,
    max_concurrent_work_connections: usize,
    local_connect_attempts: usize,
    local_connect_timeout: Duration,
    local_retry_delay_min: Duration,
    local_retry_delay_max: Duration,
    ready_work_pool_size_per_port: usize,
    ready_work_reconnect_delay: Duration,
}

impl From<&ClientConfig> for ClientRuntimeConfig {
    fn from(config: &ClientConfig) -> Self {
        Self {
            control_heartbeat_interval: Duration::from_secs(
                config
                    .control_heartbeat_interval_secs
                    .unwrap_or(CONTROL_HEARTBEAT_INTERVAL.as_secs()),
            ),
            control_connect_timeout: Duration::from_secs(
                config
                    .control_connect_timeout_secs
                    .unwrap_or(DEFAULT_CONTROL_CONNECT_TIMEOUT_SECS),
            ),
            control_read_timeout: Duration::from_secs(
                config
                    .control_read_timeout_secs
                    .unwrap_or(DEFAULT_CONTROL_READ_TIMEOUT_SECS),
            ),
            control_write_timeout: Duration::from_secs(
                config
                    .control_write_timeout_secs
                    .unwrap_or(DEFAULT_CONTROL_WRITE_TIMEOUT_SECS),
            ),
            server_work_connect_timeout: Duration::from_secs(
                config
                    .server_work_connect_timeout_secs
                    .unwrap_or(DEFAULT_SERVER_WORK_CONNECT_TIMEOUT_SECS),
            ),
            max_concurrent_work_connections: config
                .max_concurrent_work_connections
                .unwrap_or(DEFAULT_MAX_CONCURRENT_WORK_CONNECTIONS),
            local_connect_attempts: config
                .local_connect_attempts
                .unwrap_or(DEFAULT_LOCAL_CONNECT_ATTEMPTS),
            local_connect_timeout: Duration::from_millis(
                config
                    .local_connect_timeout_ms
                    .unwrap_or(DEFAULT_LOCAL_CONNECT_TIMEOUT_MS),
            ),
            local_retry_delay_min: Duration::from_millis(
                config
                    .local_retry_delay_min_ms
                    .unwrap_or(DEFAULT_LOCAL_RETRY_DELAY_MIN_MS),
            ),
            local_retry_delay_max: Duration::from_millis(
                config
                    .local_retry_delay_max_ms
                    .unwrap_or(DEFAULT_LOCAL_RETRY_DELAY_MAX_MS),
            ),
            ready_work_pool_size_per_port: config
                .ready_work_pool_size_per_port
                .unwrap_or(DEFAULT_READY_WORK_POOL_SIZE_PER_PORT),
            ready_work_reconnect_delay: Duration::from_millis(
                config
                    .ready_work_reconnect_delay_ms
                    .unwrap_or(DEFAULT_READY_WORK_RECONNECT_DELAY_MS),
            ),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_client_config()?;
    let forwards = config.normalized_forwards()?;
    let runtime = ClientRuntimeConfig::from(&config);
    run_client(&config.server_addr, forwards, config.token, runtime).await?;
    Ok(())
}

fn load_client_config() -> Result<ClientConfig> {
    if let Some(path) = config::load_config_path() {
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read client config from {path}"))?;
        let config = serde_json::from_str::<ClientConfig>(&raw)
            .with_context(|| format!("failed to parse client config from {path}"))?;
        log_info("client", "config_loaded", &[("path", format!("{path:?}"))]);
        Ok(config)
    } else {
        log_info("client", "config_default", &[]);
        Ok(ClientConfig {
            server_addr: "localhost:7000".into(),
            token: "dev-token".into(),
            forwards: vec![ClientForwardConfig {
                proxy_port: 7001,
                local_addr: "localhost:63306".into(),
                protocol: TransportProtocol::Tcp,
            }],
            local_addr: None,
            proxy_port: None,
            control_heartbeat_interval_secs: None,
            control_connect_timeout_secs: None,
            control_read_timeout_secs: None,
            control_write_timeout_secs: None,
            server_work_connect_timeout_secs: None,
            max_concurrent_work_connections: None,
            local_connect_attempts: None,
            local_connect_timeout_ms: None,
            local_retry_delay_min_ms: None,
            local_retry_delay_max_ms: None,
            ready_work_pool_size_per_port: None,
            ready_work_reconnect_delay_ms: None,
        })
    }
}

async fn run_client(
    server_addr: &str,
    forwards: Vec<ClientForwardConfig>,
    token: String,
    runtime: ClientRuntimeConfig,
) -> Result<()> {
    let mut backoff = RECONNECT_DELAY_MIN;
    let tcp_local_by_proxy_port = build_forward_map(&forwards, TransportProtocol::Tcp)?;
    let udp_local_by_proxy_port = build_forward_map(&forwards, TransportProtocol::Udp)?;
    let runtime = Arc::new(runtime);

    loop {
        match connect_control_session(server_addr, &token, &forwards, Arc::clone(&runtime)).await {
            Ok(session) => {
                backoff = RECONNECT_DELAY_MIN;
                log_info(
                    "control",
                    "session_ready",
                    &[("session_id", session.session_id.to_string())],
                );

                if let Err(error) = run_control_session(
                    session,
                    server_addr.to_string(),
                    tcp_local_by_proxy_port.clone(),
                    udp_local_by_proxy_port.clone(),
                    Arc::clone(&runtime),
                )
                .await
                {
                    log_error(
                        "control",
                        "session_ended",
                        &[("error", format!("{error:#?}"))],
                    );
                }
            }
            Err(error) => {
                log_error(
                    "control",
                    "connect_failed",
                    &[("error", format!("{error:#?}"))],
                );
            }
        }

        log_info(
            "control",
            "reconnect_scheduled",
            &[("delay_ms", backoff.as_millis().to_string())],
        );
        time::sleep(backoff).await;
        backoff = std::cmp::min(backoff.saturating_mul(2), RECONNECT_DELAY_MAX);
    }
}

async fn connect_control_session(
    server_addr: &str,
    token: &str,
    forwards: &[ClientForwardConfig],
    runtime: Arc<ClientRuntimeConfig>,
) -> Result<ClientControlSession> {
    let stream = time::timeout(runtime.control_connect_timeout, TcpStream::connect(server_addr))
        .await
        .with_context(|| {
            format!(
                "timed out connecting to server control at {server_addr} after {:?}",
                runtime.control_connect_timeout
            )
        })?
        .with_context(|| format!("failed to connect to server control at {server_addr}"))?;
    if let Err(error) = configure_tunnel_stream(&stream) {
        log_warn(
            "control",
            "tcp_config_failed",
            &[
                ("server_addr", format!("{server_addr:?}")),
                ("error", format!("{error:?}")),
            ],
        );
    }
    log_info(
        "control",
        "connected",
        &[("server_addr", format!("{server_addr:?}"))],
    );

    let (read, mut write) = stream.into_split();
    time::timeout(
        runtime.control_write_timeout,
        write_json_line(
            &mut write,
            &ControlMessage::Hello {
                token: token.to_string(),
                forwards: forwards
                    .iter()
                    .flat_map(|forward| {
                        forward
                            .protocol
                            .expands_to()
                            .into_iter()
                            .map(move |protocol| ForwardRegistration {
                                proxy_port: forward.proxy_port,
                                protocol,
                            })
                    })
                    .collect(),
                proxy_port: None,
            },
        ),
    )
    .await
    .context("hello write timeout")?
    .context("failed to send hello")?;

    let mut reader = BufReader::new(read);
    let response = time::timeout(
        runtime.control_read_timeout,
        read_json_line::<_, ControlMessage>(&mut reader),
    )
    .await
    .context("timed out waiting for hello_ack")??;

    match response {
        ControlMessage::HelloAck {
            session_id,
            work_token,
        } => Ok(ClientControlSession {
            reader,
            writer: write,
            session_id,
            work_token,
        }),
        other => Err(anyhow!("unexpected hello response: {:?}", other)),
    }
}

async fn run_control_session(
    session: ClientControlSession,
    server_addr: String,
    local_by_proxy_port: HashMap<u16, String>,
    udp_local_by_proxy_port: HashMap<u16, String>,
    runtime: Arc<ClientRuntimeConfig>,
) -> Result<()> {
    let ClientControlSession {
        reader,
        writer,
        session_id,
        work_token,
    } = session;
    let mut reader = reader;
    let work_limit = Arc::new(Semaphore::new(runtime.max_concurrent_work_connections));
    let ready_tasks = spawn_ready_work_pool(
        server_addr.clone(),
        local_by_proxy_port.clone(),
        session_id,
        work_token.clone(),
        Arc::clone(&runtime),
        Arc::clone(&work_limit),
    );
    let udp_tasks = spawn_udp_forwarders(
        server_addr.clone(),
        udp_local_by_proxy_port,
        session_id,
        work_token.clone(),
        Arc::clone(&runtime),
    );

    let heartbeat_runtime = Arc::clone(&runtime);
    let mut heartbeat_task =
        tokio::spawn(async move { heartbeat_loop(writer, session_id, heartbeat_runtime).await });

    let read_loop = async {
        loop {
            match time::timeout(
                runtime.control_read_timeout,
                read_json_line::<_, ControlMessage>(&mut reader),
            )
            .await
            {
                Ok(Ok(ControlMessage::NewConn {
                    id,
                    proxy_port,
                    protocol,
                })) => {
                    if protocol != TransportProtocol::Tcp {
                        log_warn(
                            "control",
                            "unsupported_new_conn_protocol",
                            &[
                                ("conn_id", id.to_string()),
                                ("proxy_port", proxy_port.to_string()),
                                ("protocol", format!("{protocol:?}")),
                            ],
                        );
                        continue;
                    }
                    log_info(
                        "control",
                        "new_conn_received",
                        &[
                            ("conn_id", id.to_string()),
                            ("proxy_port", proxy_port.to_string()),
                        ],
                    );

                    let Some(local_addr) = local_by_proxy_port.get(&proxy_port).cloned() else {
                        log_error(
                            "control",
                            "local_forward_missing",
                            &[
                                ("conn_id", id.to_string()),
                                ("proxy_port", proxy_port.to_string()),
                            ],
                        );
                        continue;
                    };

                    let server_addr = server_addr.clone();
                    let work_token = work_token.clone();
                    let runtime = Arc::clone(&runtime);
                    let work_permit = match Arc::clone(&work_limit).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            log_error(
                                "work",
                                "concurrency_limit_reached",
                                &[
                                    ("conn_id", id.to_string()),
                                    ("proxy_port", proxy_port.to_string()),
                                    (
                                        "max_concurrent",
                                        runtime.max_concurrent_work_connections.to_string(),
                                    ),
                                ],
                            );
                            continue;
                        }
                    };

                    tokio::spawn(async move {
                        let _work_permit = work_permit;
                        match handle_work_connection(
                            server_addr,
                            local_addr,
                            id,
                            session_id,
                            work_token,
                            runtime,
                        )
                        .await
                        {
                            Ok(()) => {
                                log_info("work", "completed", &[("conn_id", id.to_string())]);
                            }
                            Err(error) => {
                                log_error(
                                    "work",
                                    "failed",
                                    &[
                                        ("conn_id", id.to_string()),
                                        ("error", format!("{error:#?}")),
                                    ],
                                );
                            }
                        }
                    });
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
                    return Err(error.context("control connection read failed"));
                }
                Err(_) => {
                    return Err(anyhow!(
                        "control heartbeat timeout, session_id={}",
                        session_id
                    ));
                }
            }
        }
    };

    tokio::pin!(read_loop);
    let result = tokio::select! {
        read_result = &mut read_loop => {
            heartbeat_task.abort();
            read_result
        }
        heartbeat_result = (&mut heartbeat_task) => {
            heartbeat_result.context("heartbeat task join error")?
        }
    };

    abort_tasks(ready_tasks);
    abort_tasks(udp_tasks);
    result
}

fn build_forward_map(
    forwards: &[ClientForwardConfig],
    protocol: TransportProtocol,
) -> Result<HashMap<u16, String>> {
    if forwards.is_empty() {
        anyhow::bail!("client requires at least one forward");
    }

    let mut map = HashMap::new();
    for forward in forwards {
        if !forward.protocol.expands_to().contains(&protocol) {
            continue;
        }

        if forward.local_addr.trim().is_empty() {
            anyhow::bail!("local_addr for proxy port {} is empty", forward.proxy_port);
        }

        if map
            .insert(forward.proxy_port, forward.local_addr.clone())
            .is_some()
        {
            anyhow::bail!(
                "duplicate proxy_port in client config: {}",
                forward.proxy_port
            );
        }
    }

    Ok(map)
}

fn spawn_ready_work_pool(
    server_addr: String,
    local_by_proxy_port: HashMap<u16, String>,
    session_id: u64,
    work_token: String,
    runtime: Arc<ClientRuntimeConfig>,
    work_limit: Arc<Semaphore>,
) -> Vec<tokio::task::JoinHandle<()>> {
    if runtime.ready_work_pool_size_per_port == 0 {
        return Vec::new();
    }

    let mut tasks = Vec::new();
    for (proxy_port, local_addr) in local_by_proxy_port {
        for slot in 0..runtime.ready_work_pool_size_per_port {
            let server_addr = server_addr.clone();
            let local_addr = local_addr.clone();
            let work_token = work_token.clone();
            let runtime = Arc::clone(&runtime);
            let work_limit = Arc::clone(&work_limit);

            tasks.push(tokio::spawn(async move {
                loop {
                    let permit = match Arc::clone(&work_limit).acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => return,
                    };

                    if let Err(error) = handle_ready_work_connection(
                        server_addr.clone(),
                        local_addr.clone(),
                        proxy_port,
                        slot,
                        session_id,
                        work_token.clone(),
                        Arc::clone(&runtime),
                    )
                    .await
                    {
                        log_warn(
                            "work",
                            "ready_failed",
                            &[
                                ("proxy_port", proxy_port.to_string()),
                                ("slot", slot.to_string()),
                                ("error", format!("{error:#?}")),
                            ],
                        );
                    }

                    drop(permit);
                    time::sleep(runtime.ready_work_reconnect_delay).await;
                }
            }));
        }
    }

    tasks
}

fn abort_tasks(tasks: Vec<tokio::task::JoinHandle<()>>) {
    for task in tasks {
        task.abort();
    }
}

fn spawn_udp_forwarders(
    server_addr: String,
    local_by_proxy_port: HashMap<u16, String>,
    session_id: u64,
    work_token: String,
    runtime: Arc<ClientRuntimeConfig>,
) -> Vec<tokio::task::JoinHandle<()>> {
    local_by_proxy_port
        .into_iter()
        .map(|(proxy_port, local_addr)| {
            let server_addr = server_addr.clone();
            let work_token = work_token.clone();
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                if let Err(error) = udp_forward_loop(
                    server_addr,
                    local_addr,
                    proxy_port,
                    session_id,
                    work_token,
                    runtime,
                )
                .await
                {
                    log_error(
                        "udp",
                        "forwarder_failed",
                        &[
                            ("proxy_port", proxy_port.to_string()),
                            ("error", format!("{error:#?}")),
                        ],
                    );
                }
            })
        })
        .collect()
}

async fn udp_forward_loop(
    server_addr: String,
    local_addr: String,
    proxy_port: u16,
    session_id: u64,
    work_token: String,
    runtime: Arc<ClientRuntimeConfig>,
) -> Result<()> {
    let udp_server_addr = udp_server_addr_for_proxy(&server_addr, proxy_port)?;
    let server_addr = lookup_host(&udp_server_addr)
        .await?
        .next()
        .ok_or_else(|| anyhow!("failed to resolve udp server addr {}", udp_server_addr))?;
    let server_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    server_socket.connect(server_addr).await?;
    let local_sessions = Arc::new(Mutex::new(HashMap::<u64, Arc<UdpSocket>>::new()));
    let register_frame = build_udp_register(session_id, proxy_port, &work_token);

    {
        let server_socket = Arc::clone(&server_socket);
        let register_frame = register_frame.clone();
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            loop {
                if server_socket.send(&register_frame).await.is_err() {
                    return;
                }
                time::sleep(runtime.control_heartbeat_interval).await;
            }
        });
    }

    server_socket.send(&register_frame).await?;
    log_info(
        "udp",
        "registered",
        &[
            ("proxy_port", proxy_port.to_string()),
            ("server_addr", format!("{server_addr:?}")),
            ("local_addr", format!("{local_addr:?}")),
        ],
    );

    let mut buf = vec![0u8; UDP_MAX_DATAGRAM_SIZE];
    loop {
        let n = server_socket.recv(&mut buf).await?;
        match parse_udp_packet(&buf[..n])? {
            Some(UdpPacket::Data {
                session_id: packet_session_id,
                proxy_port: packet_proxy_port,
                conn_id,
                payload,
            }) if packet_session_id == session_id && packet_proxy_port == proxy_port => {
                let local_socket = get_or_create_udp_local_socket(
                    conn_id,
                    &local_addr,
                    proxy_port,
                    session_id,
                    Arc::clone(&server_socket),
                    Arc::clone(&local_sessions),
                )
                .await?;
                local_socket.send(&payload).await?;
            }
            Some(other) => {
                log_warn(
                    "udp",
                    "unexpected_packet",
                    &[
                        ("proxy_port", proxy_port.to_string()),
                        ("packet", format!("{other:?}")),
                    ],
                );
            }
            None => {
                log_warn(
                    "udp",
                    "non_tunnel_packet_received",
                    &[("proxy_port", proxy_port.to_string())],
                );
            }
        }
    }
}

fn udp_server_addr_for_proxy(server_addr: &str, proxy_port: u16) -> Result<String> {
    if let Ok(mut addr) = server_addr.parse::<std::net::SocketAddr>() {
        addr.set_port(proxy_port);
        return Ok(addr.to_string());
    }

    let host = server_addr
        .rsplit_once(':')
        .map(|(host, _)| host)
        .filter(|host| !host.is_empty())
        .ok_or_else(|| anyhow!("server_addr must include host and port: {}", server_addr))?;

    Ok(format!("{host}:{proxy_port}"))
}

async fn get_or_create_udp_local_socket(
    conn_id: u64,
    local_addr: &str,
    proxy_port: u16,
    session_id: u64,
    server_socket: Arc<UdpSocket>,
    local_sessions: Arc<Mutex<HashMap<u64, Arc<UdpSocket>>>>,
) -> Result<Arc<UdpSocket>> {
    {
        let sessions = local_sessions.lock().await;
        if let Some(socket) = sessions.get(&conn_id) {
            return Ok(Arc::clone(socket));
        }
    }

    let local_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    local_socket.connect(local_addr).await?;
    {
        let mut sessions = local_sessions.lock().await;
        sessions.insert(conn_id, Arc::clone(&local_socket));
    }

    let reply_socket = Arc::clone(&local_socket);
    tokio::spawn(async move {
        let mut buf = vec![0u8; UDP_MAX_DATAGRAM_SIZE];
        loop {
            match time::timeout(UDP_LOCAL_SESSION_TTL, reply_socket.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    let frame = build_udp_data(session_id, proxy_port, conn_id, &buf[..n]);
                    if server_socket.send(&frame).await.is_err() {
                        break;
                    }
                }
                Ok(Err(_)) | Err(_) => break,
            }
        }

        let mut sessions = local_sessions.lock().await;
        sessions.remove(&conn_id);
    });

    Ok(local_socket)
}

async fn heartbeat_loop(
    mut write: OwnedWriteHalf,
    session_id: u64,
    runtime: Arc<ClientRuntimeConfig>,
) -> Result<()> {
    loop {
        time::sleep(runtime.control_heartbeat_interval).await;
        time::timeout(
            runtime.control_write_timeout,
            write_json_line(&mut write, &ControlMessage::Ping { session_id }),
        )
        .await
        .context("heartbeat ping write timeout")?
        .context("failed to send heartbeat ping")?;
    }
}

async fn connect_local_service_with_retry(
    local_addr: &str,
    conn_label: &str,
    runtime: &ClientRuntimeConfig,
) -> Result<TcpStream> {
    let mut delay = runtime.local_retry_delay_min;
    let mut last_error = None;

    for attempt in 1..=runtime.local_connect_attempts {
        log_info(
            "local",
            "connect_attempt",
            &[
                ("conn_id", conn_label.to_string()),
                ("attempt", attempt.to_string()),
                ("max_attempts", runtime.local_connect_attempts.to_string()),
                ("local_addr", format!("{local_addr:?}")),
            ],
        );

        match time::timeout(runtime.local_connect_timeout, TcpStream::connect(local_addr)).await {
            Ok(Ok(stream)) => {
                log_info(
                    "local",
                    "connect_success",
                    &[
                        ("conn_id", conn_label.to_string()),
                        ("attempt", attempt.to_string()),
                        ("local_addr", format!("{local_addr:?}")),
                    ],
                );
                if let Err(error) = configure_tunnel_stream(&stream) {
                    log_warn(
                        "local",
                        "tcp_config_failed",
                        &[
                            ("conn_id", conn_label.to_string()),
                            ("local_addr", format!("{local_addr:?}")),
                            ("error", format!("{error:?}")),
                        ],
                    );
                }
                return Ok(stream);
            }
            Ok(Err(error)) => {
                log_warn(
                    "local",
                    "connect_failed",
                    &[
                        ("conn_id", conn_label.to_string()),
                        ("attempt", attempt.to_string()),
                        ("error_kind", classify_connect_error(&error).to_string()),
                        ("error", format!("{error:?}")),
                    ],
                );
                last_error = Some(error.to_string());
            }
            Err(_) => {
                log_warn(
                    "local",
                    "connect_timeout",
                    &[
                        ("conn_id", conn_label.to_string()),
                        ("attempt", attempt.to_string()),
                        ("timeout_ms", runtime.local_connect_timeout.as_millis().to_string()),
                    ],
                );
                last_error = Some(format!(
                    "connect attempt timed out after {:?}",
                    runtime.local_connect_timeout
                ));
            }
        }

        if attempt < runtime.local_connect_attempts {
            time::sleep(delay).await;
            delay = std::cmp::min(delay.saturating_mul(2), runtime.local_retry_delay_max);
        }
    }

    Err(anyhow!(
        "failed to connect local service at {} after {} attempts: {}",
        local_addr,
        runtime.local_connect_attempts,
        last_error.unwrap_or_else(|| "unknown error".to_string())
    ))
}

fn classify_connect_error(error: &std::io::Error) -> &'static str {
    match error.kind() {
        std::io::ErrorKind::ConnectionRefused => "connection_refused",
        std::io::ErrorKind::ConnectionReset => "connection_reset",
        std::io::ErrorKind::ConnectionAborted => "connection_aborted",
        std::io::ErrorKind::TimedOut => "timed_out",
        std::io::ErrorKind::AddrInUse => "addr_in_use",
        std::io::ErrorKind::AddrNotAvailable => "addr_not_available",
        std::io::ErrorKind::NotFound => "not_found",
        std::io::ErrorKind::PermissionDenied => "permission_denied",
        _ => "other",
    }
}

async fn handle_ready_work_connection(
    server_addr: String,
    local_addr: String,
    proxy_port: u16,
    slot: usize,
    session_id: u64,
    work_token: String,
    runtime: Arc<ClientRuntimeConfig>,
) -> Result<()> {
    let conn_label = format!("ready:{proxy_port}:{slot}");
    log_info(
        "work",
        "ready_local_connect_start",
        &[
            ("conn_id", conn_label.clone()),
            ("proxy_port", proxy_port.to_string()),
            ("slot", slot.to_string()),
            ("local_addr", format!("{local_addr:?}")),
        ],
    );
    let mut local_stream =
        connect_local_service_with_retry(&local_addr, &conn_label, &runtime).await?;

    let mut server_stream = time::timeout(
        runtime.server_work_connect_timeout,
        TcpStream::connect(&server_addr),
    )
    .await
    .with_context(|| {
        format!(
            "timed out connecting ready work stream to {server_addr} after {:?}",
            runtime.server_work_connect_timeout
        )
    })?
    .with_context(|| format!("failed to connect ready work stream to {server_addr}"))?;
    if let Err(error) = configure_tunnel_stream(&server_stream) {
        log_warn(
            "work",
            "ready_tcp_config_failed",
            &[
                ("conn_id", conn_label.clone()),
                ("server_addr", format!("{server_addr:?}")),
                ("error", format!("{error:?}")),
            ],
        );
    }

    time::timeout(
        runtime.control_write_timeout,
        write_json_line(
            &mut server_stream,
            &ControlMessage::ReadyWork {
                session_id,
                work_token,
                proxy_port,
            },
        ),
    )
    .await
    .context("ready work registration write timeout")?
    .with_context(|| format!("failed to send ready work registration, proxy_port={proxy_port}"))?;

    log_info(
        "work",
        "ready_registered",
        &[
            ("conn_id", conn_label.clone()),
            ("proxy_port", proxy_port.to_string()),
            ("slot", slot.to_string()),
        ],
    );
    let result = tokio::io::copy_bidirectional(&mut server_stream, &mut local_stream).await;

    match result {
        Ok((server_to_local, local_to_server)) => {
            log_info(
                "work",
                "ready_finished",
                &[
                    ("conn_id", conn_label),
                    ("server_to_local_bytes", server_to_local.to_string()),
                    ("local_to_server_bytes", local_to_server.to_string()),
                ],
            );
        }
        Err(error) => {
            log_warn(
                "work",
                "ready_forward_failed",
                &[("conn_id", conn_label), ("error", format!("{error:?}"))],
            );
        }
    }

    Ok(())
}

async fn handle_work_connection(
    server_addr: String,
    local_addr: String,
    conn_id: u64,
    session_id: u64,
    work_token: String,
    runtime: Arc<ClientRuntimeConfig>,
) -> Result<()> {
    log_info(
        "work",
        "local_connect_start",
        &[
            ("conn_id", conn_id.to_string()),
            ("local_addr", format!("{local_addr:?}")),
        ],
    );

    // 这里先连接本地服务，再向服务端注册 Work 连接。
    // 原来的顺序是先把 Work 连接交给服务端，再连接本地服务；如果本地服务
    // 临时不可用，服务端会拿到一条马上断开的 work stream，外部用户看到的
    // 结果更像“隧道异常”。先连本地服务可以把错误明确归类为 local_connect，
    // 并允许客户端在服务端 work_connect_timeout_secs 到期前做几次短重试。
    let conn_label = conn_id.to_string();
    let mut local_stream =
        connect_local_service_with_retry(&local_addr, &conn_label, &runtime).await?;

    log_info(
        "work",
        "server_connect_start",
        &[
            ("conn_id", conn_id.to_string()),
            ("session_id", session_id.to_string()),
            ("server_addr", format!("{server_addr:?}")),
        ],
    );
    let mut server_stream = time::timeout(
        runtime.server_work_connect_timeout,
        TcpStream::connect(&server_addr),
    )
    .await
    .with_context(|| {
        format!(
            "timed out connecting work stream to {server_addr} after {:?}",
            runtime.server_work_connect_timeout
        )
    })?
    .with_context(|| format!("failed to connect work stream to {server_addr}"))?;
    if let Err(error) = configure_tunnel_stream(&server_stream) {
        log_warn(
            "work",
            "tcp_config_failed",
            &[
                ("conn_id", conn_id.to_string()),
                ("server_addr", format!("{server_addr:?}")),
                ("error", format!("{error:?}")),
            ],
        );
    }
    log_info(
        "work",
        "server_connected",
        &[("conn_id", conn_id.to_string())],
    );

    time::timeout(
        runtime.control_write_timeout,
        write_json_line(
            &mut server_stream,
            &ControlMessage::Work {
                id: conn_id,
                session_id,
                work_token,
            },
        ),
    )
    .await
    .context("work registration write timeout")?
    .with_context(|| format!("failed to send work registration, id={conn_id}"))?;

    log_info(
        "work",
        "forward_started",
        &[("conn_id", conn_id.to_string())],
    );
    let result = tokio::io::copy_bidirectional(&mut server_stream, &mut local_stream).await;

    match result {
        Ok((server_to_local, local_to_server)) => {
            log_info(
                "work",
                "forward_finished",
                &[
                    ("conn_id", conn_id.to_string()),
                    ("server_to_local_bytes", server_to_local.to_string()),
                    ("local_to_server_bytes", local_to_server.to_string()),
                ],
            );
        }
        Err(error) => {
            log_error(
                "work",
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
