use std::collections::HashMap;
use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use internalNetworkLan::config::{self, ClientConfig, ClientForwardConfig};
use internalNetworkLan::{ControlMessage, ForwardRegistration, read_json_line, write_json_line};
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::{self, Duration};

const CONTROL_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(45);
const RECONNECT_DELAY_MIN: Duration = Duration::from_secs(1);
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(30);
const LOCAL_CONNECT_ATTEMPTS: usize = 4;
const LOCAL_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const LOCAL_RETRY_DELAY_MIN: Duration = Duration::from_millis(200);
const LOCAL_RETRY_DELAY_MAX: Duration = Duration::from_secs(1);

struct ClientControlSession {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    session_id: u64,
    work_token: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_client_config()?;
    let forwards = config.normalized_forwards()?;
    run_client(&config.server_addr, forwards, config.token).await?;
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
            }],
            local_addr: None,
            proxy_port: None,
        })
    }
}

async fn run_client(
    server_addr: &str,
    forwards: Vec<ClientForwardConfig>,
    token: String,
) -> Result<()> {
    let mut backoff = RECONNECT_DELAY_MIN;
    let local_by_proxy_port = build_forward_map(&forwards)?;

    loop {
        match connect_control_session(server_addr, &token, &forwards).await {
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
                    local_by_proxy_port.clone(),
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
) -> Result<ClientControlSession> {
    let stream = TcpStream::connect(server_addr)
        .await
        .with_context(|| format!("failed to connect to server control at {server_addr}"))?;
    log_info(
        "control",
        "connected",
        &[("server_addr", format!("{server_addr:?}"))],
    );

    let (read, mut write) = stream.into_split();
    write_json_line(
        &mut write,
        &ControlMessage::Hello {
            token: token.to_string(),
            forwards: forwards
                .iter()
                .map(|forward| ForwardRegistration {
                    proxy_port: forward.proxy_port,
                })
                .collect(),
            proxy_port: None,
        },
    )
    .await
    .context("failed to send hello")?;

    let mut reader = BufReader::new(read);
    let response = time::timeout(
        CONTROL_READ_TIMEOUT,
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
) -> Result<()> {
    let ClientControlSession {
        reader,
        writer,
        session_id,
        work_token,
    } = session;
    let mut reader = reader;

    let mut heartbeat_task = tokio::spawn(async move { heartbeat_loop(writer, session_id).await });

    let read_loop = async {
        loop {
            match time::timeout(
                CONTROL_READ_TIMEOUT,
                read_json_line::<_, ControlMessage>(&mut reader),
            )
            .await
            {
                Ok(Ok(ControlMessage::NewConn { id, proxy_port })) => {
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

                    tokio::spawn(async move {
                        match handle_work_connection(
                            server_addr,
                            local_addr,
                            id,
                            session_id,
                            work_token,
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

    result
}

fn build_forward_map(forwards: &[ClientForwardConfig]) -> Result<HashMap<u16, String>> {
    if forwards.is_empty() {
        anyhow::bail!("client requires at least one forward");
    }

    let mut map = HashMap::new();
    for forward in forwards {
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

async fn heartbeat_loop(mut write: OwnedWriteHalf, session_id: u64) -> Result<()> {
    loop {
        time::sleep(CONTROL_HEARTBEAT_INTERVAL).await;
        write_json_line(&mut write, &ControlMessage::Ping { session_id })
            .await
            .context("failed to send heartbeat ping")?;
    }
}

async fn connect_local_service_with_retry(local_addr: &str, conn_id: u64) -> Result<TcpStream> {
    let mut delay = LOCAL_RETRY_DELAY_MIN;
    let mut last_error = None;

    for attempt in 1..=LOCAL_CONNECT_ATTEMPTS {
        log_info(
            "local",
            "connect_attempt",
            &[
                ("conn_id", conn_id.to_string()),
                ("attempt", attempt.to_string()),
                ("max_attempts", LOCAL_CONNECT_ATTEMPTS.to_string()),
                ("local_addr", format!("{local_addr:?}")),
            ],
        );

        match time::timeout(LOCAL_CONNECT_TIMEOUT, TcpStream::connect(local_addr)).await {
            Ok(Ok(stream)) => {
                log_info(
                    "local",
                    "connect_success",
                    &[
                        ("conn_id", conn_id.to_string()),
                        ("attempt", attempt.to_string()),
                        ("local_addr", format!("{local_addr:?}")),
                    ],
                );
                return Ok(stream);
            }
            Ok(Err(error)) => {
                log_warn(
                    "local",
                    "connect_failed",
                    &[
                        ("conn_id", conn_id.to_string()),
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
                        ("conn_id", conn_id.to_string()),
                        ("attempt", attempt.to_string()),
                        ("timeout_ms", LOCAL_CONNECT_TIMEOUT.as_millis().to_string()),
                    ],
                );
                last_error = Some(format!(
                    "connect attempt timed out after {:?}",
                    LOCAL_CONNECT_TIMEOUT
                ));
            }
        }

        if attempt < LOCAL_CONNECT_ATTEMPTS {
            time::sleep(delay).await;
            delay = std::cmp::min(delay.saturating_mul(2), LOCAL_RETRY_DELAY_MAX);
        }
    }

    Err(anyhow!(
        "failed to connect local service at {} after {} attempts: {}",
        local_addr,
        LOCAL_CONNECT_ATTEMPTS,
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

async fn handle_work_connection(
    server_addr: String,
    local_addr: String,
    conn_id: u64,
    session_id: u64,
    work_token: String,
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
    // 并允许客户端在服务端 WORK_CONNECT_TIMEOUT 到期前做几次短重试。
    let mut local_stream = connect_local_service_with_retry(&local_addr, conn_id).await?;

    log_info(
        "work",
        "server_connect_start",
        &[
            ("conn_id", conn_id.to_string()),
            ("session_id", session_id.to_string()),
            ("server_addr", format!("{server_addr:?}")),
        ],
    );
    let mut server_stream = TcpStream::connect(&server_addr)
        .await
        .with_context(|| format!("failed to connect work stream to {server_addr}"))?;
    log_info(
        "work",
        "server_connected",
        &[("conn_id", conn_id.to_string())],
    );

    write_json_line(
        &mut server_stream,
        &ControlMessage::Work {
            id: conn_id,
            session_id,
            work_token,
        },
    )
    .await
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
