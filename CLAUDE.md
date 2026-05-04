# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build / Test Commands

```bash
cargo build                    # Build all binaries
cargo build --bin rtunServer   # Build specific binary
cargo test                     # Run all tests
cargo test --test MyTest       # Run the integration test file
cargo fmt                      # Format code
cargo clippy                   # Lint
```

## Architecture

This is a reverse-tunnel proxy in Rust (tokio async). It lets an internal-network client expose local services through a public server, similar to ngrok.

**Two binaries:**

- `rtunServer` — Public-facing server. Listens on two ports:
  - Control port (7000): internal clients connect here, authenticate with a token, then receive control commands.
  - Proxy port (9000): external users connect here; their connections get forwarded through the client's tunnel.
- `rtunClient` — Runs inside the internal network. Connects to the server's control port, authenticates, and on receiving `NewConn`, opens a work connection back to the server to carry user traffic.

`src/main.rs` is a standalone single-port TCP forwarder (legacy/demo).

**Wire protocol:** JSON-line framing (`src/frame.rs`) — each message is a single line of JSON terminated by `\n`. Control messages (`src/control_message.rs`) use a serde-tagged enum:

- `{"type":"hello","token":"..."}` — client auth
- `{"type":"new_conn","id":<u64>}` — server tells client to open a work connection
- `{"type":"work","id":<u64>}` — client announces a work connection for the given id

**Connection lifecycle (the core flow):**

1. Internal client connects to server control port, sends `Hello` with token.
2. External user connects to proxy port → server generates a connection ID, sends `NewConn` to the client via the control channel (an `mpsc` stored behind a `Mutex`).
3. Server inserts a `oneshot::Sender<TcpStream>` into `pending_map` keyed by the connection ID.
4. Client receives `NewConn`, opens a new TCP connection to the server, and sends `Work { id }`.
5. Server matches the work connection's ID to the pending oneshot, delivers the stream, and bridges user ↔ work via `tokio::io::copy_bidirectional`.

**Key types (in `src/lib.rs` re-exports):**
- `ControlMessage` — the tagged enum
- `read_json_line` / `write_json_line` — JSON-line framing over async TCP

**Dependencies:** tokio (full features), serde/serde_json, anyhow.
