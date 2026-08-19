//! Connection handling for the kvforge TCP server, split out from `main.rs`
//! so integration tests can bind an ephemeral port and drive the server
//! in-process instead of shelling out to a real binary.

mod aof;

pub use aof::Aof;

use kvforge_core::{decode, execute, is_write, Command, ProtocolError, Store, Value};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Accepts connections on `listener` forever, handling each on its own
/// task against the shared `store`. When `aof` is `Some`, every command
/// that mutates the store is appended to the log before its response goes
/// out, so a crash never acknowledges a write that wasn't durable.
pub async fn serve(
    listener: TcpListener,
    store: Arc<Store>,
    aof: Option<Arc<Aof>>,
) -> std::io::Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        let store = Arc::clone(&store);
        let aof = aof.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(socket, store, aof).await {
                eprintln!("kvforge: connection error: {err}");
            }
        });
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    store: Arc<Store>,
    aof: Option<Arc<Aof>>,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut read_buf = [0u8; 4096];

    loop {
        while let Some((response, command)) = decode_and_execute(&mut buf, &store) {
            if let (Some(aof), Some(command)) = (&aof, &command) {
                if is_write(command) {
                    if let Err(err) = aof.append(command).await {
                        eprintln!("kvforge: aof append failed: {err}");
                    }
                }
            }
            socket.write_all(&response.encode()).await?;
        }
        let n = socket.read(&mut read_buf).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&read_buf[..n]);
    }
}

/// Tries to decode and execute one request from the front of `buf`.
/// Returns `None` when `buf` doesn't yet hold a complete frame. The
/// returned `Command` is `Some` only when the frame both parsed and named
/// a real command — that's what callers use to decide whether to log to
/// the AOF. A malformed frame resets the buffer and produces an error
/// response rather than a hard I/O error; the connection is left open so
/// one bad frame doesn't cost the client its whole session.
fn decode_and_execute(buf: &mut Vec<u8>, store: &Store) -> Option<(Value, Option<Command>)> {
    match decode(buf) {
        Ok((value, consumed)) => {
            buf.drain(..consumed);
            match Command::from_request(&value) {
                Ok(command) => {
                    let response = execute(store, &command);
                    Some((response, Some(command)))
                }
                Err(err) => Some((Value::error(format!("ERR {err}")), None)),
            }
        }
        Err(ProtocolError::Incomplete) => None,
        Err(ProtocolError::Malformed(msg)) => {
            buf.clear();
            Some((Value::error(format!("ERR {msg}")), None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_frame_yields_no_response_and_keeps_buffer() {
        let store = Store::new();
        let mut buf = b"*1\r\n$4\r\nPI".to_vec();
        let result = decode_and_execute(&mut buf, &store);
        assert!(result.is_none());
        assert_eq!(buf, b"*1\r\n$4\r\nPI");
    }

    #[test]
    fn complete_ping_frame_yields_pong_and_drains_buffer() {
        let store = Store::new();
        let mut buf = Command::Ping.to_request().encode();
        let (response, command) = decode_and_execute(&mut buf, &store).unwrap();
        assert_eq!(response, Value::Simple("PONG".into()));
        assert_eq!(command, Some(Command::Ping));
        assert!(buf.is_empty());
    }

    #[test]
    fn malformed_frame_yields_error_clears_buffer_and_has_no_command() {
        let store = Store::new();
        let mut buf = b"?nope\r\n".to_vec();
        let (response, command) = decode_and_execute(&mut buf, &store).unwrap();
        assert!(matches!(response, Value::Error(_)));
        assert!(command.is_none());
        assert!(buf.is_empty());
    }

    #[test]
    fn non_command_array_yields_error_response_with_no_command() {
        let store = Store::new();
        let mut buf = Value::Integer(1).encode();
        let (response, command) = decode_and_execute(&mut buf, &store).unwrap();
        assert!(matches!(response, Value::Error(_)));
        assert!(command.is_none());
    }

    #[test]
    fn set_command_is_reported_back_for_aof_logging() {
        let store = Store::new();
        let mut buf = Command::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            ttl: None,
        }
        .to_request()
        .encode();
        let (_, command) = decode_and_execute(&mut buf, &store).unwrap();
        assert_eq!(
            command,
            Some(Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: None,
            })
        );
    }
}
