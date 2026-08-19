//! Connection handling for the kvforge TCP server, split out from `main.rs`
//! so integration tests can bind an ephemeral port and drive the server
//! in-process instead of shelling out to a real binary.

use kvforge_core::{decode, execute, Command, ProtocolError, Store, Value};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Accepts connections on `listener` forever, handling each on its own
/// task against the shared `store`.
pub async fn serve(listener: TcpListener, store: Arc<Store>) -> std::io::Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            if let Err(err) = handle_connection(socket, store).await {
                eprintln!("kvforge: connection error: {err}");
            }
        });
    }
}

async fn handle_connection(mut socket: TcpStream, store: Arc<Store>) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut read_buf = [0u8; 4096];

    loop {
        while let Some(response) = try_decode_one(&mut buf, &store)? {
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
/// Returns `Ok(None)` when `buf` doesn't yet hold a complete frame.
/// A malformed frame produces an error response rather than a hard I/O
/// error — the caller is expected to send it and close the connection.
fn try_decode_one(buf: &mut Vec<u8>, store: &Store) -> std::io::Result<Option<Value>> {
    match decode(buf) {
        Ok((value, consumed)) => {
            let response = match Command::from_request(&value) {
                Ok(command) => execute(store, &command),
                Err(err) => Value::error(format!("ERR {err}")),
            };
            buf.drain(..consumed);
            Ok(Some(response))
        }
        Err(ProtocolError::Incomplete) => Ok(None),
        Err(ProtocolError::Malformed(msg)) => {
            buf.clear();
            Ok(Some(Value::error(format!("ERR {msg}"))))
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
        let result = try_decode_one(&mut buf, &store).unwrap();
        assert!(result.is_none());
        assert_eq!(buf, b"*1\r\n$4\r\nPI");
    }

    #[test]
    fn complete_ping_frame_yields_pong_and_drains_buffer() {
        let store = Store::new();
        let mut buf = Command::Ping.to_request().encode();
        let result = try_decode_one(&mut buf, &store).unwrap();
        assert_eq!(result, Some(Value::Simple("PONG".into())));
        assert!(buf.is_empty());
    }

    #[test]
    fn malformed_frame_yields_error_and_clears_buffer() {
        let store = Store::new();
        let mut buf = b"?nope\r\n".to_vec();
        let result = try_decode_one(&mut buf, &store).unwrap();
        assert!(matches!(result, Some(Value::Error(_))));
        assert!(buf.is_empty());
    }

    #[test]
    fn non_command_array_yields_error_response() {
        let store = Store::new();
        let mut buf = Value::Integer(1).encode();
        let result = try_decode_one(&mut buf, &store).unwrap();
        assert!(matches!(result, Some(Value::Error(_))));
    }
}
