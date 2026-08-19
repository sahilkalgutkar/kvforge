//! A thin TCP client: send one request, read exactly one response. No
//! pipelining — a REPL only ever has one request in flight at a time.

use kvforge_core::{decode, Command, ProtocolError, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    pub async fn connect(addr: &str) -> std::io::Result<Client> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Client {
            stream,
            buf: Vec::new(),
        })
    }

    pub async fn call(&mut self, command: &Command) -> std::io::Result<Value> {
        self.stream
            .write_all(&command.to_request().encode())
            .await?;

        loop {
            match decode(&self.buf) {
                Ok((value, consumed)) => {
                    self.buf.drain(..consumed);
                    return Ok(value);
                }
                Err(ProtocolError::Incomplete) => {}
                Err(ProtocolError::Malformed(msg)) => {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
                }
            }
            let mut read_buf = [0u8; 4096];
            let n = self.stream.read(&mut read_buf).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed the connection",
                ));
            }
            self.buf.extend_from_slice(&read_buf[..n]);
        }
    }
}
