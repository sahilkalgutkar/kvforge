//! End-to-end tests that speak the real wire protocol over a real TCP
//! socket, rather than calling connection-handling functions directly.

use kvforge_core::{decode, Command, Store, Value};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct TestServer {
    addr: std::net::SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start() -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let store = Arc::new(Store::new());
        let handle = tokio::spawn(async move {
            let _ = kvforge_server::serve(listener, store, None).await;
        });
        TestServer { addr, handle }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn send(stream: &mut TcpStream, command: Command) -> Value {
    stream
        .write_all(&command.to_request().encode())
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let (value, consumed) = decode(&buf[..n]).unwrap();
    assert_eq!(consumed, n, "response should be exactly one frame");
    value
}

#[tokio::test]
async fn ping_replies_pong() {
    let server = TestServer::start().await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    assert_eq!(
        send(&mut stream, Command::Ping).await,
        Value::Simple("PONG".into())
    );
}

#[tokio::test]
async fn set_then_get_round_trips_over_the_wire() {
    let server = TestServer::start().await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    let set = Command::Set {
        key: b"greeting".to_vec(),
        value: b"hello".to_vec(),
        ttl: None,
    };
    assert_eq!(send(&mut stream, set).await, Value::ok());

    let get = Command::Get {
        key: b"greeting".to_vec(),
    };
    assert_eq!(send(&mut stream, get).await, Value::bulk(b"hello".to_vec()));
}

#[tokio::test]
async fn get_of_missing_key_is_nil() {
    let server = TestServer::start().await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    let get = Command::Get {
        key: b"nope".to_vec(),
    };
    assert_eq!(send(&mut stream, get).await, Value::nil());
}

#[tokio::test]
async fn multiple_requests_on_one_connection_are_handled_in_order() {
    let server = TestServer::start().await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    for i in 0..5 {
        let key = format!("k{i}").into_bytes();
        let value = format!("v{i}").into_bytes();
        let set = Command::Set {
            key: key.clone(),
            value: value.clone(),
            ttl: None,
        };
        assert_eq!(send(&mut stream, set).await, Value::ok());
        let get = Command::Get { key };
        assert_eq!(send(&mut stream, get).await, Value::bulk(value));
    }
}

#[tokio::test]
async fn two_connections_share_the_same_store() {
    let server = TestServer::start().await;
    let mut writer = TcpStream::connect(server.addr).await.unwrap();
    let mut reader = TcpStream::connect(server.addr).await.unwrap();

    let set = Command::Set {
        key: b"shared".to_vec(),
        value: b"1".to_vec(),
        ttl: None,
    };
    assert_eq!(send(&mut writer, set).await, Value::ok());

    let get = Command::Get {
        key: b"shared".to_vec(),
    };
    assert_eq!(send(&mut reader, get).await, Value::bulk(b"1".to_vec()));
}

#[tokio::test]
async fn malformed_request_gets_an_error_response() {
    let server = TestServer::start().await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    stream.write_all(b"?garbage\r\n").await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let (value, _) = decode(&buf[..n]).unwrap();
    assert!(matches!(value, Value::Error(_)));
}

#[tokio::test]
async fn del_and_exists_reflect_store_state() {
    let server = TestServer::start().await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    let set = Command::Set {
        key: b"a".to_vec(),
        value: b"1".to_vec(),
        ttl: None,
    };
    send(&mut stream, set).await;

    let exists = Command::Exists { key: b"a".to_vec() };
    assert_eq!(send(&mut stream, exists).await, Value::Integer(1));

    let del = Command::Del { key: b"a".to_vec() };
    assert_eq!(send(&mut stream, del).await, Value::Integer(1));

    let exists_after = Command::Exists { key: b"a".to_vec() };
    assert_eq!(send(&mut stream, exists_after).await, Value::Integer(0));
}
