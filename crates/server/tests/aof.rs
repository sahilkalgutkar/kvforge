//! Proves durability end-to-end: writes go through a real server over a
//! real socket, the server is torn down, and a fresh server — replaying
//! the same log file the way `main` does before it starts serving —
//! answers GET for that data over a brand new connection.

use kvforge_core::{decode, replay_aof, Command, Store, Value};
use kvforge_server::Aof;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn temp_path() -> std::path::PathBuf {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("kvforge-e2e-aof-{}-{n}.aof", std::process::id()))
}

/// Mirrors what `main` does: replay the log into a store, then serve with
/// an AOF writer pointed at the same file for subsequent writes.
async fn start_server_from_log(
    path: &std::path::Path,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let store = Arc::new(Store::new());
    replay_aof(path, &store).unwrap();
    let aof = Arc::new(Aof::open(path).await.unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = kvforge_server::serve(listener, store, Some(aof)).await;
    });
    (addr, handle)
}

async fn send(stream: &mut TcpStream, command: Command) -> Value {
    stream
        .write_all(&command.to_request().encode())
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    decode(&buf[..n]).unwrap().0
}

#[tokio::test]
async fn writes_survive_a_server_restart_via_the_aof() {
    let path = temp_path();

    // First server: write some data, then tear it down as if it crashed.
    let (addr, handle) = start_server_from_log(&path).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    send(
        &mut stream,
        Command::Set {
            key: b"user:1".to_vec(),
            value: b"sahil".to_vec(),
            ttl: None,
        },
    )
    .await;
    send(
        &mut stream,
        Command::Set {
            key: b"user:2".to_vec(),
            value: b"temp".to_vec(),
            ttl: None,
        },
    )
    .await;
    send(
        &mut stream,
        Command::Del {
            key: b"user:2".to_vec(),
        },
    )
    .await;
    drop(stream);
    handle.abort();

    // Second server, same log file, started the same way `main` starts —
    // nothing carried over from the first server except the file on disk.
    let (addr2, handle2) = start_server_from_log(&path).await;
    let mut stream2 = TcpStream::connect(addr2).await.unwrap();

    assert_eq!(
        send(
            &mut stream2,
            Command::Get {
                key: b"user:1".to_vec()
            }
        )
        .await,
        Value::bulk(b"sahil".to_vec())
    );
    assert_eq!(
        send(
            &mut stream2,
            Command::Get {
                key: b"user:2".to_vec()
            }
        )
        .await,
        Value::nil()
    );

    // A fresh write against the restarted server appends on top of the
    // replayed log rather than overwriting it.
    send(
        &mut stream2,
        Command::Set {
            key: b"user:3".to_vec(),
            value: b"new".to_vec(),
            ttl: None,
        },
    )
    .await;
    drop(stream2);
    handle2.abort();

    let verify_store = Store::new();
    let replayed = replay_aof(&path, &verify_store).unwrap();
    assert_eq!(replayed, 4); // 2 sets + 1 del from server one, 1 set from server two
    assert_eq!(verify_store.get(b"user:1"), Some(b"sahil".to_vec()));
    assert_eq!(verify_store.get(b"user:3"), Some(b"new".to_vec()));

    std::fs::remove_file(&path).unwrap();
}
