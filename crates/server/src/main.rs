use kvforge_core::{replay_aof, Store};
use kvforge_server::Aof;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("KVFORGE_ADDR").unwrap_or_else(|_| "127.0.0.1:6390".to_string());
    let aof_path = std::env::var("KVFORGE_AOF").ok().map(PathBuf::from);

    let store = Arc::new(Store::new());
    let aof = match aof_path {
        Some(path) => {
            let replayed = replay_aof(&path, &store)?;
            if replayed > 0 {
                println!(
                    "kvforge-server: replayed {replayed} command(s) from {}",
                    path.display()
                );
            }
            Some(Arc::new(Aof::open(&path).await?))
        }
        None => None,
    };

    let listener = TcpListener::bind(&addr).await?;
    println!("kvforge-server listening on {addr}");
    kvforge_server::serve(listener, store, aof).await?;
    Ok(())
}
