use kvforge_core::Store;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("KVFORGE_ADDR").unwrap_or_else(|_| "127.0.0.1:6390".to_string());
    let listener = TcpListener::bind(&addr).await?;
    println!("kvforge-server listening on {addr}");

    let store = Arc::new(Store::new());
    kvforge_server::serve(listener, store).await?;
    Ok(())
}
