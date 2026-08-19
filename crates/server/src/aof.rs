//! The live, async side of AOF durability: appending each write command to
//! disk as it happens. Replaying the log back into a `Store` on startup is
//! synchronous and lives in `kvforge_core::replay_aof` — this half only
//! needs to run inside the async server.

use kvforge_core::{encode_command, Command};
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

pub struct Aof {
    file: Mutex<File>,
}

impl Aof {
    pub async fn open(path: &Path) -> std::io::Result<Aof> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Aof {
            file: Mutex::new(file),
        })
    }

    /// Appends one command to the log and fsyncs before returning, so a
    /// crash right after this call still finds the command on replay.
    pub async fn append(&self, command: &Command) -> std::io::Result<()> {
        let bytes = encode_command(command);
        let mut file = self.file.lock().await;
        file.write_all(&bytes).await?;
        file.flush().await?;
        file.sync_data().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvforge_core::{replay_aof, Store};
    use std::sync::atomic::{AtomicU32, Ordering};

    fn temp_path() -> std::path::PathBuf {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "kvforge-aof-writer-test-{}-{n}.aof",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn appended_commands_replay_back_into_a_store() {
        let path = temp_path();
        let aof = Aof::open(&path).await.unwrap();

        aof.append(&Command::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            ttl: None,
        })
        .await
        .unwrap();
        aof.append(&Command::Del { key: b"a".to_vec() })
            .await
            .unwrap();
        aof.append(&Command::Set {
            key: b"b".to_vec(),
            value: b"2".to_vec(),
            ttl: None,
        })
        .await
        .unwrap();

        let store = Store::new();
        let replayed = replay_aof(&path, &store).unwrap();

        assert_eq!(replayed, 3);
        assert_eq!(store.get(b"a"), None);
        assert_eq!(store.get(b"b"), Some(b"2".to_vec()));

        std::fs::remove_file(&path).unwrap();
    }

    #[tokio::test]
    async fn reopening_an_existing_log_appends_rather_than_truncating() {
        let path = temp_path();
        {
            let aof = Aof::open(&path).await.unwrap();
            aof.append(&Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: None,
            })
            .await
            .unwrap();
        }
        {
            let aof = Aof::open(&path).await.unwrap();
            aof.append(&Command::Set {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
                ttl: None,
            })
            .await
            .unwrap();
        }

        let store = Store::new();
        let replayed = replay_aof(&path, &store).unwrap();

        assert_eq!(replayed, 2);
        assert_eq!(store.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b"), Some(b"2".to_vec()));

        std::fs::remove_file(&path).unwrap();
    }
}
