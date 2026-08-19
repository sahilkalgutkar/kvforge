//! Replays an append-only file of logged write commands to rebuild a
//! [`Store`]'s contents on startup.
//!
//! The AOF format is deliberately just the wire protocol: each entry is a
//! [`Command`] encoded as a request `Value` (the same bytes a client would
//! send over TCP), one after another. That means a corrupt or truncated
//! tail — the last write cut off mid-append by a crash — decodes as
//! `Incomplete` rather than garbage, so replay can stop cleanly at the last
//! whole command instead of failing the whole file.

use crate::command::Command;
use crate::exec::execute;
use crate::protocol::{decode, ProtocolError};
use crate::store::Store;
use std::io::Read;
use std::path::Path;

/// Replays every command logged at `path` into `store`, in order.
///
/// A missing file is treated as an empty log (a fresh store, not an
/// error) so first-run startup doesn't need special-casing by the caller.
/// Returns the number of commands replayed.
pub fn replay(path: &Path, store: &Store) -> std::io::Result<usize> {
    let bytes = match std::fs::File::open(path) {
        Ok(mut file) => {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            bytes
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };
    Ok(replay_bytes(&bytes, store))
}

fn replay_bytes(mut bytes: &[u8], store: &Store) -> usize {
    let mut count = 0;
    loop {
        match decode(bytes) {
            Ok((value, consumed)) => {
                if let Ok(command) = Command::from_request(&value) {
                    execute(store, &command);
                    count += 1;
                }
                bytes = &bytes[consumed..];
            }
            // A truncated final command (crash mid-append) or the normal
            // end of the log both look like "not enough bytes left" —
            // either way, replay stops here rather than erroring.
            Err(ProtocolError::Incomplete) => break,
            Err(ProtocolError::Malformed(_)) => break,
        }
    }
    count
}

/// Encodes `command` as the bytes to append to the log.
pub fn encode(command: &Command) -> Vec<u8> {
    command.to_request().encode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn replay_of_missing_file_is_a_no_op() {
        let store = Store::new();
        let replayed = replay(Path::new("/nonexistent/kvforge-test.aof"), &store).unwrap();
        assert_eq!(replayed, 0);
        assert!(store.is_empty());
    }

    #[test]
    fn replay_bytes_applies_logged_writes_in_order() {
        let store = Store::new();
        let mut log = Vec::new();
        log.extend(encode(&Command::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            ttl: None,
        }));
        log.extend(encode(&Command::Set {
            key: b"a".to_vec(),
            value: b"2".to_vec(),
            ttl: None,
        }));
        log.extend(encode(&Command::Del { key: b"a".to_vec() }));
        log.extend(encode(&Command::Set {
            key: b"b".to_vec(),
            value: b"3".to_vec(),
            ttl: None,
        }));

        let replayed = replay_bytes(&log, &store);

        assert_eq!(replayed, 4);
        assert_eq!(store.get(b"a"), None);
        assert_eq!(store.get(b"b"), Some(b"3".to_vec()));
    }

    #[test]
    fn replay_bytes_preserves_ttl() {
        let store = Store::new();
        let log = encode(&Command::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            ttl: Some(Duration::from_secs(30)),
        });

        replay_bytes(&log, &store);

        assert!(store.ttl(b"a").unwrap().is_some());
    }

    #[test]
    fn replay_bytes_stops_at_a_truncated_trailing_command() {
        let store = Store::new();
        let mut log = encode(&Command::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            ttl: None,
        });
        let full_second = encode(&Command::Set {
            key: b"b".to_vec(),
            value: b"2".to_vec(),
            ttl: None,
        });
        // Simulate a crash mid-write: only half of the second command made
        // it to disk.
        log.extend_from_slice(&full_second[..full_second.len() / 2]);

        let replayed = replay_bytes(&log, &store);

        assert_eq!(replayed, 1);
        assert_eq!(store.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b"), None);
    }

    #[test]
    fn replay_bytes_of_empty_log_replays_nothing() {
        let store = Store::new();
        assert_eq!(replay_bytes(&[], &store), 0);
    }

    #[test]
    fn replay_round_trips_through_a_real_file() {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("kvforge-aof-test-{}-{n}.aof", std::process::id()));

        std::fs::write(
            &path,
            encode(&Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: None,
            }),
        )
        .unwrap();

        let store = Store::new();
        let replayed = replay(&path, &store).unwrap();

        assert_eq!(replayed, 1);
        assert_eq!(store.get(b"a"), Some(b"1".to_vec()));

        std::fs::remove_file(&path).unwrap();
    }
}
