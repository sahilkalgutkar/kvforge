//! Executes a parsed [`Command`] against a [`Store`], producing the
//! response `Value` to send back over the wire.
//!
//! This is transport-agnostic on purpose: the TCP server calls it per
//! request, and AOF replay on startup calls it per logged command, so the
//! semantics only need to be right in one place.

use crate::command::Command;
use crate::protocol::Value;
use crate::store::Store;

pub fn execute(store: &Store, command: &Command) -> Value {
    match command {
        Command::Ping => Value::Simple("PONG".to_string()),
        Command::Set { key, value, ttl } => {
            match ttl {
                Some(ttl) => store.set_with_ttl(key.clone(), value.clone(), *ttl),
                None => store.set(key.clone(), value.clone()),
            }
            Value::ok()
        }
        Command::Get { key } => match store.get(key) {
            Some(value) => Value::bulk(value),
            None => Value::nil(),
        },
        Command::Del { key } => Value::Integer(i64::from(store.del(key))),
        Command::Exists { key } => Value::Integer(i64::from(store.exists(key))),
        Command::Expire { key, ttl } => Value::Integer(i64::from(store.expire(key, *ttl))),
        Command::Ttl { key } => match store.ttl(key) {
            None => Value::Integer(-2),       // key does not exist
            Some(None) => Value::Integer(-1), // key exists, no expiry
            // Round up to whole seconds so a key set with a 10s TTL still
            // reports 10 immediately afterward, not 9 from truncation.
            Some(Some(remaining)) => Value::Integer(remaining.as_millis().div_ceil(1000) as i64),
        },
        Command::FlushAll => {
            store.flush();
            Value::ok()
        }
    }
}

/// Whether executing `command` can change the store's contents. AOF only
/// needs to log commands where this is `true`.
pub fn is_write(command: &Command) -> bool {
    matches!(
        command,
        Command::Set { .. } | Command::Del { .. } | Command::Expire { .. } | Command::FlushAll
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn ping_replies_pong() {
        let store = Store::new();
        assert_eq!(
            execute(&store, &Command::Ping),
            Value::Simple("PONG".into())
        );
    }

    #[test]
    fn set_then_get_round_trips() {
        let store = Store::new();
        execute(
            &store,
            &Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: None,
            },
        );
        assert_eq!(
            execute(&store, &Command::Get { key: b"a".to_vec() }),
            Value::bulk(b"1".to_vec())
        );
    }

    #[test]
    fn get_missing_key_is_nil() {
        let store = Store::new();
        assert_eq!(
            execute(&store, &Command::Get { key: b"a".to_vec() }),
            Value::nil()
        );
    }

    #[test]
    fn del_reports_one_when_removed_zero_when_absent() {
        let store = Store::new();
        execute(
            &store,
            &Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: None,
            },
        );
        assert_eq!(
            execute(&store, &Command::Del { key: b"a".to_vec() }),
            Value::Integer(1)
        );
        assert_eq!(
            execute(&store, &Command::Del { key: b"a".to_vec() }),
            Value::Integer(0)
        );
    }

    #[test]
    fn exists_reports_zero_or_one() {
        let store = Store::new();
        assert_eq!(
            execute(&store, &Command::Exists { key: b"a".to_vec() }),
            Value::Integer(0)
        );
        execute(
            &store,
            &Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: None,
            },
        );
        assert_eq!(
            execute(&store, &Command::Exists { key: b"a".to_vec() }),
            Value::Integer(1)
        );
    }

    #[test]
    fn ttl_reports_minus_two_for_missing_key() {
        let store = Store::new();
        assert_eq!(
            execute(&store, &Command::Ttl { key: b"a".to_vec() }),
            Value::Integer(-2)
        );
    }

    #[test]
    fn ttl_reports_minus_one_for_key_without_expiry() {
        let store = Store::new();
        execute(
            &store,
            &Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: None,
            },
        );
        assert_eq!(
            execute(&store, &Command::Ttl { key: b"a".to_vec() }),
            Value::Integer(-1)
        );
    }

    #[test]
    fn set_with_ttl_then_ttl_reports_remaining_seconds() {
        let store = Store::new();
        execute(
            &store,
            &Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: Some(Duration::from_secs(10)),
            },
        );
        assert_eq!(
            execute(&store, &Command::Ttl { key: b"a".to_vec() }),
            Value::Integer(10)
        );
    }

    #[test]
    fn expire_on_existing_key_reports_one() {
        let store = Store::new();
        execute(
            &store,
            &Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: None,
            },
        );
        assert_eq!(
            execute(
                &store,
                &Command::Expire {
                    key: b"a".to_vec(),
                    ttl: Duration::from_secs(5),
                }
            ),
            Value::Integer(1)
        );
    }

    #[test]
    fn expire_on_missing_key_reports_zero() {
        let store = Store::new();
        assert_eq!(
            execute(
                &store,
                &Command::Expire {
                    key: b"a".to_vec(),
                    ttl: Duration::from_secs(5),
                }
            ),
            Value::Integer(0)
        );
    }

    #[test]
    fn flushall_clears_the_store_and_replies_ok() {
        let store = Store::new();
        execute(
            &store,
            &Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: None,
            },
        );
        assert_eq!(execute(&store, &Command::FlushAll), Value::ok());
        assert!(store.is_empty());
    }

    #[test]
    fn is_write_classifies_commands_correctly() {
        assert!(!is_write(&Command::Ping));
        assert!(!is_write(&Command::Get { key: b"a".to_vec() }));
        assert!(!is_write(&Command::Exists { key: b"a".to_vec() }));
        assert!(!is_write(&Command::Ttl { key: b"a".to_vec() }));
        assert!(is_write(&Command::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            ttl: None,
        }));
        assert!(is_write(&Command::Del { key: b"a".to_vec() }));
        assert!(is_write(&Command::Expire {
            key: b"a".to_vec(),
            ttl: Duration::from_secs(1),
        }));
        assert!(is_write(&Command::FlushAll));
    }
}
