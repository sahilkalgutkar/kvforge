//! Parses a request [`Value`](crate::protocol::Value) — always an array of
//! bulk strings, the same shape Redis clients send — into a typed
//! [`Command`], and encodes commands back into that shape for the client
//! side.

use crate::protocol::Value;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    },
    Get {
        key: Vec<u8>,
    },
    Del {
        key: Vec<u8>,
    },
    Exists {
        key: Vec<u8>,
    },
    Expire {
        key: Vec<u8>,
        ttl: Duration,
    },
    Ttl {
        key: Vec<u8>,
    },
    FlushAll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandError(pub String);

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CommandError {}

impl Command {
    /// Encodes this command as the bulk-string array a server expects to
    /// receive on the wire.
    pub fn to_request(&self) -> Value {
        let parts: Vec<Value> = match self {
            Command::Ping => vec![Value::bulk(b"PING".to_vec())],
            Command::Set { key, value, ttl } => {
                let mut parts = vec![
                    Value::bulk(b"SET".to_vec()),
                    Value::bulk(key.clone()),
                    Value::bulk(value.clone()),
                ];
                if let Some(ttl) = ttl {
                    parts.push(Value::bulk(b"PX".to_vec()));
                    parts.push(Value::bulk(ttl.as_millis().to_string().into_bytes()));
                }
                parts
            }
            Command::Get { key } => vec![Value::bulk(b"GET".to_vec()), Value::bulk(key.clone())],
            Command::Del { key } => vec![Value::bulk(b"DEL".to_vec()), Value::bulk(key.clone())],
            Command::Exists { key } => {
                vec![Value::bulk(b"EXISTS".to_vec()), Value::bulk(key.clone())]
            }
            Command::Expire { key, ttl } => vec![
                Value::bulk(b"EXPIRE".to_vec()),
                Value::bulk(key.clone()),
                Value::bulk(ttl.as_secs().to_string().into_bytes()),
            ],
            Command::Ttl { key } => vec![Value::bulk(b"TTL".to_vec()), Value::bulk(key.clone())],
            Command::FlushAll => vec![Value::bulk(b"FLUSHALL".to_vec())],
        };
        Value::array(parts)
    }

    /// Parses a request `Value` (expected: a non-empty array of bulk
    /// strings) into a `Command`.
    pub fn from_request(value: &Value) -> Result<Command, CommandError> {
        let Value::Array(Some(items)) = value else {
            return Err(CommandError("expected an array request".into()));
        };
        let mut args = Vec::with_capacity(items.len());
        for item in items {
            match item {
                Value::Bulk(Some(bytes)) => args.push(bytes.clone()),
                _ => return Err(CommandError("expected bulk string array elements".into())),
            }
        }
        let Some(name) = args.first() else {
            return Err(CommandError("empty command".into()));
        };
        let name = String::from_utf8_lossy(name).to_ascii_uppercase();

        match name.as_str() {
            "PING" => Ok(Command::Ping),
            "GET" => Ok(Command::Get {
                key: arg(&args, 1, "GET requires a key")?,
            }),
            "DEL" => Ok(Command::Del {
                key: arg(&args, 1, "DEL requires a key")?,
            }),
            "EXISTS" => Ok(Command::Exists {
                key: arg(&args, 1, "EXISTS requires a key")?,
            }),
            "TTL" => Ok(Command::Ttl {
                key: arg(&args, 1, "TTL requires a key")?,
            }),
            "FLUSHALL" => Ok(Command::FlushAll),
            "EXPIRE" => {
                let key = arg(&args, 1, "EXPIRE requires a key")?;
                let secs = parse_u64(&args, 2, "EXPIRE requires a numeric seconds argument")?;
                Ok(Command::Expire {
                    key,
                    ttl: Duration::from_secs(secs),
                })
            }
            "SET" => {
                let key = arg(&args, 1, "SET requires a key")?;
                let value = arg(&args, 2, "SET requires a value")?;
                let ttl = if args.len() > 3 {
                    let opt = args
                        .get(3)
                        .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase());
                    match opt.as_deref() {
                        Some("PX") => {
                            let ms =
                                parse_u64(&args, 4, "PX requires a numeric millisecond argument")?;
                            Some(Duration::from_millis(ms))
                        }
                        Some("EX") => {
                            let secs =
                                parse_u64(&args, 4, "EX requires a numeric seconds argument")?;
                            Some(Duration::from_secs(secs))
                        }
                        _ => return Err(CommandError("unsupported SET option".into())),
                    }
                } else {
                    None
                };
                Ok(Command::Set { key, value, ttl })
            }
            other => Err(CommandError(format!("unknown command '{other}'"))),
        }
    }
}

fn arg(args: &[Vec<u8>], index: usize, msg: &str) -> Result<Vec<u8>, CommandError> {
    args.get(index)
        .cloned()
        .ok_or_else(|| CommandError(msg.to_string()))
}

fn parse_u64(args: &[Vec<u8>], index: usize, msg: &str) -> Result<u64, CommandError> {
    let raw = args
        .get(index)
        .ok_or_else(|| CommandError(msg.to_string()))?;
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| CommandError(msg.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk_array(strs: &[&str]) -> Value {
        Value::array(
            strs.iter()
                .map(|s| Value::bulk(s.as_bytes().to_vec()))
                .collect(),
        )
    }

    #[test]
    fn parses_ping() {
        assert_eq!(
            Command::from_request(&bulk_array(&["PING"])).unwrap(),
            Command::Ping
        );
    }

    #[test]
    fn parses_ping_case_insensitively() {
        assert_eq!(
            Command::from_request(&bulk_array(&["ping"])).unwrap(),
            Command::Ping
        );
    }

    #[test]
    fn parses_get() {
        assert_eq!(
            Command::from_request(&bulk_array(&["GET", "a"])).unwrap(),
            Command::Get { key: b"a".to_vec() }
        );
    }

    #[test]
    fn parses_set_without_ttl() {
        assert_eq!(
            Command::from_request(&bulk_array(&["SET", "a", "1"])).unwrap(),
            Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: None,
            }
        );
    }

    #[test]
    fn parses_set_with_px() {
        assert_eq!(
            Command::from_request(&bulk_array(&["SET", "a", "1", "PX", "500"])).unwrap(),
            Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: Some(Duration::from_millis(500)),
            }
        );
    }

    #[test]
    fn parses_set_with_ex() {
        assert_eq!(
            Command::from_request(&bulk_array(&["SET", "a", "1", "EX", "5"])).unwrap(),
            Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                ttl: Some(Duration::from_secs(5)),
            }
        );
    }

    #[test]
    fn rejects_set_with_bad_ttl_option() {
        assert!(Command::from_request(&bulk_array(&["SET", "a", "1", "ZZ", "5"])).is_err());
    }

    #[test]
    fn rejects_get_missing_key() {
        assert!(Command::from_request(&bulk_array(&["GET"])).is_err());
    }

    #[test]
    fn rejects_unknown_command() {
        assert!(Command::from_request(&bulk_array(&["NOPE"])).is_err());
    }

    #[test]
    fn rejects_empty_array() {
        assert!(Command::from_request(&Value::array(vec![])).is_err());
    }

    #[test]
    fn rejects_non_array_request() {
        assert!(Command::from_request(&Value::Integer(1)).is_err());
    }

    #[test]
    fn rejects_expire_with_non_numeric_seconds() {
        assert!(Command::from_request(&bulk_array(&["EXPIRE", "a", "soon"])).is_err());
    }

    #[test]
    fn to_request_round_trips_through_from_request() {
        let cmd = Command::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            ttl: Some(Duration::from_millis(500)),
        };
        let parsed = Command::from_request(&cmd.to_request()).unwrap();
        assert_eq!(parsed, cmd);
    }

    #[test]
    fn expire_to_request_round_trips() {
        let cmd = Command::Expire {
            key: b"a".to_vec(),
            ttl: Duration::from_secs(30),
        };
        assert_eq!(Command::from_request(&cmd.to_request()).unwrap(), cmd);
    }
}
