//! A RESP-inspired wire protocol: the same five value types Redis uses
//! (simple string, error, integer, bulk string, array), encoded the same
//! way, so any existing RESP client can talk to kvforge without a custom
//! driver.

use std::fmt;

/// A single protocol value, either sent as a request element or received as
/// a response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// `+OK\r\n` — short, trusted, no embedded CR/LF.
    Simple(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:123\r\n`
    Integer(i64),
    /// `$6\r\nfoobar\r\n`, or `None` for the null bulk string `$-1\r\n`.
    Bulk(Option<Vec<u8>>),
    /// `*2\r\n...\r\n`, or `None` for the null array `*-1\r\n`.
    Array(Option<Vec<Value>>),
}

impl Value {
    pub fn bulk(bytes: impl Into<Vec<u8>>) -> Value {
        Value::Bulk(Some(bytes.into()))
    }

    pub fn nil() -> Value {
        Value::Bulk(None)
    }

    pub fn ok() -> Value {
        Value::Simple("OK".to_string())
    }

    pub fn error(msg: impl Into<String>) -> Value {
        Value::Error(msg.into())
    }

    pub fn array(values: Vec<Value>) -> Value {
        Value::Array(Some(values))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Value::Simple(s) => {
                out.push(b'+');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Value::Error(s) => {
                out.push(b'-');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Value::Integer(n) => {
                out.push(b':');
                out.extend_from_slice(n.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Value::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
            Value::Bulk(Some(bytes)) => {
                out.push(b'$');
                out.extend_from_slice(bytes.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
            }
            Value::Array(None) => out.extend_from_slice(b"*-1\r\n"),
            Value::Array(Some(items)) => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode_into(out);
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// The buffer doesn't yet contain a full value; the caller should read
    /// more bytes from the socket and retry.
    Incomplete,
    /// The buffer contains bytes that can never form a valid value.
    Malformed(String),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Incomplete => write!(f, "incomplete frame"),
            ProtocolError::Malformed(msg) => write!(f, "malformed frame: {msg}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Parses one [`Value`] from the front of `buf`. Returns the value and the
/// number of bytes it consumed, so callers decoding a stream can advance
/// their buffer by that amount and try again for the next frame.
pub fn decode(buf: &[u8]) -> Result<(Value, usize), ProtocolError> {
    decode_at(buf, 0)
}

fn decode_at(buf: &[u8], start: usize) -> Result<(Value, usize), ProtocolError> {
    let Some(&tag) = buf.get(start) else {
        return Err(ProtocolError::Incomplete);
    };
    let line_start = start + 1;
    match tag {
        b'+' => {
            let (line, end) = read_line(buf, line_start)?;
            Ok((Value::Simple(to_utf8(line)?), end))
        }
        b'-' => {
            let (line, end) = read_line(buf, line_start)?;
            Ok((Value::Error(to_utf8(line)?), end))
        }
        b':' => {
            let (line, end) = read_line(buf, line_start)?;
            let n = to_utf8(line)?
                .parse::<i64>()
                .map_err(|_| ProtocolError::Malformed("invalid integer".into()))?;
            Ok((Value::Integer(n), end))
        }
        b'$' => {
            let (line, after_len) = read_line(buf, line_start)?;
            let len = to_utf8(line)?
                .parse::<i64>()
                .map_err(|_| ProtocolError::Malformed("invalid bulk length".into()))?;
            if len < 0 {
                return Ok((Value::Bulk(None), after_len));
            }
            let len = len as usize;
            let data_end = after_len + len;
            let crlf_end = data_end + 2;
            if buf.len() < crlf_end {
                return Err(ProtocolError::Incomplete);
            }
            if &buf[data_end..crlf_end] != b"\r\n" {
                return Err(ProtocolError::Malformed(
                    "bulk string not CRLF-terminated".into(),
                ));
            }
            Ok((
                Value::Bulk(Some(buf[after_len..data_end].to_vec())),
                crlf_end,
            ))
        }
        b'*' => {
            let (line, mut pos) = read_line(buf, line_start)?;
            let len = to_utf8(line)?
                .parse::<i64>()
                .map_err(|_| ProtocolError::Malformed("invalid array length".into()))?;
            if len < 0 {
                return Ok((Value::Array(None), pos));
            }
            let mut items = Vec::with_capacity(len as usize);
            for _ in 0..len {
                let (value, end) = decode_at(buf, pos)?;
                items.push(value);
                pos = end;
            }
            Ok((Value::Array(Some(items)), pos))
        }
        other => Err(ProtocolError::Malformed(format!(
            "unknown type tag '{}'",
            other as char
        ))),
    }
}

fn read_line(buf: &[u8], start: usize) -> Result<(&[u8], usize), ProtocolError> {
    let mut i = start;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Ok((&buf[start..i], i + 2));
        }
        i += 1;
    }
    Err(ProtocolError::Incomplete)
}

fn to_utf8(bytes: &[u8]) -> Result<String, ProtocolError> {
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|_| ProtocolError::Malformed("not valid UTF-8".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(value: Value) {
        let encoded = value.encode();
        let (decoded, consumed) = decode(&encoded).expect("should decode");
        assert_eq!(decoded, value);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn roundtrips_simple_string() {
        roundtrip(Value::Simple("OK".into()));
    }

    #[test]
    fn roundtrips_error() {
        roundtrip(Value::Error("ERR no such key".into()));
    }

    #[test]
    fn roundtrips_integer() {
        roundtrip(Value::Integer(-42));
    }

    #[test]
    fn roundtrips_bulk_string() {
        roundtrip(Value::bulk(b"hello world".to_vec()));
    }

    #[test]
    fn roundtrips_empty_bulk_string() {
        roundtrip(Value::bulk(Vec::new()));
    }

    #[test]
    fn roundtrips_nil_bulk_string() {
        roundtrip(Value::nil());
    }

    #[test]
    fn roundtrips_nil_array() {
        roundtrip(Value::Array(None));
    }

    #[test]
    fn roundtrips_nested_array() {
        roundtrip(Value::array(vec![
            Value::bulk(b"SET".to_vec()),
            Value::bulk(b"key".to_vec()),
            Value::bulk(b"value".to_vec()),
        ]));
    }

    #[test]
    fn bulk_string_survives_embedded_crlf() {
        roundtrip(Value::bulk(b"line one\r\nline two".to_vec()));
    }

    #[test]
    fn decode_reports_bytes_consumed_and_leaves_the_rest() {
        let mut buf = Value::Simple("OK".into()).encode();
        buf.extend_from_slice(&Value::Integer(7).encode());
        let (first, consumed) = decode(&buf).unwrap();
        assert_eq!(first, Value::Simple("OK".into()));
        let (second, _) = decode(&buf[consumed..]).unwrap();
        assert_eq!(second, Value::Integer(7));
    }

    #[test]
    fn incomplete_simple_string_reports_incomplete() {
        assert_eq!(decode(b"+OK"), Err(ProtocolError::Incomplete));
    }

    #[test]
    fn incomplete_bulk_body_reports_incomplete() {
        assert_eq!(decode(b"$5\r\nhel"), Err(ProtocolError::Incomplete));
    }

    #[test]
    fn empty_buffer_reports_incomplete() {
        assert_eq!(decode(b""), Err(ProtocolError::Incomplete));
    }

    #[test]
    fn unknown_tag_is_malformed() {
        assert!(matches!(
            decode(b"?xyz\r\n"),
            Err(ProtocolError::Malformed(_))
        ));
    }

    #[test]
    fn bulk_missing_trailing_crlf_is_malformed() {
        assert!(matches!(
            decode(b"$3\r\nabcXX"),
            Err(ProtocolError::Malformed(_))
        ));
    }

    #[test]
    fn non_utf8_simple_string_is_malformed() {
        let mut buf = vec![b'+'];
        buf.extend_from_slice(&[0xff, 0xfe]);
        buf.extend_from_slice(b"\r\n");
        assert!(matches!(decode(&buf), Err(ProtocolError::Malformed(_))));
    }
}
