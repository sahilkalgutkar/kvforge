//! Formats a protocol `Value` the way redis-cli formats its responses, so
//! anyone who's used `redis-cli` can read kvforge-cli output at a glance.

use kvforge_core::Value;

pub fn format_value(value: &Value) -> String {
    format_at(value, 0)
}

fn format_at(value: &Value, indent: usize) -> String {
    match value {
        Value::Simple(s) => s.clone(),
        Value::Error(e) => format!("(error) {e}"),
        Value::Integer(n) => format!("(integer) {n}"),
        Value::Bulk(None) => "(nil)".to_string(),
        Value::Bulk(Some(bytes)) => format!("{:?}", String::from_utf8_lossy(bytes)),
        Value::Array(None) => "(nil)".to_string(),
        Value::Array(Some(items)) if items.is_empty() => "(empty array)".to_string(),
        Value::Array(Some(items)) => items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                format!(
                    "{}{}) {}",
                    " ".repeat(indent),
                    i + 1,
                    format_at(item, indent + 3)
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_simple_string_as_is() {
        assert_eq!(format_value(&Value::Simple("OK".into())), "OK");
    }

    #[test]
    fn formats_error_with_prefix() {
        assert_eq!(
            format_value(&Value::Error("ERR boom".into())),
            "(error) ERR boom"
        );
    }

    #[test]
    fn formats_integer_with_prefix() {
        assert_eq!(format_value(&Value::Integer(42)), "(integer) 42");
    }

    #[test]
    fn formats_nil_bulk_string() {
        assert_eq!(format_value(&Value::Bulk(None)), "(nil)");
    }

    #[test]
    fn formats_bulk_string_quoted() {
        assert_eq!(format_value(&Value::bulk(b"hello".to_vec())), "\"hello\"");
    }

    #[test]
    fn formats_empty_array() {
        assert_eq!(format_value(&Value::array(vec![])), "(empty array)");
    }

    #[test]
    fn formats_array_with_numbered_lines() {
        let value = Value::array(vec![Value::bulk(b"a".to_vec()), Value::Integer(2)]);
        assert_eq!(format_value(&value), "1) \"a\"\n2) (integer) 2");
    }
}
