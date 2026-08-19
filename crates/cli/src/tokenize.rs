//! Splits one REPL input line into command arguments, the way a shell
//! would: whitespace-separated, with single or double quotes letting a
//! value contain spaces (`SET greeting "hello world"`).

pub fn tokenize(line: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_token = false;
    let mut quote: Option<char> = None;

    for c in line.chars() {
        match quote {
            Some(q) if c == q => {
                quote = None;
            }
            Some(_) => current.push(c),
            None => match c {
                '\'' | '"' => {
                    quote = Some(c);
                    in_token = true;
                }
                c if c.is_whitespace() => {
                    if in_token {
                        tokens.push(std::mem::take(&mut current));
                        in_token = false;
                    }
                }
                _ => {
                    current.push(c);
                    in_token = true;
                }
            },
        }
    }

    if quote.is_some() {
        return Err("unterminated quote".to_string());
    }
    if in_token {
        tokens.push(current);
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_whitespace() {
        assert_eq!(tokenize("SET a 1").unwrap(), vec!["SET", "a", "1"]);
    }

    #[test]
    fn collapses_repeated_whitespace() {
        assert_eq!(tokenize("SET   a    1").unwrap(), vec!["SET", "a", "1"]);
    }

    #[test]
    fn empty_line_yields_no_tokens() {
        assert_eq!(tokenize("").unwrap(), Vec::<String>::new());
        assert_eq!(tokenize("   ").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn double_quotes_group_a_value_with_spaces() {
        assert_eq!(
            tokenize(r#"SET greeting "hello world""#).unwrap(),
            vec!["SET", "greeting", "hello world"]
        );
    }

    #[test]
    fn single_quotes_group_a_value_with_spaces() {
        assert_eq!(
            tokenize("SET greeting 'hello world'").unwrap(),
            vec!["SET", "greeting", "hello world"]
        );
    }

    #[test]
    fn quoted_value_can_be_adjacent_to_other_text() {
        assert_eq!(tokenize(r#"SET a "1"2"#).unwrap(), vec!["SET", "a", "12"]);
    }

    #[test]
    fn unterminated_quote_is_an_error() {
        assert!(tokenize(r#"SET a "unterminated"#).is_err());
    }

    #[test]
    fn empty_quoted_value_yields_an_empty_token() {
        assert_eq!(tokenize(r#"SET a """#).unwrap(), vec!["SET", "a", ""]);
    }
}
