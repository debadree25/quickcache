use anyhow::anyhow;

#[derive(Debug, PartialEq)]
pub enum RedisValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Option<Vec<RedisValue>>),
    Boolean(bool),
    Null,
}

impl RedisValue {
    pub fn to_resp_string(&self) -> String {
        match self {
            RedisValue::SimpleString(s) => format!("+{}\r\n", s),
            RedisValue::Error(e) => format!("-{}\r\n", e),
            RedisValue::Integer(i) => format!(":{}\r\n", i),
            RedisValue::BulkString(Some(b)) => format!("${}\r\n{}\r\n", b.len(), b),
            RedisValue::BulkString(None) => "$-1\r\n".to_string(),
            RedisValue::Array(Some(a)) => {
                let mut result = format!("*{}\r\n", a.len());
                for v in a {
                    result.push_str(&&v.to_resp_string());
                }
                result
            }
            RedisValue::Array(None) => "*-1\r\n".to_string(),
            RedisValue::Boolean(b) => format!("#{}\r\n", if *b { "t" } else { "f" }),
            RedisValue::Null => "_\r\n".to_string(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum RedisCommand {
    PING(RedisValue),
    ECHO(RedisValue),
    SET(RedisValue, RedisValue, Option<u64>),
    GET(RedisValue),
    CONFIG,
    COMMAND,
}

fn pick_simple_string(
    iter: &mut std::iter::Peekable<std::slice::Iter<u8>>,
) -> Result<RedisValue, anyhow::Error> {
    iter.next();
    let mut result = Vec::new();
    while let Some(&&byte) = iter.peek() {
        if byte == b'\r' {
            iter.next();
            iter.next();
            break;
        }
        result.push(byte);
        iter.next();
    }
    Ok(RedisValue::SimpleString(String::from_utf8(result)?))
}

fn pick_error(
    iter: &mut std::iter::Peekable<std::slice::Iter<u8>>,
) -> Result<RedisValue, anyhow::Error> {
    iter.next();
    let mut result = Vec::new();
    while let Some(&&byte) = iter.peek() {
        if byte == b'\r' {
            iter.next();
            iter.next();
            break;
        }
        result.push(byte);
        iter.next();
    }
    Ok(RedisValue::Error(String::from_utf8(result)?))
}

fn pick_integer(
    iter: &mut std::iter::Peekable<std::slice::Iter<u8>>,
) -> Result<RedisValue, anyhow::Error> {
    iter.next();
    let mut result = Vec::new();
    while let Some(&&byte) = iter.peek() {
        if byte == b'\r' {
            iter.next();
            iter.next();
            break;
        }
        result.push(byte);
        iter.next();
    }
    Ok(RedisValue::Integer(String::from_utf8(result)?.parse()?))
}

fn pick_bulk_string(
    iter: &mut std::iter::Peekable<std::slice::Iter<u8>>,
) -> Result<RedisValue, anyhow::Error> {
    iter.next();
    let mut result = Vec::new();
    while let Some(&&byte) = iter.peek() {
        if byte == b'\r' {
            iter.next();
            iter.next();
            break;
        }
        result.push(byte);
        iter.next();
    }
    let len = String::from_utf8(result)?.parse()?;
    if len == -1 {
        return Ok(RedisValue::BulkString(None));
    }
    let mut result = Vec::new();
    for _ in 0..len {
        result.push(*iter.next().ok_or(anyhow!("Unexpected end of input"))?);
    }
    iter.next();
    iter.next();
    Ok(RedisValue::BulkString(Some(String::from_utf8(result)?)))
}

fn pick_array(
    iter: &mut std::iter::Peekable<std::slice::Iter<u8>>,
) -> Result<RedisValue, anyhow::Error> {
    iter.next();
    let mut result = Vec::new();
    while let Some(&&byte) = iter.peek() {
        if byte == b'\r' {
            iter.next();
            iter.next();
            break;
        }
        result.push(byte);
        iter.next();
    }
    let len = String::from_utf8(result)?.parse::<i64>()?;
    if len == -1 {
        return Ok(RedisValue::Array(None));
    }
    let mut array = Vec::with_capacity(len as usize);
    while let Some(&&array_byte) = iter.peek() {
        match array_byte {
            b'+' => {
                array.push(pick_simple_string(iter)?);
            }
            b'-' => {
                array.push(pick_error(iter)?);
            }
            b':' => {
                array.push(pick_integer(iter)?);
            }
            b'$' => {
                array.push(pick_bulk_string(iter)?);
            }
            b'*' => {
                array.push(pick_array(iter)?);
            }
            b'\r' => {
                iter.next();
                iter.next();
                break;
            }
            _ => {
                return Err(anyhow!("Unexpected byte in array"));
            }
        }
    }
    Ok(RedisValue::Array(Some(array)))
}

fn pick_boolean(
    iter: &mut std::iter::Peekable<std::slice::Iter<u8>>,
) -> Result<RedisValue, anyhow::Error> {
    iter.next();
    let mut result = Vec::new();
    while let Some(&&byte) = iter.peek() {
        if byte == b'\r' {
            iter.next();
            iter.next();
            break;
        }
        result.push(byte);
        iter.next();
    }
    match String::from_utf8(result)?.as_str() {
        "t" => Ok(RedisValue::Boolean(true)),
        "f" => Ok(RedisValue::Boolean(false)),
        _ => Err(anyhow!("Unexpected byte in boolean")),
    }
}

pub fn parse_resp(buffer: &[u8]) -> Result<Option<RedisValue>, anyhow::Error> {
    let mut iter = buffer.iter().peekable();
    while let Some(&&byte) = iter.peek() {
        match byte {
            b'+' => {
                return Ok(Some(pick_simple_string(&mut iter)?));
            }
            b'-' => {
                return Ok(Some(pick_error(&mut iter)?));
            }
            b':' => {
                return Ok(Some(pick_integer(&mut iter)?));
            }
            b'$' => {
                return Ok(Some(pick_bulk_string(&mut iter)?));
            }
            b'*' => {
                return Ok(Some(pick_array(&mut iter)?));
            }
            b'#' => {
                return Ok(Some(pick_boolean(&mut iter)?));
            }
            b'_' => {
                iter.next();
                iter.next();
                iter.next();
                return Ok(Some(RedisValue::Null));
            }
            b'P' => {
                // Pre resp PING_INLINE
                let mut result = Vec::new();
                for _ in 0..6 {
                    result.push(*iter.next().ok_or(anyhow!("Unexpected end of input"))?);
                }
                let parsed = String::from_utf8(result)?;
                if parsed == "PING\r\n" {
                    return Ok(Some(RedisValue::Array(Some(vec![RedisValue::SimpleString(
                        "PING".to_string(),
                    )]))));
                }
            }
            _ => {
                return Ok(None);
            }
        }
    }
    Ok(None)
}

pub fn extract_commands(buffer: &[u8]) -> Result<RedisCommand, anyhow::Error> {
    let parsed = parse_resp(buffer)?;
    match parsed {
        Some(RedisValue::Array(Some(array))) => {
            let command = &array[0];
            let args = &array[1..];
            match command {
                RedisValue::SimpleString(s) | RedisValue::BulkString(Some(s)) => {
                    match s.to_uppercase().as_str() {
                        "PING" => {
                            if args.len() > 1 {
                                return Err(anyhow!("Invalid number of arguments for PING"));
                            }
                            let message = if args.len() == 1 {
                                match &args[0] {
                                    RedisValue::SimpleString(s)
                                    | RedisValue::BulkString(Some(s)) => {
                                        RedisValue::BulkString(Some(s.clone()))
                                    }
                                    _ => return Err(anyhow!("Invalid argument for PING")),
                                }
                            } else {
                                RedisValue::SimpleString("PONG".to_string())
                            };
                            Ok(RedisCommand::PING(message))
                        }
                        "ECHO" => {
                            if args.len() != 1 {
                                return Err(anyhow!("Invalid number of arguments for ECHO"));
                            }
                            let message = match &args[0] {
                                RedisValue::SimpleString(s)
                                | RedisValue::BulkString(Some(s)) => RedisValue::BulkString(Some(s.clone())),
                                _ => return Err(anyhow!("Invalid argument for ECHO")),
                            };
                            Ok(RedisCommand::ECHO(message))
                        }
                        "GET" => {
                            if args.len() != 1 {
                                return Err(anyhow!("Invalid number of arguments for GET"));
                            }
                            let key = match &args[0] {
                                RedisValue::SimpleString(s)
                                | RedisValue::BulkString(Some(s)) => RedisValue::BulkString(Some(s.clone())),
                                _ => return Err(anyhow!("Invalid argument for GET")),
                            };
                            Ok(RedisCommand::GET(key))
                        }
                        "SET" => {
                            if args.len() < 2 {
                                return Err(anyhow!("Invalid number of arguments for SET"));
                            }
                            let key = match &args[0] {
                                RedisValue::SimpleString(s)
                                | RedisValue::BulkString(Some(s)) => RedisValue::BulkString(Some(s.clone())),
                                _ => return Err(anyhow!("Invalid argument for SET")),
                            };
                            let value = match &args[1] {
                                RedisValue::SimpleString(s)
                                | RedisValue::BulkString(Some(s)) => RedisValue::BulkString(Some(s.clone())),
                                _ => return Err(anyhow!("Invalid argument for SET")),
                            };
                            let additional_args = &mut args[2..].iter();
                            let mut expiry = None;
                            while let Some(arg) = additional_args.next() {
                              match arg {
                                  RedisValue::SimpleString(s) | RedisValue::BulkString(Some(s)) => {
                                    match s.to_uppercase().as_str() {
                                        "EX" => {
                                            let arg = additional_args.next().ok_or(anyhow!("Invalid number of arguments for SET"))?;
                                            match arg {
                                                RedisValue::SimpleString(s) | RedisValue::BulkString(Some(s)) => {
                                                    expiry = Some(s.parse::<u64>()? * 1000);
                                                }
                                                _ => return Err(anyhow!("Invalid argument for SET")),
                                            }
                                        }
                                        "PX" => {
                                            let arg = additional_args.next().ok_or(anyhow!("Invalid number of arguments for SET"))?;
                                            match arg {
                                                RedisValue::SimpleString(s) | RedisValue::BulkString(Some(s)) => {
                                                    expiry = Some(s.parse::<u64>()?);
                                                }
                                                _ => return Err(anyhow!("Invalid argument for SET")),
                                            }
                                        }
                                        _ => return Err(anyhow!("Invalid argument for SET")),
                                    }
                                  }
                                  _ => return Err(anyhow!("Invalid argument for SET")),
                              }
                            }
                            Ok(RedisCommand::SET(key, value, expiry))
                        }
                        "CONFIG" => Ok(RedisCommand::CONFIG),
                        "COMMAND" => Ok(RedisCommand::COMMAND),
                        _ => Err(anyhow!("Unknown command")),
                    }
                }
                _ => {
                    Err(anyhow!("Invalid command in matching"))
                }
            }
        }
        None => Err(anyhow!("Invalid command parsed a None")),
        _ => Err(anyhow!("Invalid command couldnt match")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_parse_resp(input: &[u8], expected: Option<RedisValue>) {
        assert_eq!(parse_resp(input).unwrap(), expected);
    }

    #[test]
    fn test_parse_resp_simple_string() {
        test_parse_resp(b"+OK\r\n", Some(RedisValue::SimpleString("OK".to_string())));
    }

    #[test]
    fn test_parse_resp_error() {
        test_parse_resp(
            b"-ERR unknown command 'foobar'\r\n",
            Some(RedisValue::Error(
                "ERR unknown command 'foobar'".to_string(),
            )),
        );
    }

    #[test]
    fn test_parse_resp_integer() {
        test_parse_resp(b":1000\r\n", Some(RedisValue::Integer(1000)));
    }

    #[test]
    fn test_parse_resp_bulk_string() {
        test_parse_resp(
            b"$6\r\nfoobar\r\n",
            Some(RedisValue::BulkString(Some("foobar".to_string()))),
        );
        test_parse_resp(
            b"$0\r\n\r\n",
            Some(RedisValue::BulkString(Some("".to_string()))),
        );
        test_parse_resp(b"$-1\r\n", Some(RedisValue::BulkString(None)));
    }

    #[test]
    fn test_parse_resp_array() {
        test_parse_resp(
            b"*2\r\n+Foo\r\n-Bar\r\n",
            Some(RedisValue::Array(Some(vec![
                RedisValue::SimpleString("Foo".to_string()),
                RedisValue::Error("Bar".to_string()),
            ]))),
        );
        test_parse_resp(
            b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
            Some(RedisValue::Array(Some(vec![
                RedisValue::BulkString(Some("hello".to_string())),
                RedisValue::BulkString(Some("world".to_string())),
            ]))),
        );
        test_parse_resp(b"*-1\r\n", Some(RedisValue::Array(None)));
        test_parse_resp(
            b"*3\r\n:1\r\n:2\r\n:3\r\n",
            Some(RedisValue::Array(Some(vec![
                RedisValue::Integer(1),
                RedisValue::Integer(2),
                RedisValue::Integer(3),
            ]))),
        );
        test_parse_resp(
            b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n",
            Some(RedisValue::Array(Some(vec![
                RedisValue::Integer(1),
                RedisValue::Integer(2),
                RedisValue::Integer(3),
                RedisValue::Integer(4),
                RedisValue::BulkString(Some("hello".to_string())),
            ]))),
        );
        test_parse_resp(
            b"*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n",
            Some(RedisValue::Array(Some(vec![
                RedisValue::BulkString(Some("hello".to_string())),
                RedisValue::BulkString(None),
                RedisValue::BulkString(Some("world".to_string())),
            ]))),
        );
        test_parse_resp(
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
            Some(RedisValue::Array(Some(vec![
                RedisValue::BulkString(Some("SET".to_string())),
                RedisValue::BulkString(Some("key".to_string())),
                RedisValue::BulkString(Some("value".to_string())),
            ]))),
        );
        test_parse_resp(
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$-1\r\n",
            Some(RedisValue::Array(Some(vec![
                RedisValue::BulkString(Some("SET".to_string())),
                RedisValue::BulkString(Some("key".to_string())),
                RedisValue::BulkString(None),
            ]))),
        );
        test_parse_resp(
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            Some(RedisValue::Array(Some(vec![
                RedisValue::BulkString(Some("GET".to_string())),
                RedisValue::BulkString(Some("key".to_string())),
            ]))),
        );
    }

    #[test]
    fn test_parse_resp_boolean() {
        test_parse_resp(b"#t\r\n", Some(RedisValue::Boolean(true)));
        test_parse_resp(b"#f\r\n", Some(RedisValue::Boolean(false)));
    }

    #[test]
    fn test_parse_resp_null() {
        test_parse_resp(b"_\r\n", Some(RedisValue::Null));
    }

    #[test]
    fn test_parse_pre_resp_ping() {
        test_parse_resp(b"PING\r\n", Some(RedisValue::Array(Some(vec![RedisValue::SimpleString(
            "PING".to_string(),
        )]))));
    }

    fn test_extract_commands(input: &[u8], expected: RedisCommand) {
        assert_eq!(extract_commands(input).unwrap(), expected);
    }

    #[test]
    fn test_extract_commands_ping() {
        test_extract_commands(b"*1\r\n$4\r\nPING\r\n", RedisCommand::PING(RedisValue::SimpleString("PONG".to_string())));
        test_extract_commands(b"*2\r\n$4\r\nPING\r\n$4\r\nPING\r\n", RedisCommand::PING(RedisValue::BulkString(Some("PING".to_string()))));
    }

    #[test]
    fn test_extract_commands_echo() {
        test_extract_commands(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n", RedisCommand::ECHO(RedisValue::BulkString(Some("hello".to_string()))));
    }

    #[test]
    fn test_extract_commands_get() {
        test_extract_commands(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", RedisCommand::GET(RedisValue::BulkString(Some("key".to_string()))));
    }

    #[test]
    fn test_extract_commands_set() {
        test_extract_commands(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", RedisCommand::SET(RedisValue::BulkString(Some("key".to_string())), RedisValue::BulkString(Some("value".to_string())), None));
        test_extract_commands(b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n5\r\n", RedisCommand::SET(RedisValue::BulkString(Some("key".to_string())), RedisValue::BulkString(Some("value".to_string())), Some(5000)));
        test_extract_commands(b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n100\r\n", RedisCommand::SET(RedisValue::BulkString(Some("key".to_string())), RedisValue::BulkString(Some("value".to_string())), Some(100)));
    }
}
