use crate::config;
use crate::db;
use crate::rdb;
use crate::resp::Frame;

/// Ping command just returns "PONG" as a simple string.
pub async fn ping(_args: Vec<Frame>) -> Vec<u8> {
    Frame::SimpleString("PONG".into()).encode()
}

/// Echo command returns the same bulk string passed to it
/// If the argument is not a bulk string or if the number of arguments is not 1
/// it returns an error.
pub async fn echo(args: Vec<Frame>) -> Vec<u8> {
    if args.len() != 1 {
        return Frame::Error("ERR wrong number of arguments for 'echo'".into()).encode();
    }
    match &args[0] {
        Frame::BulkString(Some(bs)) => {
            // respond with the same bulk string
            Frame::BulkString(Some(bs.clone())).encode()
        }
        _ => Frame::Error("ERR invalid argument for 'echo'".into()).encode(),
    }
}

/// Set command parses arguments and performs error checking before delegating to db::set.
/// It expects at least 2 arguments: key and value (both BulkString).
pub async fn set(args: Vec<Frame>) -> Vec<u8> {
    if args.len() < 2 {
        return Frame::Error("ERR wrong number of arguments for 'set'".into()).encode();
    }

    // Parse key
    let key = match &args[0] {
        Frame::BulkString(Some(bs)) => bs.clone(),
        _ => return Frame::Error("ERR invalid key for 'set'".into()).encode(),
    };

    // Parse value
    let value = match &args[1] {
        Frame::BulkString(Some(bs)) => bs.clone(),
        _ => return Frame::Error("ERR invalid value for 'set'".into()).encode(),
    };

    // Parse options
    let mut ex: Option<u64> = None;
    let mut px: Option<u64> = None;
    let mut nx = false;
    let mut xx = false;
    let mut i = 2;
    while i < args.len() {
        match &args[i] {
            Frame::BulkString(Some(opt)) if opt.eq_ignore_ascii_case(b"EX") => {
                if i + 1 >= args.len() {
                    return Frame::Error("ERR syntax error: EX requires seconds".into()).encode();
                }
                match &args[i + 1] {
                    Frame::BulkString(Some(sec)) => {
                        let sec_str = String::from_utf8_lossy(sec);
                        match sec_str.parse::<u64>() {
                            Ok(sec_val) if sec_val > 0 => ex = Some(sec_val),
                            _ => {
                                return Frame::Error(
                                    "ERR EX value must be a positive integer".into(),
                                )
                                .encode();
                            }
                        }
                    }
                    _ => {
                        return Frame::Error("ERR EX value must be a positive integer".into())
                            .encode();
                    }
                }
                i += 2;
            }
            Frame::BulkString(Some(opt)) if opt.eq_ignore_ascii_case(b"PX") => {
                if i + 1 >= args.len() {
                    return Frame::Error("ERR syntax error: PX requires milliseconds".into())
                        .encode();
                }
                match &args[i + 1] {
                    Frame::BulkString(Some(ms)) => {
                        let ms_str = String::from_utf8_lossy(ms);
                        match ms_str.parse::<u64>() {
                            Ok(ms_val) if ms_val > 0 => px = Some(ms_val),
                            _ => {
                                return Frame::Error(
                                    "ERR PX value must be a positive integer".into(),
                                )
                                .encode();
                            }
                        }
                    }
                    _ => {
                        return Frame::Error("ERR PX value must be a positive integer".into())
                            .encode();
                    }
                }
                i += 2;
            }
            Frame::BulkString(Some(opt)) if opt.eq_ignore_ascii_case(b"NX") => {
                nx = true;
                i += 1;
            }
            Frame::BulkString(Some(opt)) if opt.eq_ignore_ascii_case(b"XX") => {
                xx = true;
                i += 1;
            }
            _ => {
                return Frame::Error("ERR syntax error in 'set' options".into()).encode();
            }
        }
    }

    // Delegate to db::set with options
    match db::set(key, value, ex, px, nx, xx).await {
        Ok(()) => Frame::SimpleString("OK".into()).encode(),
        Err(e) => Frame::Error(format!("ERR {}", e)).encode(),
    }
}

/// Get command retrieves a value by key, checking for expiration.
/// It expects a single argument which is the key (BulkString).
pub async fn get(args: Vec<Frame>) -> Vec<u8> {
    if args.len() != 1 {
        return Frame::Error("ERR wrong number of arguments for 'get'".into()).encode();
    }
    let key = match &args[0] {
        Frame::BulkString(Some(bs)) => bs.clone(),
        _ => return Frame::Error("ERR invalid key for 'get'".into()).encode(),
    };
    db::get(key).await
}

/// KEYS command returns all the keys that match a given pattern, as a RESP array.
/// It expects the pattern as a single argument.
pub async fn keys(args: Vec<Frame>) -> Vec<u8> {
    // Debug logging - check what we're actually getting
    println!("DEBUG: args.len() = {}", args.len());
    if args.len() > 0 {
        println!("DEBUG: args[0] = {:?}", args[0]);
    }
    if args.len() > 1 {
        println!("DEBUG: args[1] = {:?}", args[1]);
    }

    // Handle both cases: quoted pattern (len=1) and shell-expanded pattern (len>1)
    if args.len() == 0 {
        return Frame::Error("ERR wrong number of arguments for 'keys' command".into()).encode();
    }

    let keys = if args.len() == 1 {
        // Normal case: KEYS "*" - we have a pattern to match against
        let pattern = match &args[0] {
            Frame::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
            Frame::BulkString(None) => {
                return Frame::Error("ERR invalid pattern for 'keys' command".into()).encode()
            }
            Frame::SimpleString(s) => s.clone(),
            _ => {
                return Frame::Error("ERR invalid pattern type for 'keys' command".into()).encode()
            }
        };
        db::get_keys_matching_pattern(&pattern).await
    } else {
        // Shell-expanded case: KEYS * - parser already expanded the glob
        // Convert the Frame arguments back to strings
        args.into_iter()
            .filter_map(|frame| match frame {
                Frame::BulkString(Some(bs)) => Some(String::from_utf8_lossy(&bs).to_string()),
                Frame::SimpleString(s) => Some(s),
                _ => None,
            })
            .collect()
    };

    let resp = Frame::Array(Some(
        keys.into_iter()
            .map(|k| Frame::BulkString(Some(k.into_bytes())))
            .collect(),
    ));
    resp.encode()
}

/// SAVE command synchronously saves the dataset to disk.
/// It expects no arguments.
pub async fn save(args: Vec<Frame>) -> Vec<u8> {
    if !args.is_empty() {
        return Frame::Error("ERR wrong number of arguments for 'save'".into()).encode();
    }

    match rdb::save().await {
        Ok(()) => Frame::SimpleString("OK".into()).encode(),
        Err(e) => Frame::Error(format!("ERR {}", e)).encode(),
    }
}

/// CONFIG GET command returns config values as RESP array
/// It expects a single argument which is the parameter name.
pub async fn config_get(args: Vec<Frame>) -> Vec<u8> {
    if args.len() != 1 {
        return Frame::Error("ERR wrong number of arguments for 'config get'".into()).encode();
    }
    let param = match &args[0] {
        Frame::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_lowercase(),
        _ => return Frame::Error("ERR invalid argument for 'config get'".into()).encode(),
    };
    let config = config::get_config();
    let value = match param.as_str() {
        "dir" => config.dir.to_string_lossy().to_string(),
        "dbfilename" => config.dbfilename,
        _ => String::new(),
    };
    let resp = Frame::Array(Some(vec![
        Frame::BulkString(Some(param.into_bytes())),
        Frame::BulkString(Some(value.into_bytes())),
    ]));
    resp.encode()
}

/// CONFIG SET command allows setting configuration parameters
/// It expects two arguments: the parameter name and the value.
pub async fn config_set(args: Vec<Frame>) -> Vec<u8> {
    if args.len() != 2 {
        return Frame::Error("ERR wrong number of arguments for 'config set'".into()).encode();
    }
    let param = match &args[0] {
        Frame::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_lowercase(),
        _ => return Frame::Error("ERR invalid argument for 'config set'".into()).encode(),
    };
    let value = match &args[1] {
        Frame::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
        _ => return Frame::Error("ERR invalid value for 'config set'".into()).encode(),
    };

    match param.as_str() {
        "dir" => config::set_dir(value),
        "dbfilename" => config::set_dbfilename(value),
        _ => return Frame::Error("ERR unknown configuration parameter".into()).encode(),
    }

    Frame::SimpleString("OK".into()).encode()
}

pub async fn unknown() -> Vec<u8> {
    Frame::Error("unknown command".into()).encode()
}

pub async fn error(msg: &str) -> Vec<u8> {
    Frame::Error(msg.into()).encode()
}
