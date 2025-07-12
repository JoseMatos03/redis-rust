use crate::resp::Frame;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

static KV: Lazy<RwLock<HashMap<String, Vec<u8>>>> = Lazy::new(|| RwLock::new(HashMap::new()));
static EXP: Lazy<RwLock<HashMap<String, Instant>>> = Lazy::new(|| RwLock::new(HashMap::new()));

/// Set a key with optional PX expiration
pub async fn set(args: Vec<Frame>) -> Vec<u8> {
    if args.len() < 2 {
        return Frame::Error("ERR wrong number of arguments for 'set'".into()).encode();
    }
    let key = match &args[0] {
        Frame::BulkString(Some(k)) => String::from_utf8_lossy(k).into_owned(),
        _ => return Frame::Error("ERR invalid key for 'set'".into()).encode(),
    };
    let val = match &args[1] {
        Frame::BulkString(Some(v)) => v.clone(),
        _ => return Frame::Error("ERR invalid value for 'set'".into()).encode(),
    };

    let mut px: Option<u64> = None;
    let mut i = 2;
    while i < args.len() {
        match &args[i] {
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
            _ => {
                return Frame::Error("ERR syntax error in 'set' options".into()).encode();
            }
        }
    }

    KV.write().await.insert(key.clone(), val);
    if let Some(ms) = px {
        EXP.write()
            .await
            .insert(key.clone(), Instant::now() + Duration::from_millis(ms));
    } else {
        EXP.write().await.remove(&key);
    }
    Frame::SimpleString("OK".into()).encode()
}

/// Get a key, checking for expiration
pub async fn get(args: Vec<Frame>) -> Vec<u8> {
    if let [Frame::BulkString(Some(key))] = &args[..] {
        let k = String::from_utf8_lossy(key);
        if let Some(expiry) = EXP.read().await.get(&*k) {
            if Instant::now() > *expiry {
                // Do not remove here, just return null bulk string
                return Frame::BulkString(None).encode();
            }
        }
        match KV.read().await.get(&*k) {
            Some(val) => Frame::BulkString(Some(val.clone())).encode(),
            None => Frame::BulkString(None).encode(),
        }
    } else {
        Frame::Error("ERR wrong number of arguments for 'get'".into()).encode()
    }
}

/// Purge expired keys from KV and EXP
pub async fn purge_expired_keys() {
    let now = Instant::now();
    let mut exp = EXP.write().await;
    let mut kv = KV.write().await;
    let expired_keys: Vec<String> = exp
        .iter()
        .filter_map(|(k, &v)| if now > v { Some(k.clone()) } else { None })
        .collect();
    for k in expired_keys {
        exp.remove(&k);
        kv.remove(&k);
    }
}
