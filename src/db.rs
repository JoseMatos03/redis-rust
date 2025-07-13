use crate::resp::Frame;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

static KV: Lazy<RwLock<HashMap<String, Vec<u8>>>> = Lazy::new(|| RwLock::new(HashMap::new()));
static EXP: Lazy<RwLock<HashMap<String, Instant>>> = Lazy::new(|| RwLock::new(HashMap::new()));

/// Set a key with optional expiration and NX/XX options
pub async fn set(
    key: Vec<u8>,
    value: Vec<u8>,
    ex: Option<u64>,
    px: Option<u64>,
    nx: bool,
    xx: bool,
) -> Result<Option<Vec<u8>>, String> {
    let key_str = String::from_utf8_lossy(&key).into_owned();
    let mut kv = KV.write().await;
    let mut exp = EXP.write().await;

    let exists = kv.contains_key(&key_str);
    if nx && exists {
        // NX: only set if key does not exist
        return Ok(None);
    }
    if xx && !exists {
        // XX: only set if key exists
        return Ok(None);
    }

    kv.insert(key_str.clone(), value);

    // Handle expiration
    if let Some(ms) = px {
        exp.insert(key_str.clone(), Instant::now() + Duration::from_millis(ms));
    } else if let Some(sec) = ex {
        exp.insert(key_str.clone(), Instant::now() + Duration::from_secs(sec));
    } else {
        exp.remove(&key_str);
    }

    Ok(Some(Frame::SimpleString("OK".into()).encode()))
}

/// Get a key, checking for expiration
pub async fn get(key: Vec<u8>) -> Vec<u8> {
    let k = String::from_utf8_lossy(&key);
    if let Some(expiry) = EXP.read().await.get(&*k) {
        if Instant::now() > *expiry {
            // Do not remove here, just return null bulk string
            // It will be purged later by the server background task
            return Frame::BulkString(None).encode();
        }
    }
    match KV.read().await.get(&*k) {
        Some(val) => Frame::BulkString(Some(val.clone())).encode(),
        None => Frame::BulkString(None).encode(),
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
