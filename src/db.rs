use crate::model::redis_value::RedisValue;
use crate::rdb::RdbDatabase;
use crate::resp::types::Frame;
use glob::Pattern;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

pub static KV: Lazy<RwLock<HashMap<String, RedisValue>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
pub static EXP: Lazy<RwLock<HashMap<String, Instant>>> = Lazy::new(|| RwLock::new(HashMap::new()));

/// Load data from RDB file into the in-memory database
pub async fn load_from_rdb(rdb_db: RdbDatabase) -> Result<(), String> {
    let mut kv = KV.write().await;
    let mut exp = EXP.write().await;

    // Clear existing data
    kv.clear();
    exp.clear();

    let now = Instant::now();
    // Get current Unix timestamp in milliseconds
    let current_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("System time error: {}", e))?
        .as_millis() as u64;

    // Load all data from RDB
    for (key, redis_entry) in rdb_db.data {
        // Check if the key has expired
        if let Some(expiry_timestamp) = redis_entry.expiry {
            if expiry_timestamp <= current_timestamp {
                // Key has already expired, skip it
                continue;
            }

            // Calculate when the key should expire relative to tokio::time::Instant::now()
            let remaining_ms = expiry_timestamp - current_timestamp;
            let expiry_instant = now + Duration::from_millis(remaining_ms);
            exp.insert(key.clone(), expiry_instant);
        }

        // Insert the value
        kv.insert(key, redis_entry.value);
    }

    println!("Loaded {} keys from RDB file", kv.len());
    Ok(())
}

/// Set a key with optional expiration and NX/XX options
pub async fn set(
    key: Vec<u8>,
    value: Vec<u8>,
    ex: Option<u64>,
    px: Option<u64>,
    nx: bool,
    xx: bool,
) -> Result<(), String> {
    let key_str = String::from_utf8_lossy(&key).into_owned();
    let mut kv = KV.write().await;
    let mut exp = EXP.write().await;

    let exists = kv.contains_key(&key_str);
    if nx && exists {
        // NX: only set if key does not exist
        return Ok(());
    }
    if xx && !exists {
        // XX: only set if key exists
        return Ok(());
    }

    kv.insert(key_str.clone(), RedisValue::String(value));

    // Handle expiration
    if let Some(ms) = px {
        exp.insert(key_str.clone(), Instant::now() + Duration::from_millis(ms));
    } else if let Some(sec) = ex {
        exp.insert(key_str.clone(), Instant::now() + Duration::from_secs(sec));
    } else {
        exp.remove(&key_str);
    }

    Ok(())
}

/// Get a key, checking for expiration
pub async fn get(key: Vec<u8>) -> Vec<u8> {
    let k = String::from_utf8_lossy(&key);
    if let Some(expiry) = EXP.read().await.get(&*k) {
        if Instant::now() > *expiry {
            return Frame::BulkString(None).encode();
        }
    }
    match KV.read().await.get(&*k) {
        Some(val) => match val {
            RedisValue::String(s) => Frame::BulkString(Some(s.clone())).encode(),
            RedisValue::Integer(i) => Frame::Integer(*i).encode(),
            // Add more conversions as needed
            RedisValue::Float(f) => Frame::BulkString(Some(f.to_string().into_bytes())).encode(),
            RedisValue::Boolean(b) => Frame::BulkString(Some(b.to_string().into_bytes())).encode(),
            RedisValue::Null => Frame::Null.encode(),
            RedisValue::List(l) => Frame::Array(Some(
                l.iter()
                    .map(|v| Frame::BulkString(Some(v.clone())))
                    .collect(),
            ))
            .encode(),
            RedisValue::Set(s) => Frame::Array(Some(
                s.iter()
                    .map(|v| Frame::BulkString(Some(v.clone())))
                    .collect(),
            ))
            .encode(),
            RedisValue::SortedSet(ss) => Frame::Array(Some(
                ss.iter()
                    .map(|(member, score)| {
                        Frame::Array(Some(vec![
                            Frame::BulkString(Some(member.clone())),
                            Frame::BulkString(Some(score.to_string().into_bytes())),
                        ]))
                    })
                    .collect(),
            ))
            .encode(),
            RedisValue::Hash(h) => Frame::Array(Some(
                h.iter()
                    .map(|(k, v)| {
                        Frame::Array(Some(vec![
                            Frame::BulkString(Some(k.clone())),
                            Frame::BulkString(Some(v.clone())),
                        ]))
                    })
                    .collect(),
            ))
            .encode(),
            RedisValue::Zipmap(z) => Frame::BulkString(Some(z.clone())).encode(),
            RedisValue::Ziplist(z) => Frame::BulkString(Some(z.clone())).encode(),
            RedisValue::Intset(i) => Frame::BulkString(Some(i.clone())).encode(),
            RedisValue::Quicklist(q) => Frame::BulkString(Some(q.clone())).encode(),
        },
        None => Frame::BulkString(None).encode(),
    }
}

/// Get all keys matching a  glob-style pattern
pub async fn get_keys_matching_pattern(pattern: &str) -> Vec<String> {
    let kv = KV.read().await;

    // Handle the special case of "*" pattern for efficiency
    if pattern == "*" {
        return kv.keys().cloned().collect();
    }

    // Try to compile the pattern first
    let compiled_pattern = match Pattern::new(pattern) {
        Ok(p) => p,
        Err(_) => {
            // If pattern compilation fails, return empty result
            return Vec::new();
        }
    };

    kv.keys()
        .filter(|k| compiled_pattern.matches(k))
        .cloned()
        .collect()
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
