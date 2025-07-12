use crate::resp::Frame;
use std::collections::HashMap;
use tokio::sync::RwLock;

pub async fn ping(_args: Vec<Frame>) -> Vec<u8> {
    Frame::SimpleString("PONG".into()).encode()
}

pub async fn echo(args: Vec<Frame>) -> Vec<u8> {
    if args.len() != 1 {
        // wrong arg count
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

pub async fn set(args: Vec<Frame>, kv: &RwLock<HashMap<String, Vec<u8>>>) -> Vec<u8> {
    if let [Frame::BulkString(Some(key)), Frame::BulkString(Some(val))] = &args[..] {
        let k = String::from_utf8_lossy(key).into_owned();
        kv.write().await.insert(k, val.clone());
        Frame::SimpleString("OK".into()).encode()
    } else {
        Frame::Error("ERR wrong number of arguments for 'set'".into()).encode()
    }
}

pub async fn get(args: Vec<Frame>, kv: &RwLock<HashMap<String, Vec<u8>>>) -> Vec<u8> {
    if let [Frame::BulkString(Some(key))] = &args[..] {
        let k = String::from_utf8_lossy(key);
        match kv.read().await.get(&*k) {
            Some(val) => Frame::BulkString(Some(val.clone())).encode(),
            None => Frame::BulkString(None).encode(),
        }
    } else {
        Frame::Error("ERR wrong number of arguments for 'get'".into()).encode()
    }
}

pub async fn unknown() -> Vec<u8> {
    Frame::Error("unknown command".into()).encode()
}

pub async fn error(msg: &str) -> Vec<u8> {
    Frame::Error(msg.into()).encode()
}
