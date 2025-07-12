use crate::db;
use crate::resp::Frame;

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

/// Set command delegates to db
pub async fn set(args: Vec<Frame>) -> Vec<u8> {
    db::set(args).await
}

/// Get command delegates to db
pub async fn get(args: Vec<Frame>) -> Vec<u8> {
    db::get(args).await
}

pub async fn unknown() -> Vec<u8> {
    Frame::Error("unknown command".into()).encode()
}

pub async fn error(msg: &str) -> Vec<u8> {
    Frame::Error(msg.into()).encode()
}
