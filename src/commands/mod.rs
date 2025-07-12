use crate::resp::Frame;
mod default;

pub async fn dispatch(frame: Frame) -> Vec<u8> {
    match frame {
        Frame::Array(Some(mut v)) if !v.is_empty() => {
            if let Frame::BulkString(Some(cmd)) = v.remove(0) {
                let cmd_str = String::from_utf8_lossy(&cmd).to_lowercase();

                match cmd_str.as_str() {
                    "ping" => default::ping(v).await,
                    "echo" => default::echo(v).await,
                    _ => default::unknown().await,
                }
            } else {
                default::error("Protocol error: invalid command").await
            }
        }
        _ => default::error("Protocol error: expected array").await,
    }
}
