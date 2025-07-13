use crate::resp::Frame;
mod default;

/// Dispatch function to handle commands based on the RESP protocol.
/// It expects a command in the form of an array where the first element is the command name.
pub async fn dispatch(frame: Frame) -> Vec<u8> {
    match frame {
        Frame::Array(Some(mut v)) if !v.is_empty() => {
            if let Frame::BulkString(Some(cmd)) = v.remove(0) {
                let cmd_str = String::from_utf8_lossy(&cmd).to_lowercase();

                match cmd_str.as_str() {
                    "ping" => default::ping(v).await,
                    "echo" => default::echo(v).await,
                    "set" => default::set(v).await,
                    "get" => default::get(v).await,
                    "config" => {
                        if v.is_empty() {
                            return default::error("ERR wrong number of arguments for 'config'")
                                .await;
                        }
                        // First argument is subcommand (e.g., GET)
                        if let Frame::BulkString(Some(subcmd)) = v.remove(0) {
                            let subcmd_str = String::from_utf8_lossy(&subcmd).to_lowercase();
                            match subcmd_str.as_str() {
                                "get" => default::config_get(v).await,
                                "set" => default::config_set(v).await,
                                _ => default::error("ERR unknown subcommand for 'config'").await,
                            }
                        } else {
                            default::error("ERR invalid subcommand for 'config'").await
                        }
                    }
                    _ => default::unknown().await,
                }
            } else {
                default::error("Protocol error: invalid command").await
            }
        }
        _ => default::error("Protocol error: expected array").await,
    }
}
