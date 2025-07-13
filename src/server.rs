use crate::commands::dispatch;
use std::error::Error;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Start the Redis server on the specified address
/// This function listens for incoming connections and spawns a handler for each client.
pub async fn start(addr: &str) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on {}", addr);
    loop {
        let (socket, peer) = listener.accept().await?;
        println!("New client: {}", peer);
        tokio::spawn(handle(socket, peer));
    }
}

/// Handle a single client connection
/// This function reads commands from the client, processes them, and sends responses back.
/// It runs in its own task to allow multiple clients to be handled concurrently.
async fn handle(mut socket: TcpStream, peer: SocketAddr) {
    use crate::resp::parser::FrameParser;
    let mut parser = FrameParser::new();
    let mut buf = [0u8; 1024];

    loop {
        match socket.read(&mut buf).await {
            Ok(0) => {
                println!("Client {} disconnected", peer);
                return;
            }
            Ok(n) => {
                parser.feed(&buf[..n]);
                while let Some(frame) = parser.parse().unwrap() {
                    // Process command frame
                    let response = dispatch(frame).await;
                    if let Err(e) = socket.write_all(&response).await {
                        eprintln!("Write error {}: {}", peer, e);
                        return;
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                println!("Client {} disconnected", peer);
                return;
            }
            Err(e) => {
                eprintln!("Unexpected read error {}: {}", peer, e);
                return;
            }
        }
    }
}
