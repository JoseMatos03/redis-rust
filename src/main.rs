use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

fn main() -> std::io::Result<()> {
    let listener: TcpListener = TcpListener::bind("127.0.0.1:6379").unwrap();
    listener.set_nonblocking(true)?;
    println!("Listening on 127.0.0.1:6379");

    let mut clients: Vec<TcpStream> = Vec::new();
    
    loop {
        // accept new clients
        match listener.accept() {
            Ok((sock, addr)) => {
                println!("New client connected: {}", addr);
                sock.set_nonblocking(true)?;
                clients.push(sock);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No new clients, continue
            }
            Err(e) => {
                eprintln!("Error accepting client: {}", e);
                continue;
            }
        }

        // handle existing clients
        let mut i = 0;
        while i < clients.len() {
            let mut buf = [0u8; 1024];
            let client = &mut clients[i];

            // read data from the client
            match client.read(&mut buf) {
                Ok(0) => { // Client disconnected
                    println!("Client disconnected: {}", client.peer_addr().unwrap());
                    clients.swap_remove(i);
                    continue;
                }
                Ok(n) => { // Client sent data
                    let data = &buf[..n];

                    if data.starts_with(b"PING") {
                        println!("Received PING command from client: {}", client.peer_addr().unwrap());
                        
                        if let Err(e) = client.write_all(b"+PONG\r\n") {
                            eprintln!("Error writing to client: {}", e);
                            clients.swap_remove(i);
                            continue;
                        }
                    } else if data.starts_with(b"ECHO") {
                        println!("Received ECHO command from client: {}", client.peer_addr().unwrap());
                        let echo_content = data[4..].iter() // Remove "ECHO"
                            .skip_while(|&&b| b == b' ' || b == b'\r' || b == b'\n') // Skip leading whitespace
                            .cloned()
                            .collect::<Vec<u8>>();
                        let response = format!("${}\r\n{}\r\n", echo_content.len(), String::from_utf8_lossy(&echo_content));

                        if let Err(e) = client.write_all(response.as_bytes()) {
                            eprintln!("Error writing to client: {}", e);
                            clients.swap_remove(i);
                            continue;
                        }
                    } else {
                        println!("Received unknown command from client: {}", client.peer_addr().unwrap());
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No data available
                }
                Err(e) => {
                    eprintln!("Error reading from client: {}", e);
                    clients.swap_remove(i);
                    continue;
                }
            }

            i += 1; // Move to the next client
        }

        // Sleep briefly to avoid busy waiting
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}
