use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

fn main() -> std::io::Result<()> {
    let listener: TcpListener = TcpListener::bind("127.0.0.1:6379").unwrap();
    listener.set_nonblocking(true)?;
    println!("Listening on 127.0.0.1:6379");

    // store clients in a vector
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
                Ok(0) => {
                    // Client disconnected
                    println!("Client disconnected: {}", client.peer_addr().unwrap());
                    clients.swap_remove(i);
                    continue;
                }
                Ok(n) => {
                    // Process the data received from the client
                    println!("Received {} bytes from client: {}", n, client.peer_addr().unwrap());

                    // Echo back the data to the client
                    if let Err(e) = client.write_all(b"+PONG\r\n") {
                        eprintln!("write err: {}", e);
                        continue;
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
