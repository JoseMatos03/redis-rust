#![allow(unused_imports)]
use std::io::{self, Read, Write};
use std::net::TcpListener;

fn main() {
    let listener: TcpListener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("New connection established!");
                let mut buffer = [0; 1024];
                loop  {
                    let bytes_read = stream.read(&mut buffer).unwrap();
                    if bytes_read == 0 {
                        println!("Connection closed by client.");
                        break;
                    }

                    stream.write(b"+PONG\r\n").unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
