use std::error::Error;
mod commands;
mod resp;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    server::start("127.0.0.1:6379").await
}
