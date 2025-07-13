use std::error::Error;
mod commands;
mod config;
mod db;
mod resp;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    config::parse_args_and_set_config();
    println!(
        "Starting Redis server with config: {:?}",
        config::get_config()
    );
    tokio::spawn(async {
        // spawn background purging task
        loop {
            db::purge_expired_keys().await;
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        }
    });
    server::start("127.0.0.1:6379").await
}
