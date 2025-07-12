use std::error::Error;
mod commands;
mod db;
mod resp;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tokio::spawn(async {
        // spawn background purging task
        loop {
            db::purge_expired_keys().await;
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        }
    });

    server::start("127.0.0.1:6379").await
}
