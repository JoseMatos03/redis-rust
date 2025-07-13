use std::error::Error;
mod commands;
mod config;
mod db;
mod model;
mod rdb;
mod resp;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    config::parse_args_and_set_config();

    if let Err(e) = load_rdb_file().await {
        eprintln!("Warning: Failed to load RDB file: {}", e);
        // Continue running even if RDB loading fails
    }

    tokio::spawn(async {
        // spawn background purging task
        loop {
            db::purge_expired_keys().await;
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        }
    });

    server::start("127.0.0.1:6379").await
}

async fn load_rdb_file() -> Result<(), Box<dyn Error>> {
    let rdb_path = config::get_dir().join(config::get_dbfilename());

    if !std::path::Path::new(&rdb_path).exists() {
        println!(
            "No RDB file found at {}, starting with empty database",
            rdb_path.display()
        );
        return Ok(());
    }

    println!("Loading RDB file from: {}", rdb_path.display());

    // Parse the RDB file
    let rdb_db = rdb::RdbParser::load(&rdb_path)?;
    let keys_count = rdb_db.data.len();

    // Load the data into your in-memory database
    db::load_from_rdb(rdb_db).await?;

    println!("Successfully loaded {} keys from RDB file", keys_count);

    Ok(())
}
