use lazy_static::lazy_static;
use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub dir: PathBuf,
    pub dbfilename: String,
}

impl Default for Config {
    fn default() -> Self {
        let dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let dbfilename = "dump.rdb".to_string();
        Config { dir, dbfilename }
    }
}

lazy_static! {
    static ref CONFIG: std::sync::RwLock<Config> = std::sync::RwLock::new(Config::default());
}

pub fn get_config() -> Config {
    CONFIG.read().unwrap().clone()
}

pub fn set_dir<P: Into<PathBuf>>(path: P) {
    let mut config = CONFIG.write().unwrap();
    config.dir = path.into();
}

pub fn set_dbfilename<S: Into<String>>(filename: S) {
    let mut config = CONFIG.write().unwrap();
    config.dbfilename = filename.into();
}

pub fn parse_args_and_set_config() {
    let args: Vec<String> = env::args().collect();
    for i in 1..args.len() {
        match args[i].as_str() {
            "--dir" => {
                if i + 1 < args.len() {
                    set_dir(&args[i + 1]);
                } else {
                    eprintln!("Error: --dir requires a path argument");
                }
            }
            "--dbfilename" => {
                if i + 1 < args.len() {
                    set_dbfilename(&args[i + 1]);
                } else {
                    eprintln!("Error: --dbfilename requires a filename argument");
                }
            }
            _ => {}
        }
    }
}
