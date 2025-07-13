use crate::model::redis_value::RedisValue;
use crate::{config, db};
use crc64::crc64;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::Path;
use tokio::time::Instant;

#[derive(Debug)]
pub struct RedisEntry {
    pub value: RedisValue,
    pub expiry: Option<u64>, // Unix timestamp in milliseconds
}

#[derive(Debug)]
pub struct RdbDatabase {
    pub data: HashMap<String, RedisEntry>,
}

pub struct RdbParser;

impl RdbParser {
    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<RdbDatabase> {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(_) => {
                return Ok(RdbDatabase {
                    data: HashMap::new(),
                })
            }
        };
        let mut reader = BufReader::new(file);
        Self::parse(&mut reader)
    }

    fn parse<R: Read>(reader: &mut R) -> io::Result<RdbDatabase> {
        let mut magic = [0u8; 5];
        reader.read_exact(&mut magic)?;
        if &magic != b"REDIS" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid RDB magic string",
            ));
        }

        let mut version = [0u8; 4];
        reader.read_exact(&mut version)?;
        if version != [0, 0, 0, 11] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported RDB version",
            ));
        }

        let mut data = HashMap::new();
        let mut buf = [0u8; 1];
        let mut current_expiry: Option<u64> = None;

        // For checksum, buffer all bytes except the last 8
        let mut file_bytes: Vec<u8> = Vec::new();
        file_bytes.extend_from_slice(&magic);
        file_bytes.extend_from_slice(&version);

        loop {
            match reader.read_exact(&mut buf) {
                Ok(_) => {
                    file_bytes.push(buf[0]);
                    let opcode = buf[0];
                    match opcode {
                        0xFA => {
                            let _key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let _value = read_length_prefixed_string(reader, &mut file_bytes)?;
                            // Metadata - don't reset expiry
                        }
                        0xFB => {
                            let _ht_size = read_rdb_length(reader, &mut file_bytes)?;
                            let _expire_ht_size = read_rdb_length(reader, &mut file_bytes)?;
                            // Resize hint - don't reset expiry
                        }
                        0xFE => {
                            let _db_number = read_rdb_length(reader, &mut file_bytes)?;
                            // Database selector - don't reset expiry
                        }
                        0xFD => {
                            // Expiry in seconds
                            let mut expiry_buf = [0u8; 4];
                            reader.read_exact(&mut expiry_buf)?;
                            file_bytes.extend_from_slice(&expiry_buf);
                            let expiry_seconds = u32::from_le_bytes(expiry_buf) as u64;
                            current_expiry = Some(expiry_seconds * 1000); // Convert to milliseconds
                        }
                        0xFC => {
                            // Expiry in milliseconds
                            let mut expiry_buf = [0u8; 8];
                            reader.read_exact(&mut expiry_buf)?;
                            file_bytes.extend_from_slice(&expiry_buf);
                            current_expiry = Some(u64::from_le_bytes(expiry_buf));
                        }
                        0xFF => {
                            // End of RDB file
                            break;
                        }
                        // Value types
                        0x00 => {
                            // String Encoding
                            let key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let value = read_length_prefixed_bytes(reader, &mut file_bytes)?;
                            data.insert(
                                key,
                                RedisEntry {
                                    value: RedisValue::String(value),
                                    expiry: current_expiry,
                                },
                            );
                            current_expiry = None;
                        }
                        0x01 => {
                            // List Encoding
                            let key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let len = read_rdb_length(reader, &mut file_bytes)?;
                            let mut items = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                let item = read_length_prefixed_bytes(reader, &mut file_bytes)?;
                                items.push(item);
                            }
                            data.insert(
                                key,
                                RedisEntry {
                                    value: RedisValue::List(items),
                                    expiry: current_expiry,
                                },
                            );
                            current_expiry = None;
                        }
                        0x02 => {
                            // Set Encoding
                            let key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let len = read_rdb_length(reader, &mut file_bytes)?;
                            let mut items = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                let item = read_length_prefixed_bytes(reader, &mut file_bytes)?;
                                items.push(item);
                            }
                            data.insert(
                                key,
                                RedisEntry {
                                    value: RedisValue::Set(items),
                                    expiry: current_expiry,
                                },
                            );
                            current_expiry = None;
                        }
                        0x03 => {
                            // Sorted Set in Ziplist Encoding
                            let key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let ziplist = read_length_prefixed_bytes(reader, &mut file_bytes)?;
                            data.insert(
                                key,
                                RedisEntry {
                                    value: RedisValue::Ziplist(ziplist),
                                    expiry: current_expiry,
                                },
                            );
                            current_expiry = None;
                        }
                        0x04 => {
                            // Hash in Zipmap Encoding
                            let key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let zipmap = read_length_prefixed_bytes(reader, &mut file_bytes)?;
                            data.insert(
                                key,
                                RedisEntry {
                                    value: RedisValue::Zipmap(zipmap),
                                    expiry: current_expiry,
                                },
                            );
                            current_expiry = None;
                        }
                        0x09 => {
                            // Hashmap in Ziplist Encoding
                            let key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let ziplist = read_length_prefixed_bytes(reader, &mut file_bytes)?;
                            data.insert(
                                key,
                                RedisEntry {
                                    value: RedisValue::Ziplist(ziplist),
                                    expiry: current_expiry,
                                },
                            );
                            current_expiry = None;
                        }
                        0x0A => {
                            // List in Ziplist Encoding
                            let key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let ziplist = read_length_prefixed_bytes(reader, &mut file_bytes)?;
                            data.insert(
                                key,
                                RedisEntry {
                                    value: RedisValue::Ziplist(ziplist),
                                    expiry: current_expiry,
                                },
                            );
                            current_expiry = None;
                        }
                        0x0B => {
                            // Set in Intset Encoding
                            let key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let intset = read_length_prefixed_bytes(reader, &mut file_bytes)?;
                            data.insert(
                                key,
                                RedisEntry {
                                    value: RedisValue::Intset(intset),
                                    expiry: current_expiry,
                                },
                            );
                            current_expiry = None;
                        }
                        0x0C => {
                            // Sorted Set in Intset Encoding
                            let key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let intset = read_length_prefixed_bytes(reader, &mut file_bytes)?;
                            data.insert(
                                key,
                                RedisEntry {
                                    value: RedisValue::Intset(intset),
                                    expiry: current_expiry,
                                },
                            );
                            current_expiry = None;
                        }
                        0x0D => {
                            // List in Quicklist Encoding
                            let key = read_length_prefixed_string(reader, &mut file_bytes)?;
                            let quicklist = read_length_prefixed_bytes(reader, &mut file_bytes)?;
                            data.insert(
                                key,
                                RedisEntry {
                                    value: RedisValue::Quicklist(quicklist),
                                    expiry: current_expiry,
                                },
                            );
                            current_expiry = None;
                        }
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("Unsupported RDB value type: {:#X}", opcode),
                            ));
                        }
                    }
                }
                Err(_) => break,
            }
        }

        // Read checksum (8 bytes)
        let mut checksum = [0u8; 8];
        reader.read_exact(&mut checksum)?;

        // Calculate CRC64 of all bytes except the checksum itself
        let expected = u64::from_le_bytes(checksum);
        let actual = crc64(0, &file_bytes);

        if expected != actual {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "RDB checksum mismatch: expected {:016x}, got {:016x}",
                    expected, actual
                ),
            ));
        }

        Ok(RdbDatabase { data })
    }
}

// Helper to read a length-prefixed string and update file_bytes
fn read_length_prefixed_string<R: Read>(
    reader: &mut R,
    file_bytes: &mut Vec<u8>,
) -> io::Result<String> {
    let len = read_rdb_length(reader, file_bytes)?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)?;
    file_bytes.extend_from_slice(&buf);
    Ok(String::from_utf8_lossy(&buf).to_string())
}

// Helper to read a length-prefixed byte array and update file_bytes
fn read_length_prefixed_bytes<R: Read>(
    reader: &mut R,
    file_bytes: &mut Vec<u8>,
) -> io::Result<Vec<u8>> {
    let len = read_rdb_length(reader, file_bytes)?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)?;
    file_bytes.extend_from_slice(&buf);
    Ok(buf)
}

// Reads the RDB length encoding and updates file_bytes
fn read_rdb_length<R: Read>(reader: &mut R, file_bytes: &mut Vec<u8>) -> io::Result<u64> {
    let mut first = [0u8; 1];
    reader.read_exact(&mut first)?;
    file_bytes.push(first[0]);
    let enc_type = first[0] >> 6;
    let mut len = (first[0] & 0x3F) as u64;

    match enc_type {
        0 => Ok(len), // 6-bit length
        1 => {
            let mut second = [0u8; 1];
            reader.read_exact(&mut second)?;
            file_bytes.push(second[0]);
            len = ((len << 8) | second[0] as u64) as u64;
            Ok(len)
        }
        2 => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            file_bytes.extend_from_slice(&buf);
            Ok(u32::from_le_bytes(buf) as u64)
        }
        3 => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Special RDB encoding not supported",
        )),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid RDB length encoding",
        )),
    }
}

/// Save the current database state to RDB file
pub async fn save() -> Result<(), String> {
    // First purge any expired keys
    db::purge_expired_keys().await;

    let config = config::get_config();
    let rdb_path = config.dir.join(&config.dbfilename);

    // Create a temporary file first
    let temp_path = rdb_path.with_extension("tmp");
    let mut file =
        File::create(&temp_path).map_err(|e| format!("Failed to create RDB file: {}", e))?;

    // Get current timestamp for calculating expiry
    let current_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("System time error: {}", e))?
        .as_millis() as u64;

    let mut file_bytes = Vec::new();

    // Write RDB header
    file_bytes.extend_from_slice(b"REDIS");
    file_bytes.extend_from_slice(&[0, 0, 0, 11]); // Version 0011

    // Write database selector (database 0)
    file_bytes.push(0xFE);
    write_rdb_length(&mut file_bytes, 0)?;

    // Get hash table size hint
    let kv = db::KV.read().await;
    let exp = db::EXP.read().await;

    // Write resize hint
    file_bytes.push(0xFB);
    write_rdb_length(&mut file_bytes, kv.len() as u64)?;
    write_rdb_length(&mut file_bytes, exp.len() as u64)?;

    // Write all key-value pairs
    for (key, value) in kv.iter() {
        // Check if key has expiry
        if let Some(expiry_instant) = exp.get(key) {
            // Calculate expiry timestamp in milliseconds
            let now = Instant::now();
            if *expiry_instant > now {
                let remaining_duration = *expiry_instant - now;
                let expiry_timestamp = current_timestamp + remaining_duration.as_millis() as u64;

                // Write expiry in milliseconds
                file_bytes.push(0xFC);
                file_bytes.extend_from_slice(&expiry_timestamp.to_le_bytes());
            }
        }

        // Write the key-value pair based on value type
        match value {
            RedisValue::String(s) => {
                file_bytes.push(0x00); // String encoding
                write_length_prefixed_string(&mut file_bytes, key)?;
                write_length_prefixed_bytes(&mut file_bytes, s)?;
            }
            RedisValue::List(items) => {
                file_bytes.push(0x01); // List encoding
                write_length_prefixed_string(&mut file_bytes, key)?;
                write_rdb_length(&mut file_bytes, items.len() as u64)?;
                for item in items {
                    write_length_prefixed_bytes(&mut file_bytes, item)?;
                }
            }
            RedisValue::Set(items) => {
                file_bytes.push(0x02); // Set encoding
                write_length_prefixed_string(&mut file_bytes, key)?;
                write_rdb_length(&mut file_bytes, items.len() as u64)?;
                for item in items {
                    write_length_prefixed_bytes(&mut file_bytes, item)?;
                }
            }
            RedisValue::Ziplist(data) => {
                file_bytes.push(0x0A); // List in Ziplist encoding
                write_length_prefixed_string(&mut file_bytes, key)?;
                write_length_prefixed_bytes(&mut file_bytes, data)?;
            }
            RedisValue::Zipmap(data) => {
                file_bytes.push(0x04); // Hash in Zipmap encoding
                write_length_prefixed_string(&mut file_bytes, key)?;
                write_length_prefixed_bytes(&mut file_bytes, data)?;
            }
            RedisValue::Intset(data) => {
                file_bytes.push(0x0B); // Set in Intset encoding
                write_length_prefixed_string(&mut file_bytes, key)?;
                write_length_prefixed_bytes(&mut file_bytes, data)?;
            }
            RedisValue::Quicklist(data) => {
                file_bytes.push(0x0D); // List in Quicklist encoding
                write_length_prefixed_string(&mut file_bytes, key)?;
                write_length_prefixed_bytes(&mut file_bytes, data)?;
            }
            // For complex types, we'll serialize them as strings for now
            RedisValue::Integer(i) => {
                file_bytes.push(0x00); // String encoding
                write_length_prefixed_string(&mut file_bytes, key)?;
                let value_bytes = i.to_string().into_bytes();
                write_length_prefixed_bytes(&mut file_bytes, &value_bytes)?;
            }
            RedisValue::Float(f) => {
                file_bytes.push(0x00); // String encoding
                write_length_prefixed_string(&mut file_bytes, key)?;
                let value_bytes = f.to_string().into_bytes();
                write_length_prefixed_bytes(&mut file_bytes, &value_bytes)?;
            }
            RedisValue::Boolean(b) => {
                file_bytes.push(0x00); // String encoding
                write_length_prefixed_string(&mut file_bytes, key)?;
                let value_bytes = b.to_string().into_bytes();
                write_length_prefixed_bytes(&mut file_bytes, &value_bytes)?;
            }
            RedisValue::Hash(hash) => {
                file_bytes.push(0x04); // Hash in Zipmap encoding (simplified)
                write_length_prefixed_string(&mut file_bytes, key)?;
                // Serialize hash as a simple format for now
                let mut hash_data = Vec::new();
                for (k, v) in hash {
                    hash_data.extend_from_slice(k);
                    hash_data.push(0); // separator
                    hash_data.extend_from_slice(v);
                    hash_data.push(0); // separator
                }
                write_length_prefixed_bytes(&mut file_bytes, &hash_data)?;
            }
            RedisValue::SortedSet(sorted_set) => {
                file_bytes.push(0x03); // Sorted Set in Ziplist encoding (simplified)
                write_length_prefixed_string(&mut file_bytes, key)?;
                let mut ss_data = Vec::new();
                for (member, score) in sorted_set {
                    ss_data.extend_from_slice(member);
                    ss_data.push(0); // separator
                    ss_data.extend_from_slice(&score.to_string().into_bytes());
                    ss_data.push(0); // separator
                }
                write_length_prefixed_bytes(&mut file_bytes, &ss_data)?;
            }
            RedisValue::Null => {
                // Skip null values
                continue;
            }
        }
    }

    // Write end of file marker
    file_bytes.push(0xFF);

    // Calculate and write checksum
    let checksum = crc64(0, &file_bytes);
    file_bytes.extend_from_slice(&checksum.to_le_bytes());

    // Write all data to file
    file.write_all(&file_bytes)
        .map_err(|e| format!("Failed to write RDB file: {}", e))?;

    file.flush()
        .map_err(|e| format!("Failed to flush RDB file: {}", e))?;

    // Atomically replace the old file with the new one
    std::fs::rename(temp_path, rdb_path)
        .map_err(|e| format!("Failed to rename RDB file: {}", e))?;

    println!("Saved {} keys to RDB file", kv.len());
    Ok(())
}

/// Helper function to write RDB length encoding
fn write_rdb_length(buf: &mut Vec<u8>, len: u64) -> Result<(), String> {
    if len < 64 {
        // 6-bit length
        buf.push(len as u8);
    } else if len < 16384 {
        // 14-bit length
        buf.push(0x40 | ((len >> 8) as u8));
        buf.push(len as u8);
    } else if len < 4294967296 {
        // 32-bit length
        buf.push(0x80);
        buf.extend_from_slice(&(len as u32).to_le_bytes());
    } else {
        return Err("Length too large for RDB format".to_string());
    }
    Ok(())
}

/// Helper function to write length-prefixed string
fn write_length_prefixed_string(buf: &mut Vec<u8>, s: &str) -> Result<(), String> {
    let bytes = s.as_bytes();
    write_rdb_length(buf, bytes.len() as u64)?;
    buf.extend_from_slice(bytes);
    Ok(())
}

/// Helper function to write length-prefixed bytes
fn write_length_prefixed_bytes(buf: &mut Vec<u8>, bytes: &[u8]) -> Result<(), String> {
    write_rdb_length(buf, bytes.len() as u64)?;
    buf.extend_from_slice(bytes);
    Ok(())
}
