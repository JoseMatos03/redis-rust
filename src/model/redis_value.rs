use std::collections::HashMap;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum RedisValue {
    String(Vec<u8>),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    SortedSet(Vec<(Vec<u8>, f64)>), // (member, score)
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Zipmap(Vec<u8>),    // Raw zipmap encoding
    Ziplist(Vec<u8>),   // Raw ziplist encoding
    Intset(Vec<u8>),    // Raw intset encoding
    Quicklist(Vec<u8>), // Raw quicklist encoding
}
