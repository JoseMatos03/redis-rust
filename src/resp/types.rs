/// RESP (REdis Serialization Protocol) data types
#[derive(Debug, Clone)]
pub enum Frame {
    // RESP2 classics:
    SimpleString(String),        // +
    Error(String),               // -
    Integer(i64),                // :
    BulkString(Option<Vec<u8>>), // $
    Array(Option<Vec<Frame>>),   // *

    // RESP3 additions:
    Null,              // _   (simple null)
    Boolean(bool),     // #   (true / false)
    Double(f64),       // ,   (floating point)
    BigNumber(String), // (   (arbitrary‐precision integer as string)
    BulkError(String), // !   (error that carries a payload)
    VerbatimString {
        // =   (len, subtype, data)
        subtype: String,
        data: Vec<u8>,
    },
    Map(Option<Vec<(Frame, Frame)>>), // %   (array of pair‐frames)
    Set(Option<Vec<Frame>>),          // ~
    Attribute(Option<Vec<(Frame, Frame)>>), // |
    Push(Option<Vec<Frame>>),         // >
}

impl Frame {
    /// Serialize frame back into RESP bytes
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Frame::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Frame::Error(s) => format!("-{}\r\n", s).into_bytes(),
            Frame::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            Frame::BulkString(Some(bs)) => {
                let mut v = format!("${}\r\n", bs.len()).into_bytes();
                v.extend(bs);
                v.extend(b"\r\n");
                v
            }
            Frame::BulkString(None) => b"$-1\r\n".to_vec(),
            Frame::Array(Some(arr)) => {
                let mut v = format!("*{}\r\n", arr.len()).into_bytes();
                for f in arr {
                    v.extend(f.encode());
                }
                v
            }
            Frame::Array(None) => b"*-1\r\n".to_vec(),
            Frame::Null => b"_\r\n".to_vec(),
            Frame::Boolean(b) => format!("#{}\r\n", if *b { "t" } else { "f" }).into_bytes(),
            Frame::Double(d) => format!(",{}\r\n", d).into_bytes(),
            Frame::BigNumber(s) => format!("({}\r\n", s).into_bytes(),
            Frame::BulkError(msg) => {
                let mut v = format!("!{}\r\n", msg.len()).into_bytes();
                v.extend(msg.as_bytes());
                v.extend(b"\r\n");
                v
            }
            Frame::VerbatimString { subtype, data } => {
                let mut v = format!("={} {}\r\n", subtype, data.len()).into_bytes();
                v.extend(data);
                v.extend(b"\r\n");
                v
            }
            Frame::Map(None) => b"%-1\r\n".to_vec(),
            Frame::Map(Some(pairs)) => {
                let mut v = format!("%{}\r\n", pairs.len()).into_bytes();
                for (k, val) in pairs {
                    v.extend(k.encode());
                    v.extend(val.encode());
                }
                v
            }
            Frame::Set(None) => b"~-1\r\n".to_vec(),
            Frame::Set(Some(items)) => {
                let mut v = format!("~{}\r\n", items.len()).into_bytes();
                for it in items {
                    v.extend(it.encode());
                }
                v
            }
            Frame::Attribute(None) => b"|-1\r\n".to_vec(),
            Frame::Attribute(Some(pairs)) => {
                let mut v = format!("|{}\r\n", pairs.len()).into_bytes();
                for (k, val) in pairs {
                    v.extend(k.encode());
                    v.extend(val.encode());
                }
                v
            }
            Frame::Push(None) => b">-1\r\n".to_vec(),
            Frame::Push(Some(items)) => {
                let mut v = format!(">{}\r\n", items.len()).into_bytes();
                for it in items {
                    v.extend(it.encode());
                }
                v
            }
        }
    }
}
