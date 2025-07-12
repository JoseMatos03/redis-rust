use crate::resp::types::Frame;
use bytes::Buf;
use bytes::BytesMut;

pub struct FrameParser {
    buf: BytesMut,
}

impl FrameParser {
    pub fn new() -> Self {
        FrameParser {
            buf: BytesMut::with_capacity(4096),
        }
    }
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn parse(&mut self) -> Result<Option<Frame>, String> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let b0 = self.buf[0];
        match b0 {
            // RESP2:
            b'+' => parse_simple(&mut self.buf).map(Some),
            b'-' => parse_error(&mut self.buf).map(Some),
            b':' => parse_integer(&mut self.buf).map(Some),
            b'$' => parse_bulk(&mut self.buf).map(Some),
            b'*' => parse_array(&mut self.buf).map(Some),

            // RESP3:
            b'_' => parse_null(&mut self.buf).map(Some),
            b'#' => parse_boolean(&mut self.buf).map(Some),
            b',' => parse_double(&mut self.buf).map(Some),
            b'(' => parse_bignumber(&mut self.buf).map(Some),
            b'!' => parse_bulk_error(&mut self.buf).map(Some),
            b'=' => parse_verbatim_string(&mut self.buf).map(Some),
            b'%' => parse_map(&mut self.buf).map(Some),
            b'~' => parse_set(&mut self.buf).map(Some),
            b'|' => parse_attribute(&mut self.buf).map(Some),
            b'>' => parse_push(&mut self.buf).map(Some),

            _ => Err(format!("Unexpected byte: {}", b0)),
        }
    }
}

fn parse_line(buf: &mut BytesMut) -> Option<String> {
    for i in 0..buf.len() - 1 {
        if &buf[i..i + 2] == b"\r\n" {
            let line = buf.split_to(i);
            buf.advance(2);
            return Some(String::from_utf8(line.to_vec()).unwrap());
        }
    }
    None
}

fn parse_simple(buf: &mut BytesMut) -> Result<Frame, String> {
    if let Some(line) = parse_line(buf) {
        Ok(Frame::SimpleString(line[1..].to_string()))
    } else {
        Err("Incomplete".into())
    }
}

fn parse_error(buf: &mut BytesMut) -> Result<Frame, String> {
    if let Some(line) = parse_line(buf) {
        Ok(Frame::Error(line[1..].to_string()))
    } else {
        Err("Incomplete".into())
    }
}

fn parse_integer(buf: &mut BytesMut) -> Result<Frame, String> {
    if let Some(line) = parse_line(buf) {
        let num = line[1..].parse::<i64>().map_err(|e| e.to_string())?;
        Ok(Frame::Integer(num))
    } else {
        Err("Incomplete".into())
    }
}

fn parse_bulk(buf: &mut BytesMut) -> Result<Frame, String> {
    if let Some(line) = parse_line(buf) {
        let len = line[1..].parse::<isize>().map_err(|e| e.to_string())?;
        if len < 0 {
            Ok(Frame::BulkString(None))
        } else if buf.len() >= (len as usize + 2) {
            let data = buf.split_to(len as usize).to_vec();
            buf.advance(2);
            Ok(Frame::BulkString(Some(data)))
        } else {
            Err("Incomplete".into())
        }
    } else {
        Err("Incomplete".into())
    }
}

fn parse_array(buf: &mut BytesMut) -> Result<Frame, String> {
    if let Some(line) = parse_line(buf) {
        let count = line[1..].parse::<isize>().map_err(|e| e.to_string())?;
        if count < 0 {
            Ok(Frame::Array(None))
        } else {
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                // Parse each item in-place, updating the buffer as we go
                let mut parser = FrameParser {
                    buf: BytesMut::new(),
                };
                // Move the buffer content to the parser's buffer
                parser.buf = buf.split();
                match parser.parse()? {
                    Some(frame) => {
                        items.push(frame);
                        // Move back the remaining buffer to the original buf
                        buf.unsplit(parser.buf);
                    }
                    None => return Err("Incomplete array item".into()),
                }
            }
            Ok(Frame::Array(Some(items)))
        }
    } else {
        Err("Incomplete".into())
    }
}

// RESP3 parsing functions
fn parse_null(buf: &mut BytesMut) -> Result<Frame, String> {
    let _ = parse_line(buf).ok_or("Incomplete")?;
    Ok(Frame::Null)
}

fn parse_boolean(buf: &mut BytesMut) -> Result<Frame, String> {
    let line = parse_line(buf).ok_or("Incomplete")?;
    let b = match &line[1..] {
        "t" => true,
        "f" => false,
        _ => return Err("Invalid boolean".into()),
    };
    Ok(Frame::Boolean(b))
}

fn parse_double(buf: &mut BytesMut) -> Result<Frame, String> {
    let line = parse_line(buf).ok_or("Incomplete")?;
    let d = line[1..].parse::<f64>().map_err(|e| e.to_string())?;
    Ok(Frame::Double(d))
}

fn parse_bignumber(buf: &mut BytesMut) -> Result<Frame, String> {
    let line = parse_line(buf).ok_or("Incomplete")?;
    Ok(Frame::BigNumber(line[1..].to_string()))
}

fn parse_bulk_error(buf: &mut BytesMut) -> Result<Frame, String> {
    if let Some(line) = parse_line(buf) {
        let len = line[1..].parse::<usize>().map_err(|e| e.to_string())?;
        if buf.len() < len + 2 {
            return Err("Incomplete".into());
        }
        let data = buf.split_to(len).to_vec();
        buf.advance(2);
        Ok(Frame::BulkError(String::from_utf8_lossy(&data).into()))
    } else {
        Err("Incomplete".into())
    }
}

fn parse_verbatim_string(buf: &mut BytesMut) -> Result<Frame, String> {
    if let Some(line) = parse_line(buf) {
        let mut parts = line[1..].splitn(2, ' ');
        let subtype = parts.next().unwrap().to_string();
        let len = parts
            .next()
            .unwrap()
            .parse::<usize>()
            .map_err(|e| e.to_string())?;
        if buf.len() < len + 2 {
            return Err("Incomplete".into());
        }
        let data = buf.split_to(len).to_vec();
        buf.advance(2);
        Ok(Frame::VerbatimString { subtype, data })
    } else {
        Err("Incomplete".into())
    }
}

fn parse_set(buf: &mut BytesMut) -> Result<Frame, String> {
    parse_aggregate(buf, Frame::Set(None), |n| {
        Frame::Set(Some(Vec::with_capacity(n)))
    })
}

fn parse_push(buf: &mut BytesMut) -> Result<Frame, String> {
    parse_aggregate(buf, Frame::Push(None), |n| {
        Frame::Push(Some(Vec::with_capacity(n)))
    })
}

fn parse_attribute(buf: &mut BytesMut) -> Result<Frame, String> {
    // parse_map returns Frame::Attribute
    match parse_map(buf)? {
        Frame::Attribute(attr) => Ok(Frame::Attribute(attr)),
        _ => Err("Expected attribute frame".into()),
    }
}

fn parse_aggregate(
    buf: &mut BytesMut,
    nil_frame: Frame,
    make_some: impl FnOnce(usize) -> Frame,
) -> Result<Frame, String> {
    let line = parse_line(buf).ok_or("Incomplete")?;
    let count = line[1..].parse::<isize>().map_err(|e| e.to_string())?;
    if count < 0 {
        return Ok(nil_frame);
    }
    let count = count as usize;
    let mut frame = make_some(count);
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        match (FrameParser { buf: buf.clone() }).parse()? {
            Some(f) => {
                items.push(f);
                buf.unsplit(FrameParser { buf: buf.clone() }.buf);
            }
            None => return Err("Incomplete aggregate item".into()),
        }
    }
    match &mut frame {
        Frame::Array(Some(vec)) => *vec = items,
        Frame::Set(Some(vec)) => *vec = items,
        Frame::Push(Some(vec)) => *vec = items,
        _ => {}
    }
    Ok(frame)
}

fn parse_map(buf: &mut BytesMut) -> Result<Frame, String> {
    let line = parse_line(buf).ok_or("Incomplete")?;
    let count = line[1..].parse::<isize>().map_err(|e| e.to_string())?;
    if count < 0 {
        if line.starts_with('%') {
            return Ok(Frame::Map(None));
        } else {
            return Ok(Frame::Attribute(None));
        }
    }
    let count = count as usize;
    let mut pairs = Vec::with_capacity(count);
    for _ in 0..count {
        let key = FrameParser { buf: buf.clone() }
            .parse()?
            .ok_or("Incomplete map key")?;
        let leftover = FrameParser { buf: buf.clone() }.buf;
        buf.unsplit(leftover);
        let value = FrameParser { buf: buf.clone() }
            .parse()?
            .ok_or("Incomplete map value")?;
        let leftover = FrameParser { buf: buf.clone() }.buf;
        buf.unsplit(leftover);
        pairs.push((key, value));
    }
    if line.starts_with('%') {
        Ok(Frame::Map(Some(pairs)))
    } else {
        Ok(Frame::Attribute(Some(pairs)))
    }
}
