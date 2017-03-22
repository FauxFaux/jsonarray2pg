use std::io;
use std::str;

use std::iter::Peekable;

//magic
use std::io::Read;

fn drop_whitespace<T: Iterator<Item = u8>>(iter: &mut Peekable<T>) {
    loop {
        let c = match iter.peek() {
            Some(x) => *x,
            None => break,
        };

        match c {
            // ' ' | '\t' | '\r' | '\n'
            0x20 | 0x09 | 0x0A | 0x0D => (),
            _ => break,
        };

        iter.next();
    }
}

fn read_doc<T: Iterator<Item = u8>>(mut iter: &mut Peekable<T>,
                                    mut buf: &mut Vec<u8>)
                                    -> Result<(), String> {
    assert_eq!('{' as u8, iter.next().unwrap());
    buf.push('{' as u8);
    drop_whitespace(&mut iter);
    if '}' as u8 == *iter.peek().ok_or("eof in short object")? {
        buf.push('}' as u8);
        iter.next().unwrap();
        return Ok(());
    }
    loop {
        drop_whitespace(&mut iter);
        read_string(&mut iter, &mut buf)?;
        drop_whitespace(&mut iter);
        if ':' as u8 != iter.next().ok_or("eof at key-value gap")? {
            return Err("invalid key-value separator".to_string());
        }
        buf.push(':' as u8);
        drop_whitespace(&mut iter);
        try!(parse_token(&mut iter, &mut buf));
        drop_whitespace(&mut iter);
        let end = iter.next().ok_or("eof at potential end of doc")?;
        buf.push(end);
        if ',' as u8 == end {
            continue;
        }
        if '}' as u8 == end {
            break;
        }

        return Err(format!("found {} while trying to read document", end));
    }
    return Ok(());
}

fn read_array<T: Iterator<Item = u8>>(mut iter: &mut Peekable<T>,
                                      mut buf: &mut Vec<u8>)
                                      -> Result<(), String> {
    assert_eq!('[' as u8, iter.next().unwrap());
    buf.push('[' as u8);

    drop_whitespace(iter);
    if ']' as u8 == *iter.peek().ok_or("eof in short array")? {
        iter.next().unwrap();
        buf.push(']' as u8);
        return Ok(());
    }

    loop {
        parse_token(&mut iter, &mut buf)?;

        drop_whitespace(iter);
        let end = iter.next().ok_or("eof in long array")?;
        buf.push(end);

        if ',' as u8 == end {
            continue;
        }

        if ']' as u8 == end {
            return Ok(());
        }

        return Err(format!("invalid end of array: {}", end as char));
    }
}

fn read_string<T: Iterator<Item = u8>>(mut iter: &mut T,
                                       mut buf: &mut Vec<u8>)
                                       -> Result<(), String> {
    assert_eq!('"' as u8, iter.next().unwrap());
    buf.push('"' as u8);
    loop {
        let c = iter.next().ok_or("eof in string")?;
        buf.push(c);
        if c == '\\' as u8 {
            buf.push(iter.next().ok_or("eof after backslash in string")?);
            continue;
        }
        if c == '"' as u8 {
            break;
        }
        if c < ' ' as u8 {
            return Err(format!("control character in string: {}", c));
        }
    }
    return Ok(());
}

fn read_num<T: Iterator<Item = u8>>(mut iter: &mut Peekable<T>,
                                    mut buf: &mut Vec<u8>)
                                    -> Result<(), String> {
    loop {
        let c = *iter.peek().ok_or("eof in a word/number")?;
        if !((c >= 'a' as u8 && c <= 'z' as u8) ||
             (c >= '0' as u8 && c <= '9' as u8) ||
                '.' as u8 == c ||
                '-' as u8 == c || '+' as u8 == c ||
                'N' as u8 == c || // NaN
                'I' as u8 == c || // Infinity
                (c >= 'A' as u8 && c <= 'F' as u8)) {
            break;
        }

        buf.push(c);
        iter.next().unwrap();
    }
    return Ok(());
}

fn parse_token<T: Iterator<Item = u8>>(mut iter: &mut Peekable<T>,
                                       mut buf: &mut Vec<u8>)
                                       -> Result<(), String> {
    drop_whitespace(&mut iter);

    match *iter.peek().ok_or("eof looking for an element".to_string())? as char {
        '{' => read_doc(&mut iter, &mut buf),
        '[' => read_array(&mut iter, &mut buf),
        '"' => read_string(&mut iter, &mut buf),
        _ => read_num(&mut iter, &mut buf),
    }?;
    return Ok(());
}

fn other_err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

fn bad_eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof,
                   "needed more tokens, but the end of the file was found")
}

pub fn parse_array_from_file<T: io::Read, F>(mut from: &mut T,
                                             consumer: F,
                                             array: bool)
                                             -> io::Result<()>
    where F: FnMut(&str) -> io::Result<()>
{
    let mut iter = from.bytes().map(|x| x.expect("read error")).peekable();
    return parse_array_from_iter(&mut iter, consumer, array);

}

pub fn parse_array_from_iter<T: Iterator<Item = u8>, F>(mut iter: &mut Peekable<T>,
                                                        mut consumer: F,
                                                        array: bool)
                                                        -> io::Result<()>
    where F: FnMut(&str) -> io::Result<()>
{
    let mut buf: Vec<u8> = Vec::new();
    drop_whitespace(iter);

    if array {
        let start = iter.next().ok_or_else(bad_eof)?;
        if '[' as u8 != start {
            return Err(other_err(format!("start token must be a [, not a '{}'", start as char)));
        }
        if ']' as u8 == *iter.peek().ok_or_else(bad_eof)? {
            return Ok(());
        }
    }

    loop {
        try!(parse_token(&mut iter, &mut buf).map_err(other_err));
        drop_whitespace(&mut iter);
        let end = if array {
            let end = iter.next().ok_or_else(bad_eof)?;
            if (',' as u8) != end && (']' as u8) != end {
                return Err(other_err(format!("invalid token at end of array: {}", end)));
            }
            Some(end)
        } else {
            None
        };

        let last_len = buf.len();
        {
            let as_str = str::from_utf8(buf.as_slice())
                .map_err(|e| other_err(format!("document part isn't valid utf-8: {}", e)))?;
            consumer(as_str)?;
        }

        if array {
            if ']' as u8 == end.unwrap() {
                return Ok(());
            }
        } else {
            if iter.peek().is_none() {
                return Ok(());
            }
        }

        buf = Vec::with_capacity(last_len * 2);
    }
}
