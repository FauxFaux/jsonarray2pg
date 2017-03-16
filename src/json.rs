use std::io;
use std::str;

use std::iter::Peekable;

//magic
use std::io::Read;

struct Stream<T: Iterator<Item = u8>> {
    it: T,
    next: Option<u8>,
}

impl<T: Iterator<Item = u8>> Stream<T> {
    fn peek(&mut self) -> u8 {
        return self.next.expect("asked for more!");
    }

    fn next(&mut self) -> Option<u8> {
        let old_val = self.next;
        self.next = self.it.next();
        return old_val;
    }
}

fn drop_whitespace<T: Iterator<Item = u8>>(iter: &mut Stream<T>) {
    while (iter.peek() as char).is_whitespace() {
        iter.next();
    }
}

fn read_doc<T: Iterator<Item = u8>>(mut iter: &mut Stream<T>,
                                    mut buf: &mut Vec<u8>)
                                    -> Result<(), String> {
    assert_eq!('{' as u8, iter.next().unwrap());
    buf.push('{' as u8);
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
        let end = iter.next().ok_or("eof at potential end of doc")? as char;
        buf.push(end as u8);
        match end {
            ',' => continue,
            '}' => break,
            _ => return Err(format!("found {} while trying to read document", end)),
        };
    }
    return Ok(());
}

#[allow(unused)]
fn read_array<T: Iterator<Item = u8>>(mut iter: &mut Stream<T>,
                                      buf: &Vec<u8>)
                                      -> Result<(), String> {
    unimplemented!();
}

fn read_string<T: Iterator<Item = u8>>(mut iter: &mut Stream<T>,
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
    }
    return Ok(());
}

#[allow(unused_variables, unused_mut)]
fn read_num<T: Iterator<Item = u8>>(mut iter: &mut Stream<T>, buf: &Vec<u8>) -> Result<(), String> {
    unimplemented!();
}

fn parse_token<T: Iterator<Item = u8>>(mut iter: &mut Stream<T>,
                                       mut buf: &mut Vec<u8>)
                                       -> Result<(), String> {
    drop_whitespace(&mut iter);

    let start = iter.peek() as char;
    if start.is_digit(10) || '-' == start {
        read_num(&mut iter, &mut buf)?;
    } else {
        match start {
            '{' => read_doc(&mut iter, &mut buf),
            '[' => read_array(&mut iter, &mut buf),
            '"' => read_string(&mut iter, &mut buf),
            _ => Err(format!("invalid token start: {}", start)),
        }?
    }
    return Ok(());
}

fn other_err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

fn bad_eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof,
                   "needed more tokens, but the end of the file was found")
}

pub fn parse_array<T: io::Read, F>(mut from: &mut T, mut consumer: F) -> io::Result<()>
    where F: FnMut(&str) -> io::Result<()>
{
    let mut iter = {
        let mut it = from.bytes().map(|x| x.expect("read error"));
        let next = it.next();
        Stream { it, next }
    };

    let mut buf: Vec<u8> = Vec::new();
    let start = iter.next().ok_or_else(bad_eof)? as char;
    if '[' != start {
        return Err(other_err(format!("start token must be a [, not a '{}'", start)));
    }
    loop {
        try!(parse_token(&mut iter, &mut buf).map_err(other_err));
        drop_whitespace(&mut iter);
        let end = iter.next().ok_or_else(bad_eof)? as char;
        if end == ',' {
            consumer(str::from_utf8(buf.as_slice()).map_err(|e| {
                                      other_err(format!("document part isn't valid utf-8: {}", e))
                                  })?)?;
            buf = Vec::new();
            continue;
        }
        if ']' == end {
            return Ok(());
        }
        return Err(other_err(format!("invalid token at end of array: {}", end)));
    }
}
