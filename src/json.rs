use std::io;
use std::str;

use std::iter::Peekable;

//magic
use std::io::Read;

fn drop_whitespace<T: Iterator<Item = u8>>(iter: &mut Peekable<T>) {
    loop {
        let c = {
            let next = iter.peek();
            if next.is_none() {
                break;
            }
            *next.unwrap() as char
        };

        if !c.is_whitespace() {
            break;
        }
        iter.next();
    }
}

fn read_doc<T: Iterator<Item = u8>>(mut iter: &mut Peekable<T>,
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

#[allow(unused)]
fn read_array<T: Iterator<Item = u8>>(mut iter: &mut T, buf: &Vec<u8>) -> Result<(), String> {
    unimplemented!();
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
    }
    return Ok(());
}

#[allow(unused_variables, unused_mut)]
fn read_num<T: Iterator<Item = u8>>(mut iter: &mut T, buf: &Vec<u8>) -> Result<(), String> {
    unimplemented!();
}

fn parse_token<T: Iterator<Item = u8>>(mut iter: &mut Peekable<T>,
                                       mut buf: &mut Vec<u8>)
                                       -> Result<(), String> {
    drop_whitespace(&mut iter);

    let start = *iter.peek().ok_or("parse token requires there to be a next token".to_string())? as
                char;
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

pub fn parse_array_from_file<T: io::Read, F>(mut from: &mut T, consumer: F) -> io::Result<()>
    where F: FnMut(&str) -> io::Result<()>
{
    let mut iter = from.bytes().map(|x| x.expect("read error")).peekable();
    return parse_array_from_iter(&mut iter, consumer);

}

pub fn parse_array_from_iter<T: Iterator<Item = u8>, F>(mut iter: &mut Peekable<T>,
                                                        mut consumer: F)
                                                        -> io::Result<()>
    where F: FnMut(&str) -> io::Result<()>
{
    let mut buf: Vec<u8> = Vec::new();
    let start = iter.next().ok_or_else(bad_eof)?;
    if '[' as u8 != start {
        return Err(other_err(format!("start token must be a [, not a '{}'", start as char)));
    }
    loop {
        try!(parse_token(&mut iter, &mut buf).map_err(other_err));
        drop_whitespace(&mut iter);
        let end = iter.next().ok_or_else(bad_eof)?;
        if ',' as u8 == end {
            {
                let as_str = str::from_utf8(buf.as_slice())
                    .map_err(|e| other_err(format!("document part isn't valid utf-8: {}", e)))?;
                consumer(as_str)?;
            }
            buf = Vec::new();
            continue;
        }
        if ']' as u8 == end {
            return Ok(());
        }
        return Err(other_err(format!("invalid token at end of array: {}", end)));
    }
}
