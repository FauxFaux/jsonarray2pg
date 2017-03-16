use std::io;

//magic
use std::io::Read;

struct Stream<T: io::Read> {
    it: io::Chars<T>,
    next: Option<char>,
}

impl<T: io::Read> Stream<T> {
    fn peek(&mut self) -> char {
        return self.next.expect("asked for more!");
    }

    fn next(&mut self) -> Option<char> {
        let old_val = self.next;
        self.next = self.it.next().map(|x| x.unwrap());
        return old_val;
    }

    fn new(from: &mut T) -> Stream<&mut T> {
        let mut it = from.chars();
        let next = it.next().map(|x| x.unwrap());
        return Stream { it, next };
    }
}

fn drop_whitespace<T: io::Read>(iter: &mut Stream<T>) {
    while iter.peek().is_whitespace() {
        iter.next();
    }
}

fn read_doc<T: io::Read>(mut iter: &mut Stream<T>, mut buf: &mut Vec<char>) -> Result<(), String> {
    assert_eq!('{', iter.next().unwrap());
    buf.push('{');
    loop {
        drop_whitespace(&mut iter);
        read_string(&mut iter, &mut buf)?;
        drop_whitespace(&mut iter);
        assert_eq!(':', iter.next().unwrap());
        buf.push(':');
        drop_whitespace(&mut iter);
        try!(extract_document(&mut iter, &mut buf));
        drop_whitespace(&mut iter);
        let end = iter.next().unwrap();
        buf.push(end);
        match end {
            ',' => continue,
            '}' => break,
            _ => return Err(format!("found {} while trying to read document", end)),
        };
    }
    return Ok(());
}

#[allow(unused)]
fn read_array<T: io::Read>(mut iter: &mut Stream<T>, buf: &Vec<char>) -> Result<(), String> {
    unimplemented!();
}

fn read_string<T: io::Read>(mut iter: &mut Stream<T>,
                            mut buf: &mut Vec<char>)
                            -> Result<(), String> {
    assert_eq!('"', iter.next().unwrap());
    buf.push('"');
    loop {
        let c = iter.next().unwrap();
        buf.push(c);
        if c == '\\' {
            if iter.peek() == '"' {
                panic!("can't do strings with escaped quotes: lazy");
            }
        }
        if c == '"' {
            break;
        }
    }
    return Ok(());
}

#[allow(unused_variables, unused_mut)]
fn read_num<T: io::Read>(mut iter: &mut Stream<T>, buf: &Vec<char>) -> Result<(), String> {
    unimplemented!();
}

fn extract_document<T: io::Read>(mut iter: &mut Stream<T>,
                                 mut buf: &mut Vec<char>)
                                 -> Result<(), String> {
    drop_whitespace(&mut iter);

    let start = iter.peek();
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

pub fn parse_array<T: io::Read, F>(mut from: &mut T, mut consumer: F)-> io::Result<()>
where F: FnMut(String) -> io::Result<()>
 {
    let mut iter: Stream<&mut T> = Stream::new(&mut from);
    let mut buf: Vec<char> = Vec::new();
    assert_eq!('[', iter.next().expect("non-empty file"));
    loop {
        extract_document(&mut iter, &mut buf).expect("suc");
        drop_whitespace(&mut iter);
        let end = iter.next().unwrap();
        if end == ',' {
            let s: String = buf.iter().cloned().collect();
            try!(consumer(s));
            buf.clear();
            continue;
        }
        if ']' == end {
            break;
        }
        panic!("invalid ender: {}", end);
    }
    return Ok(());
}

