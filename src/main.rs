#![feature(io)]

extern crate postgres;

use std::env;
use std::fs;
use std::io;
use std::path;
use std::sync;
use std::thread;

use std::iter::Peekable;
use std::vec::Vec;

// magic:
use std::io::Read;

type CharResult = Result<char, io::CharsError>;

struct Stream<T: io::Read> {
    it: io::Chars<T>,
    next: Option<char>,
}


impl <T: io::Read> Stream<T> {
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
        return Stream { it , next };
    }
}

type WorkStack = sync::Arc<(sync::Mutex<Vec<Option<String>>>, sync::Condvar)>;

fn push(us: &WorkStack, val: Option<String>) {
    let &(ref mux, ref cvar) = &**us;
    let mut lock = mux.lock().unwrap();
    lock.insert(0, val);
    cvar.notify_one();
}

fn pop(us: &WorkStack) -> Option<String> {
    let &(ref mux, ref cvar) = &**us;
    let mut lock = mux.lock().unwrap();
    while lock.is_empty() {
        lock = cvar.wait(lock).unwrap();
    }
    return lock.pop().unwrap();
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
        extract_document(&mut iter, &mut buf);
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

fn read_array<T: io::Read>(mut iter: &mut Stream<T>, buf: &Vec<char>) -> Result<(), String> {
    unimplemented!();
}

fn read_string<T: io::Read>(mut iter: &mut Stream<T>, mut buf: &mut Vec<char>) -> Result<(), String> {
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

fn read_num<T: io::Read>(mut iter: &mut Stream<T>, buf: &Vec<char>) -> Result<(), String> {
    unimplemented!();
}

fn extract_document<T: io::Read>(mut iter: &mut Stream<T>, mut buf: &mut Vec<char>) -> Result<(), String>
{
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

fn main() {
    let mut args = env::args();
    let us = args.next().expect("binary name must always be present??");
    let path = args.next().expect("input filename must be provided");
    let mut file = io::BufReader::new(fs::File::open(path).expect("input file must exist and be readable"));
    let mut iter = Stream::new(&mut file);

    let mut work = sync::Arc::new((sync::Mutex::new(Vec::new()), sync::Condvar::new()));

    let mut threads: Vec<thread::JoinHandle<_>> = Vec::new();

    for _ in 1..10 {
        let mut thread_work = work.clone();
        threads.push(thread::spawn(move || {
            let params = postgres::params::ConnectParams::builder()
                .user("faux", None)
                .build(postgres::params::Host::Unix(
                        path::PathBuf::from("/var/run/postgresql")));
            let conn = postgres::Connection::connect(params, postgres::TlsMode::None).unwrap();
            let tran = conn.transaction().unwrap();
            let stmt = tran.prepare("INSERT INTO db3j (row) VALUES ($1::varchar::jsonb)").unwrap();
            loop {
                let s = pop(&thread_work);
                if s.is_none() {
                    break;
                }
                stmt.execute(&[&s.unwrap()]).unwrap();
            }
            tran.commit().unwrap();
        }));
    }

    let mut buf: Vec<char> = Vec::new();
    assert_eq!('[', iter.next().expect("non-empty file"));
    loop {
        extract_document(&mut iter, &mut buf).expect("suc");
        drop_whitespace(&mut iter);
        let end = iter.next().unwrap();
        if end == ',' {
            let s: String = buf.iter().cloned().collect();
            push(&work, Some(s));
            buf.clear();
            continue;
        }
        if ']' == end {
            break;
        }

        panic!("invalid ender: {}", end);
    }

    for _ in &threads {
        push(&work, None);
    }

    for t in threads {
        t.join().unwrap();
    }
}
