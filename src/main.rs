#![feature(io)]

use std::env;
use std::fs;
use std::io;

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


fn drop_whitespace<T: io::Read>(iter: &mut Stream<T>) {
    while iter.peek().is_whitespace() {
        iter.next();
    }
}

fn extract_document<T: io::Read>(mut iter: &mut Stream<T>, buf: &Vec<char>)
    -> Result<(), String>
{
    drop_whitespace(&mut iter);
    return Err("oops".to_string());
}

fn main() {
    let mut args = env::args();
    let us = args.next().expect("binary name must always be present??");
    let path = args.next().expect("input filename must be provided");
    let mut file = fs::File::open(path).expect("input file must exist and be readable");
    let mut buf: Vec<char> = Vec::new();
    let mut iter = Stream::new(&mut file);
    assert_eq!('[', iter.next().expect("non-empty file"));
    extract_document(&mut iter, &mut buf).expect("suc");
}
