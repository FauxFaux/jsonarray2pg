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
    fh: T,
    next: char,
}

impl <T: io::Read> Stream<T> {
    fn peek() -> char {
        return ' ';
    }
}

fn drop_whitespace<T>(iter: &mut Peekable<T>)
    -> Result<(), String>
    where T: Iterator<Item=CharResult>
{
    loop {
        let res = iter.peek().unwrap();
        let c = res.unwrap();
        if !c.is_whitespace() {
            return Ok(());
        }
        iter.next();
    }
}

fn extract_document<T>(iter: &mut Peekable<T>, buf: &Vec<char>)
    -> Result<(), String>
    where T: Iterator<Item=CharResult>
{
    iter.next();
    return Err("oops".to_string());
}

fn main() {
    let mut args = env::args();
    let us = args.next().expect("binary name must always be present??");
    let path = args.next().expect("input filename must be provided");
    let file = fs::File::open(path).expect("input file must exist and be readable");
    let mut buf: Vec<char> = Vec::new();
    let mut iter = file.chars().peekable();
    assert_eq!('[', iter.next().expect("root element is an array").unwrap());
    extract_document(&mut iter, &mut buf).expect("suc");
}
