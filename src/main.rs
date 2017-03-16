#![feature(io)]

extern crate postgres;

use std::env;
use std::fs;
use std::io;
use std::path;
use std::sync;
use std::thread;

use std::vec::Vec;

mod json;

type WorkStack = sync::Arc<(sync::Mutex<Vec<Option<String>>>, sync::Condvar)>;

fn push(us: &WorkStack, val: Option<String>) -> Result<(), String> {
    let &(ref mux, ref cvar) = &**us;
    let mut lock = try!(mux.lock().map_err(|e| format!("threadpool damaged: {}", e)));
    lock.insert(0, val);
    cvar.notify_one();
    return Ok(());
}

fn pop(us: &WorkStack) -> Option<String> {
    let &(ref mux, ref cvar) = &**us;
    let mut lock = mux.lock().unwrap();
    while lock.is_empty() {
        lock = cvar.wait(lock).unwrap();
    }
    return lock.pop().unwrap();
}

fn other_err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

fn main() {
    let mut args = env::args();
    args.next().expect("binary name must always be present??");
    let path = args.next().expect("input filename must be provided");
    let file = fs::File::open(path).expect("input file must exist and be readable");
    let mut reader = io::BufReader::new(file);

    let work = sync::Arc::new((sync::Mutex::new(Vec::new()), sync::Condvar::new()));

    let mut threads: Vec<thread::JoinHandle<_>> = Vec::new();

    for _ in 1..10 {
        let thread_work = work.clone();
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

    json::parse_array(&mut reader, |doc| {
        push(&work, Some(doc)).map_err(other_err)
    }).expect("success");

    for _ in &threads {
        push(&work, None).unwrap();
    }

    for t in threads {
        t.join().unwrap();
    }
}
