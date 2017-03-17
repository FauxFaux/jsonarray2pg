extern crate argparse;
extern crate postgres;

use std::fs;
use std::io;
use std::path;
use std::sync;
use std::thread;

use std::vec::Vec;

mod json;

use argparse::{ArgumentParser, StoreTrue, Store};

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
    let mut path: String = "-".to_string();
    let mut stdout = false;
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("read a json array file");
        ap.refer(&mut stdout)
            .add_option(&["--stdout"], StoreTrue, "write cleaned lines to stdout");
        ap.refer(&mut path)
            .required()
            .add_argument("input", Store, "input file to read (or -)");
        ap.parse_args_or_exit();
    }

    let stdin = io::stdin();
    let mut reader =
        if path == "-" {
            Box::new(stdin.lock()) as Box<io::Read>
        } else {
            Box::new(io::BufReader::new(fs::File::open(path).expect("input file must exist and be readable")))
        };

    if stdout {
        json::parse_array_from_file(&mut reader, |doc| {
            println!("jeepers! {}", doc);
            Ok(())
        }).expect("success");
        return;
    }

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

    json::parse_array_from_file(&mut reader, |doc| {
        push(&work, Some(String::from(doc))).map_err(other_err)
    }).expect("success");

    for _ in &threads {
        push(&work, None).unwrap();
    }

    for t in threads {
        t.join().unwrap();
    }
}
