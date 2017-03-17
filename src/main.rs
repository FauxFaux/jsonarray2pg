extern crate argparse;
extern crate num_cpus;
extern crate postgres;

use std::env;
use std::fs;
use std::io;
use std::sync;
use std::thread;

use std::vec::Vec;

mod json;

use argparse::{ArgumentParser, StoreTrue, Store};

type WorkStack = sync::Arc<(
    sync::Mutex<Vec<Option<String>>>,
    sync::Condvar, // not_empty
    sync::Condvar)>;

fn push(us: &WorkStack, max: usize, val: Option<String>) -> Result<(), String> {
    let &(ref mux, ref not_empty, ref not_full) = &**us;
    let mut lock = try!(mux.lock().map_err(|e| format!("threadpool damaged: {}", e)));
    while lock.len() == max {
        lock = not_full.wait(lock).unwrap();
    }
    lock.insert(0, val);
    not_empty.notify_one();
    return Ok(());
}

fn pop(us: &WorkStack) -> Option<String> {
    let &(ref mux, ref not_empty, ref not_full) = &**us;
    let mut lock = mux.lock().unwrap();
    while lock.is_empty() {
        lock = not_empty.wait(lock).unwrap();
    }
    not_full.notify_one();
    return lock.pop().unwrap();
}

fn other_err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}

fn main() {
    let mut path: String = "-".to_string();
    let mut stdout = false;
    let mut thread_count: usize = num_cpus::get();
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("read a json array file");
        ap.refer(&mut stdout)
            .add_option(&["--stdout"], StoreTrue, "write cleaned lines to stdout");
        ap.refer(&mut thread_count)
            .add_option(&["-P", "--threads"], Store, "threads (connections) to use");
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
            println!("{}", doc);
            Ok(())
        }).expect("success");
        return;
    }

    let url = match env::var("PGURL") {
        Ok(val) => val,
        Err(e) => match e {
            env::VarError::NotPresent => {
                let whoami = match env::var("USER") {
                    Ok(val) => val,
                    Err(_) => panic!("USER or PGURL env is required to be present and valid unicode"),
                };
                format!("postgres://{}@%2Frun%2Fpostgresql", whoami)
            },
            env::VarError::NotUnicode(_) => panic!("PGURL was set, but contained invalid characters"),
        }
    };

    let work: WorkStack = sync::Arc::new((
            sync::Mutex::new(Vec::new()),
            sync::Condvar::new(),
            sync::Condvar::new()));

    let mut threads: Vec<thread::JoinHandle<_>> = Vec::new();

    for _ in 0..thread_count {
        let thread_work = work.clone();
        let thread_url = url.clone();
        threads.push(thread::spawn(move || -> Result<(), String> {
            let conn = postgres::Connection::connect(thread_url.as_str(), postgres::TlsMode::None)
                .map_err(|e| format!("connecting to {} failed: {}", thread_url, e))?;
            let tran = conn.transaction()
                .map_err(|e| format!("starting a transaction failed: {}", e))?;
            let stmt = tran.prepare("INSERT INTO db3j (row) VALUES ($1::varchar::jsonb)")
                .map_err(|e| format!("preparing statement failed: {}", e))?;
            loop {
                let s = pop(&thread_work);
                if s.is_none() {
                    break;
                }
                stmt.execute(&[&s.unwrap()])
                    .map_err(|e| format!("executing statement failed: {}", e))?;
            }
            tran.commit()
                .map_err(|e| format!("committing results failed: {}", e))?;

            return Ok(());
        }));
    }

    json::parse_array_from_file(&mut reader, |doc| {
        push(&work, thread_count, Some(String::from(doc))).map_err(other_err)
    }).expect("parsing or writing failed");

    for _ in &threads {
        push(&work, thread_count, None).expect("asking to shutdown failed??");
    }

    for t in threads {
        t.join().expect("thread paniced!").expect("thread didn't error");
    }
}
