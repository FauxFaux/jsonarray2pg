extern crate argparse;
extern crate num_cpus;
extern crate postgres;

use std::env;
use std::fs;
use std::io;
use std::process;
use std::sync;
use std::thread;
use std::time;

use std::vec::Vec;

mod json;

use argparse::{ArgumentParser, StoreTrue, Store};

// magic:
use std::io::Write;

type WorkStack = sync::Arc<(
    sync::Mutex<Vec<Option<String>>>,
    sync::Condvar, // not_empty
    sync::Condvar)>;

fn push(us: &WorkStack,
        max: usize,
        live_crashes: &sync::Arc<sync::atomic::AtomicBool>,
        val: Option<String>)
        -> Result<(), String> {
    let timeout = time::Duration::from_secs(1);
    let &(ref mux, ref not_empty, ref not_full) = &**us;
    let mut lock = try!(mux.lock().map_err(|e| format!("threadpool damaged: {}", e)));
    while lock.len() == max {
        let (new_lock, timeout) = not_full.wait_timeout(lock, timeout).unwrap();
        lock = new_lock;
        if timeout.timed_out() {
            if live_crashes.load(sync::atomic::Ordering::Relaxed) {
                return Err("some thread has died".to_string());
            }
        }
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

fn worker_thread(work: WorkStack, url: String, query: String) -> Result<(), String> {
    let conn = postgres::Connection::connect(url.as_str(), postgres::TlsMode::None)
        .map_err(|e| format!("connecting to {} failed: {}", url, e))?;
    let tran = conn.transaction()
        .map_err(|e| format!("starting a transaction failed: {}", e))?;
    let stmt = tran.prepare(query.as_str())
        .map_err(|e| format!("preparing statement failed: {}", e))?;
    loop {
        let s = pop(&work);
        if s.is_none() {
            break;
        }
        stmt.execute(&[&s.unwrap()])
            .map_err(|e| format!("executing statement failed: {}", e))?;
    }
    tran.commit()
        .map_err(|e| format!("committing results failed: {}", e))?;

    return Ok(());
}

fn run() -> u8 {
    let mut path: String = "-".to_string();
    let mut stdout = false;
    let mut thread_count: usize = num_cpus::get();
    let mut query = String::new();
    let mut table = String::new();
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("read a json array file");
        ap.refer(&mut stdout)
            .add_option(&["--stdout"], StoreTrue, "write cleaned lines to stdout");
        ap.refer(&mut thread_count)
            .add_option(&["-P", "--threads"], Store, "threads (connections) to use");
        ap.refer(&mut query)
            .add_option(&["--query"], Store, "query to run");
        ap.refer(&mut table)
            .add_option(&["-t", "--table"], Store, "table to insert into using generated query");
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
        return 0;
    }

    if table.is_empty() == query.is_empty() {
        writeln!(io::stderr(), "exactly one of table or query must be specified").unwrap();
        return 2;
    }

    if query.is_empty() {
        query = format!("INSERT INTO \"{}\" (row) VALUES ($1::varchar::jsonb)", table);
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
    let live_crashes = sync::Arc::new(sync::atomic::AtomicBool::new(false));

    for _ in 0..thread_count {
        let thread_work = work.clone();
        let thread_url = url.clone();
        let thread_query = query.clone();
        let thread_live_crashes = live_crashes.clone();
        threads.push(thread::spawn(move || -> Result<(), String> {
            let res = worker_thread(thread_work, thread_url, thread_query);
            thread_live_crashes.store(true, sync::atomic::Ordering::Relaxed);
            return res;
        }));
    }

    let parse_success = json::parse_array_from_file(&mut reader, |doc| {
        push(&work, thread_count, &live_crashes, Some(String::from(doc))).map_err(other_err)
    });

    let mut err: bool = false;

    if let Err(e) = parse_success {
        writeln!(io::stderr(), "error: parsing failed, trying to shut down. Cause: {}", e).unwrap();
        let &(ref mux, _, ref not_full) = &*work;
        let mut lock = mux.lock().unwrap();
        lock.clear();
        not_full.notify_all();
        err = true;
    }

    for _ in &threads {
        push(&work, thread_count, &live_crashes, None).expect("asking to shutdown failed??");
    }

    let mut id = 0;
    // can't use enumerate() 'cos it copies
    for t in threads {
        if let Err(e) = t.join().expect("thread paniced!") {
            writeln!(io::stderr(), "error: thread {} failed: {}", id, e).unwrap();
            err = true;
        }
        id += 1;
    }
    return if err { 3 } else { 0 };
}

fn main() {
    process::exit(run() as i32);
}
