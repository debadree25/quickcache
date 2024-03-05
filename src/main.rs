pub mod threadpool;

use std::error;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use threadpool::ThreadPool;

fn handle_client(mut stream: TcpStream) -> Result<(), Box<dyn error::Error>> {
    let mut buffer = [0; 1024];
    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        stream.write_all(b"+PONG\r\n")?;
    }
    Ok(())
}

fn main() {
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let pool = ThreadPool::new(4);
    println!("Thread pool size is {}", pool.get_thread_count());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                pool.execute(|| {
                    handle_client(stream).unwrap_or_else(|e| eprintln!("error: {}", e));
                })
                .unwrap_or_else(|e| eprintln!("error: {}", e));
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }

    println!("Shutting down.");
}
