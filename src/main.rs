pub mod threadpool;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};
use threadpool::ThreadPool;

#[allow(unused_macros)]
macro_rules! syscall {
    ($fn: ident ( $($arg: expr),* $(,)* ) ) => {{
        let res = unsafe { libc::$fn($($arg, )*) };
        if res == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(res)
        }
    }};
}

struct RequestContext {
    stream: TcpStream,
}

impl RequestContext {
    fn new(stream: TcpStream) -> RequestContext {
        RequestContext { stream }
    }

    fn handle_read(&mut self) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let mut buffer = [0; 1024];
        let nread = self.stream.read(&mut buffer)?;
        if nread == 0 {
            Ok(None)
        } else {
            Ok(Some(std::str::from_utf8(&buffer)?.to_string()))
        }
    }

    fn handle_write(&mut self, response: &str) -> std::io::Result<()> {
        self.stream.write_all(response.as_bytes())?;
        Ok(())
    }
}

fn kqueue() -> std::io::Result<RawFd> {
    let fd = syscall!(kqueue())?;
    if let Ok(flags) = syscall!(fcntl(fd, libc::F_GETFD)) {
        syscall!(fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC))?;
    }
    Ok(fd)
}

fn register_with_kqueue_for_read(kq: i32, fd: i32) -> std::io::Result<()> {
    let mut event = libc::kevent {
        ident: fd as usize,
        filter: libc::EVFILT_READ,
        flags: libc::EV_ADD,
        fflags: 0,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    syscall!(kevent(
        kq,
        &mut event,
        1,
        std::ptr::null_mut(),
        0,
        std::ptr::null()
    ))?;
    Ok(())
}

fn get_kqueue_events(kq: i32) -> std::io::Result<Vec<libc::kevent>> {
    let mut events: Vec<libc::kevent> = vec![
        libc::kevent {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        10
    ];
    let n = syscall!(kevent(
        kq,
        std::ptr::null(),
        0,
        events.as_mut_ptr(),
        events.len() as i32,
        std::ptr::null()
    ))?;
    events.truncate(n as usize);
    Ok(events)
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").expect("Failed to bind to address");
    listener
        .set_nonblocking(true)
        .expect("Failed to set non-blocking mode on listener");
    let listener_fd = listener.as_raw_fd();
    let kq = kqueue().expect("Failed to create kqueue");
    let mut streams_map = HashMap::new();
    register_with_kqueue_for_read(kq, listener_fd)
        .expect("Failed to register listener with kqueue");
    let pool = ThreadPool::new(4);
    loop {
        println!("Waiting for events");
        println!("Requests in flight {}", streams_map.len());
        let events = get_kqueue_events(kq).expect("Failed to get kqueue events");
        println!("Got {} events", events.len());
        for event in events {
            if event.ident == listener_fd as usize {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream
                            .set_nonblocking(true)
                            .expect("Failed to set non-blocking mode on stream");
                        let fd = stream.as_raw_fd();
                        register_with_kqueue_for_read(kq, fd)
                            .expect("Failed to register stream with kqueue");
                        streams_map.insert(fd, RequestContext::new(stream));
                    }
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::WouldBlock {
                            eprintln!("Failed to accept connection: {}", e);
                        }
                    }
                }
            } else {
                let fd = event.ident as i32;
                match streams_map.get_mut(&fd) {
                    Some(request_context) => {
                        if event.filter == libc::EVFILT_READ {
                            match request_context.handle_read() {
                                Ok(Some(request)) => {
                                    println!("Got request: {}", request);
                                    request_context
                                        .handle_write("+PONG\r\n")
                                        .unwrap_or_else(|e| {
                                            eprintln!("Failed to write response: {}", e)
                                        })
                                }
                                Ok(None) => {
                                    println!("Client disconnected");
                                    streams_map.remove(&fd);
                                }
                                Err(e) => {
                                    eprintln!("Failed to read from stream: {}", e);
                                }
                            }
                        }
                    }
                    None => {
                        eprintln!("Got event for unknown file descriptor: {}", fd);
                    }
                }
            }
        }
    }
}
