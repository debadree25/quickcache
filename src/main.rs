pub mod threadpool;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};

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
        RequestContext {
            stream,
        }
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
    register_with_kqueue_for_read(kq, listener_fd).expect("Failed to register listener with kqueue");
    loop {
        println!("Waiting for events");
        let events = get_kqueue_events(kq).expect("Failed to get kqueue events");
        println!("Got {} events", events.len());
        for event in events {
            if event.ident == listener_fd as usize {
                let (stream, _) = listener.accept().expect("Failed to accept connection");
                stream.set_nonblocking(true).expect("Failed to set non-blocking mode on stream");
                let fd = stream.as_raw_fd();
                register_with_kqueue_for_read(kq, fd).expect("Failed to register stream with kqueue");
                streams_map.insert(fd, RequestContext::new(stream));
            } else {
                let fd = event.ident as i32;
                let request_context = streams_map.get_mut(&fd).expect("Failed to get request context");
                let mut buffer = [0; 1024];
                request_context.stream.read(&mut buffer).expect("Failed to read from stream");
                std::str::from_utf8(&buffer)
                    .expect("Failed to convert buffer to string")
                    .split("\r\n")
                    .for_each(|line| {
                        println!("Got line: {}", line);
                    });
                request_context.stream.write_all(b"+PONG\r\n").expect("Failed to write to stream");
            }
        }
    }
}
