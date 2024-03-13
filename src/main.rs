pub mod threadpool;
pub mod resp;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};
use threadpool::ThreadPool;
use resp::{RedisCommand, RedisValue};

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
    write_buffer: Vec<u8>,
}

impl RequestContext {
    fn new(stream: TcpStream) -> RequestContext {
        RequestContext {
            stream,
            write_buffer: Vec::with_capacity(1024),
        }
    }

    fn handle_read(&mut self) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut buffer = [0; 1024];
        let mut total_data = Vec::with_capacity(1024);
        loop {
            match self.stream.read(&mut buffer) {
                Ok(0) => {
                    // client discoonnected
                    return Ok(None);
                }
                Ok(n) => {
                    total_data.extend_from_slice(&buffer[..n]);
                    if n < buffer.len() {
                        break;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => return Err(Box::new(e)),
            }
        }
        Ok(Some(total_data))
    }

    fn dispatch_write(&mut self, response: &[u8]) {
        self.write_buffer.extend_from_slice(response);
    }

    fn write_to_socket(&mut self) -> std::io::Result<()> {
        while !self.write_buffer.is_empty() {
            match self.stream.write(&self.write_buffer) {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    self.write_buffer.drain(0..n);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }
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

enum KqueueEventInterest {
    Read,
    Write,
}

enum KqueueRegistrationAction {
    Register,
    Unregister,
}

fn update_kqueue(
    kq: i32,
    fd: i32,
    interest: KqueueEventInterest,
    action: KqueueRegistrationAction,
) -> std::io::Result<()> {
    let (filter, flags) = match interest {
        KqueueEventInterest::Read => (libc::EVFILT_READ, libc::EV_ADD),
        KqueueEventInterest::Write => (libc::EVFILT_WRITE, libc::EV_ADD),
    };
    let flags = match action {
        KqueueRegistrationAction::Register => flags,
        KqueueRegistrationAction::Unregister => libc::EV_DELETE,
    };
    let mut event = libc::kevent {
        ident: fd as usize,
        filter,
        flags,
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
        256
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

fn handle_request(request: Vec<u8>) -> Vec<u8> {
    let extracted_command = match resp::extract_commands(&request) {
      Ok(cmd) => cmd,
      Err(e) => {
        eprintln!("Failed to parse request: {}", e);
        return b"-ERR failed to parse request\r\n".to_vec();
      }
    };
    match extracted_command {
      RedisCommand::PING(message) | RedisCommand::ECHO(message) => {
        message.to_resp_string().as_bytes().to_vec()
      }
      _ => b"-ERR unknown command\r\n".to_vec(),
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").expect("Failed to bind to address");
    listener
        .set_nonblocking(true)
        .expect("Failed to set non-blocking mode on listener");
    let listener_fd = listener.as_raw_fd();
    let kq = kqueue().expect("Failed to create kqueue");
    let mut streams_map = HashMap::new();
    update_kqueue(
        kq,
        listener_fd,
        KqueueEventInterest::Read,
        KqueueRegistrationAction::Register,
    )
    .expect("Failed to register listener with kqueue");
    let pool = ThreadPool::new(4); // TODO: make use of this
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
                        update_kqueue(
                            kq,
                            fd,
                            KqueueEventInterest::Read,
                            KqueueRegistrationAction::Register,
                        )
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
                        match event.filter {
                            libc::EVFILT_READ => match request_context.handle_read() {
                                Ok(Some(request)) => {
                                    let response = handle_request(request);
                                    request_context.dispatch_write(&response);
                                    update_kqueue(
                                        kq,
                                        fd,
                                        KqueueEventInterest::Write,
                                        KqueueRegistrationAction::Register,
                                    )
                                    .expect("Failed to register stream with kqueue");
                                }
                                Ok(None) => {
                                    println!("Client disconnected");
                                    streams_map.remove(&fd);
                                }
                                Err(e) => {
                                    eprintln!("Failed to read from stream: {}", e);
                                }
                            },
                            libc::EVFILT_WRITE => match request_context.write_to_socket() {
                                Ok(()) => {
                                    println!("Response sent");
                                    update_kqueue(kq, fd, KqueueEventInterest::Write, KqueueRegistrationAction::Unregister).unwrap_or_else(|e| {
                                    eprintln!("Failed to unregister write event for file descriptor: {}", e)
                                });
                                }
                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    eprintln!("Write would block, which should not happen!!");
                                }
                                Err(e) => {
                                    eprintln!("Failed to write to stream: {}", e);
                                }
                            },
                            _ => {
                                eprintln!(
                                    "Got unexpected event for file descriptor: {} {}",
                                    fd, event.filter
                                );
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
