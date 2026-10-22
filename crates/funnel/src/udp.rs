//! StatsD over UDP. Each receiver thread owns a socket bound to the same
//! port with SO_REUSEPORT, so the kernel spreads packets across threads.
//! Packets are gathered for up to `BATCH_BYTES` or a 2 ms lull and routed
//! together, which keeps channel traffic per shard low.
//!
//! On Linux each system call receives up to 32 datagrams with `recvmmsg`;
//! with one `recv` per datagram, the per-call cost was a large share of the
//! work at high packet rates. Other platforms receive one at a time.

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};

use crate::router::Router;
use crate::stats::Stats;
use crate::unix_now;

const BATCH_BYTES: usize = 64 * 1024;
const RECV_BUFFER: usize = 16 * 1024 * 1024;

fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_port(true)?;
    // The kernel caps this at net.core.rmem_max; a smaller buffer only means
    // bursts overflow sooner, so failure is not fatal.
    let _ = socket.set_recv_buffer_size(RECV_BUFFER);
    socket.bind(&addr.into())?;
    let socket: UdpSocket = socket.into();
    socket.set_read_timeout(Some(Duration::from_millis(2)))?;
    Ok(socket)
}

/// Binds `threads` sockets and starts a receiver on each. Returns the bound
/// address (useful with port 0) and the thread handles.
pub fn spawn(
    addr: SocketAddr,
    threads: usize,
    router: Arc<Router>,
    tenant: Arc<str>,
    stop: Arc<AtomicBool>,
) -> io::Result<(SocketAddr, Vec<JoinHandle<()>>)> {
    let first = bind(addr)?;
    let local = first.local_addr()?;
    let mut sockets = vec![first];
    for _ in 1..threads {
        sockets.push(bind(local)?);
    }
    let handles = sockets
        .into_iter()
        .enumerate()
        .map(|(i, socket)| {
            let (router, tenant, stop) = (router.clone(), tenant.clone(), stop.clone());
            std::thread::Builder::new()
                .name(format!("udp-{i}"))
                .spawn(move || receive(socket, &router, &tenant, &stop))
                .expect("spawn udp thread")
        })
        .collect();
    Ok((local, handles))
}

/// Receives datagrams in batches. [`Datagrams::recv`] waits up to the
/// socket's read timeout for the first datagram and then takes whatever
/// else is already queued, without waiting.
struct Datagrams {
    #[cfg(target_os = "linux")]
    batch: linux::Batch,
    #[cfg(not(target_os = "linux"))]
    buf: Vec<u8>,
    #[cfg(not(target_os = "linux"))]
    len: usize,
}

impl Datagrams {
    fn new() -> Self {
        Datagrams {
            #[cfg(target_os = "linux")]
            batch: linux::Batch::new(),
            #[cfg(not(target_os = "linux"))]
            buf: vec![0; MAX_DATAGRAM],
            #[cfg(not(target_os = "linux"))]
            len: 0,
        }
    }

    /// Returns how many datagrams arrived.
    fn recv(&mut self, socket: &UdpSocket) -> io::Result<usize> {
        #[cfg(target_os = "linux")]
        return self.batch.recv(socket);
        #[cfg(not(target_os = "linux"))]
        {
            self.len = socket.recv(&mut self.buf)?;
            Ok(1)
        }
    }

    /// The `i`th datagram of the last batch, or `None` if it was truncated.
    fn get(&self, i: usize) -> Option<&[u8]> {
        #[cfg(target_os = "linux")]
        return self.batch.get(i);
        #[cfg(not(target_os = "linux"))]
        {
            let _ = i;
            Some(&self.buf[..self.len])
        }
    }
}

/// Largest UDP payload over IPv4 is 65,507 bytes; a slot this size never
/// truncates.
const MAX_DATAGRAM: usize = 65536;

#[cfg(target_os = "linux")]
mod linux {
    use std::io;
    use std::net::UdpSocket;
    use std::os::fd::AsRawFd;

    use super::MAX_DATAGRAM;

    /// Datagrams per `recvmmsg` call; 2 MiB of buffers per receiver.
    const SLOTS: usize = 32;

    pub struct Batch {
        bufs: Vec<u8>,
        iovs: Vec<libc::iovec>,
        headers: Vec<libc::mmsghdr>,
    }

    // The raw pointers in `iovs` and `headers` point into the heap buffers
    // of `bufs` and `iovs`, which are allocated once and never resized, so
    // they stay valid when a `Batch` moves to another thread.
    unsafe impl Send for Batch {}

    impl Batch {
        pub fn new() -> Self {
            let mut bufs = vec![0u8; SLOTS * MAX_DATAGRAM];
            let mut iovs: Vec<libc::iovec> = bufs
                .as_chunks_mut::<MAX_DATAGRAM>()
                .0
                .iter_mut()
                .map(|slot| libc::iovec {
                    iov_base: slot.as_mut_ptr().cast(),
                    iov_len: slot.len(),
                })
                .collect();
            let headers = iovs
                .iter_mut()
                .map(|iov| {
                    // SAFETY: mmsghdr is a plain C struct for which all-zero
                    // bytes is a valid value (null pointers, zero lengths).
                    let mut h: libc::mmsghdr = unsafe { std::mem::zeroed() };
                    h.msg_hdr.msg_iov = iov;
                    h.msg_hdr.msg_iovlen = 1;
                    h
                })
                .collect();
            Batch {
                bufs,
                iovs,
                headers,
            }
        }

        pub fn recv(&mut self, socket: &UdpSocket) -> io::Result<usize> {
            debug_assert_eq!(self.iovs.len(), SLOTS);
            for h in &mut self.headers {
                h.msg_len = 0;
                h.msg_hdr.msg_flags = 0;
            }
            // SAFETY: every header points at its own iovec, and every iovec
            // at its own MAX_DATAGRAM-byte slot of `bufs`; all three
            // vectors outlive the call and are not touched during it.
            let n = unsafe {
                libc::recvmmsg(
                    socket.as_raw_fd(),
                    self.headers.as_mut_ptr(),
                    SLOTS as libc::c_uint,
                    libc::MSG_WAITFORONE,
                    std::ptr::null_mut(),
                )
            };
            if n < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(n as usize)
            }
        }

        pub fn get(&self, i: usize) -> Option<&[u8]> {
            let h = &self.headers[i];
            if h.msg_hdr.msg_flags & libc::MSG_TRUNC != 0 {
                return None;
            }
            Some(&self.bufs[i * MAX_DATAGRAM..][..h.msg_len as usize])
        }
    }
}

fn receive(socket: UdpSocket, router: &Router, tenant: &Arc<str>, stop: &AtomicBool) {
    let mut datagrams = Datagrams::new();
    let mut pending = String::with_capacity(BATCH_BYTES + MAX_DATAGRAM);
    loop {
        match datagrams.recv(&socket) {
            Ok(n) => {
                // Counted on arrival: a batch of datagrams that are all
                // invalid routes nothing, and must still be counted.
                Stats::add(&router.stats().packets_received, n as u64);
                for i in 0..n {
                    match datagrams.get(i).map(std::str::from_utf8) {
                        Some(Ok(text)) => {
                            pending.push_str(text);
                            pending.push('\n');
                        }
                        _ => Stats::add(&router.stats().parse_errors, 1),
                    }
                }
                if pending.len() < BATCH_BYTES {
                    continue;
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if stop.load(Ordering::Relaxed) && pending.is_empty() {
                    return;
                }
            }
            Err(e) => {
                tracing::warn!("udp recv: {e}");
                continue;
            }
        }
        if !pending.is_empty() {
            router.route(unix_now(), tenant, &pending);
            pending.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn receives_bursts_large_and_invalid_datagrams() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let stats = Arc::new(Stats::default());
        let router = Arc::new(Router::new(vec![tx], stats.clone()));
        let stop = Arc::new(AtomicBool::new(false));
        let (addr, handles) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            1,
            router.clone(),
            "t".into(),
            stop.clone(),
        )
        .unwrap();

        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        // A burst, so one recvmmsg call returns several datagrams.
        for i in 0..200 {
            sender.send_to(format!("a:{i}|c").as_bytes(), addr).unwrap();
        }
        // One datagram of about 44 KB, well past a typical packet.
        let big: String = (0..5000).map(|i| format!("b:{i}|c\n")).collect();
        sender.send_to(big.as_bytes(), addr).unwrap();
        sender.send_to(&[0xff, 0xfe], addr).unwrap();

        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let s = stats.snapshot();
            if s.lines_received == 5200 && s.parse_errors == 1 {
                break;
            }
            assert!(Instant::now() < deadline, "only {s:?}");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        stop.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
        let routed: usize = rx.try_iter().map(|b| b.items.len()).sum();
        assert_eq!(routed, 5200);
        assert_eq!(stats.snapshot().packets_received, 202);
    }
}
