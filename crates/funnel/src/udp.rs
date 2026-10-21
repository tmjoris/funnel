//! StatsD over UDP. Each receiver thread owns a socket bound to the same
//! port with SO_REUSEPORT, so the kernel spreads packets across threads.
//! Packets are gathered for up to `BATCH_BYTES` or a 2 ms lull and routed
//! together, which keeps channel traffic per shard low.

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

fn receive(socket: UdpSocket, router: &Router, tenant: &Arc<str>, stop: &AtomicBool) {
    let mut buf = vec![0u8; 65536];
    let mut pending = String::with_capacity(BATCH_BYTES + buf.len());
    let mut packets = 0;
    loop {
        match socket.recv(&mut buf) {
            Ok(n) => {
                packets += 1;
                match std::str::from_utf8(&buf[..n]) {
                    Ok(text) => {
                        pending.push_str(text);
                        pending.push('\n');
                    }
                    Err(_) => Stats::add(&router.stats().parse_errors, 1),
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
            Stats::add(&router.stats().packets_received, packets);
            pending.clear();
            packets = 0;
        }
    }
}
