//! Replays the generated workload against a Funnel server.
//!
//!     loadgen [--udp HOST:PORT | --http HOST:PORT] [--rate LINES_PER_SEC]
//!             [--duration SECS] [--threads N] [--packet-bytes N]
//!
//! Lines are generated once up front (a minute of the workload by default)
//! and replayed in a loop, so the sender spends its time sending. A rate of
//! 0 sends as fast as possible. Each thread uses its own socket, so the
//! server's SO_REUSEPORT receivers all get traffic.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct Args {
    udp: Option<SocketAddr>,
    http: Option<SocketAddr>,
    rate: u64,
    duration: u64,
    threads: usize,
    packet_bytes: usize,
    pool_secs: u64,
}

fn parse_args() -> Args {
    let mut args = Args {
        udp: None,
        http: None,
        rate: 0,
        duration: 10,
        threads: 2,
        packet_bytes: 1432,
        pool_secs: 60,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--udp" => args.udp = Some(value().parse().expect("--udp HOST:PORT")),
            "--http" => args.http = Some(value().parse().expect("--http HOST:PORT")),
            "--rate" => args.rate = value().parse().expect("--rate number"),
            "--duration" => args.duration = value().parse().expect("--duration secs"),
            "--threads" => args.threads = value().parse().expect("--threads number"),
            "--packet-bytes" => args.packet_bytes = value().parse().expect("--packet-bytes"),
            "--pool-secs" => args.pool_secs = value().parse().expect("--pool-secs"),
            other => panic!("unknown flag {other}"),
        }
    }
    if args.udp.is_none() && args.http.is_none() {
        args.udp = Some("127.0.0.1:8125".parse().unwrap());
    }
    args
}

/// Packs lines into payloads of at most `max` bytes, newline separated.
fn pack(lines: &[String], max: usize) -> Vec<(Vec<u8>, u64)> {
    let mut out = Vec::new();
    let mut cur = Vec::with_capacity(max);
    let mut n = 0;
    for l in lines {
        if !cur.is_empty() && cur.len() + 1 + l.len() > max {
            out.push((std::mem::take(&mut cur), n));
            n = 0;
        }
        if !cur.is_empty() {
            cur.push(b'\n');
        }
        cur.extend_from_slice(l.as_bytes());
        n += 1;
    }
    if !cur.is_empty() {
        out.push((cur, n));
    }
    out
}

fn main() {
    let args = parse_args();
    let mut workload = loadgen::Workload::new(loadgen::Config::default());
    let mut lines = Vec::new();
    for _ in 0..args.pool_secs {
        workload.next_second(&mut lines);
    }
    let max = if args.http.is_some() {
        256 * 1024
    } else {
        args.packet_bytes
    };
    let payloads = Arc::new(pack(&lines, max));
    eprintln!(
        "{} lines in {} payloads of up to {max} bytes; {} threads, rate {}",
        lines.len(),
        payloads.len(),
        args.threads,
        if args.rate == 0 {
            "max".to_string()
        } else {
            format!("{}/s", args.rate)
        }
    );

    let sent = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(args.duration);
    let per_thread_rate = args.rate as f64 / args.threads as f64;
    let start = Instant::now();
    let handles: Vec<_> = (0..args.threads)
        .map(|t| {
            let (payloads, sent) = (payloads.clone(), sent.clone());
            let (udp, http) = (args.udp, args.http);
            std::thread::spawn(move || {
                let udp = udp.map(|addr| {
                    let s = UdpSocket::bind("0.0.0.0:0").unwrap();
                    s.connect(addr).unwrap();
                    s
                });
                let mut http = http.map(|addr| (addr, TcpStream::connect(addr).unwrap()));
                let begin = Instant::now();
                let mut mine = 0u64;
                let mut i = t * payloads.len() / args.threads.max(1);
                while Instant::now() < deadline {
                    let (payload, n) = &payloads[i % payloads.len()];
                    i += 1;
                    if let Some(s) = &udp {
                        // A full kernel buffer on the sending side is not the
                        // server's fault; skip and count only what was sent.
                        if s.send(payload).is_err() {
                            continue;
                        }
                    }
                    if let Some((addr, stream)) = &mut http
                        && post(stream, payload).is_err()
                    {
                        *stream = TcpStream::connect(*addr).unwrap();
                        continue;
                    }
                    mine += n;
                    sent.fetch_add(*n, Ordering::Relaxed);
                    if per_thread_rate > 0.0 {
                        let due = Duration::from_secs_f64(mine as f64 / per_thread_rate);
                        if let Some(wait) = due.checked_sub(begin.elapsed()) {
                            std::thread::sleep(wait);
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let secs = start.elapsed().as_secs_f64();
    let total = sent.load(Ordering::Relaxed);
    println!(
        "{{\"lines_sent\":{total},\"seconds\":{secs:.3},\"lines_per_sec\":{:.0}}}",
        total as f64 / secs
    );
}

/// One HTTP/1.1 POST on a kept-alive connection; reads the whole response.
fn post(stream: &mut TcpStream, body: &[u8]) -> std::io::Result<()> {
    let head = format!(
        "POST /ingest HTTP/1.1\r\nhost: funnel\r\ncontent-type: text/plain\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut len = 0usize;
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        if line == "\r\n" {
            break;
        }
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            len = v.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0; len];
    reader.read_exact(&mut body)
}
