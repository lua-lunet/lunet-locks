//! lock-feed: serves a lock-event journal directory over REST + WebSocket.
//!
//! Reads the journal format produced by `lunet_advisory_lock::journal` and
//! exposes it to the console SPA. Hand-rolled HTTP/1.1 (no framework).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;

use lunet_advisory_lock::journal::{self, JournalEvent, Meta, RECORD_SIZE};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Config {
    dir: PathBuf,
    bind: SocketAddr,
    poll_ms: u64,
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut dir: Option<PathBuf> = None;
    let mut bind: Option<SocketAddr> = None;
    let mut poll_ms: u64 = 200;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dir" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("lock-feed: --dir requires a value");
                    process::exit(2);
                }
                dir = Some(PathBuf::from(&args[i]));
            }
            "--bind" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("lock-feed: --bind requires a value");
                    process::exit(2);
                }
                match args[i].parse() {
                    Ok(addr) => bind = Some(addr),
                    Err(e) => {
                        eprintln!("lock-feed: invalid --bind '{}': {e}", args[i]);
                        process::exit(2);
                    }
                }
            }
            "--poll-ms" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("lock-feed: --poll-ms requires a value");
                    process::exit(2);
                }
                match args[i].parse() {
                    Ok(v) => poll_ms = v,
                    Err(e) => {
                        eprintln!("lock-feed: invalid --poll-ms '{}': {e}", args[i]);
                        process::exit(2);
                    }
                }
            }
            other => {
                eprintln!("lock-feed: unknown argument '{other}'");
                process::exit(2);
            }
        }
        i += 1;
    }

    let dir = match dir {
        Some(d) => d,
        None => {
            eprintln!("lock-feed: --dir is required");
            process::exit(2);
        }
    };
    if !dir.is_dir() {
        eprintln!("lock-feed: directory does not exist: {}", dir.display());
        process::exit(2);
    }
    let bind = match bind {
        Some(b) => b,
        None => {
            eprintln!("lock-feed: --bind is required");
            process::exit(2);
        }
    };

    Config { dir, bind, poll_ms }
}

// ---------------------------------------------------------------------------
// Broadcast message types
// ---------------------------------------------------------------------------

/// Messages sent from the rescan task to WS clients.
#[derive(Debug, Clone)]
enum FeedMsg {
    Event(EventJson),
    Rolled(RolledJson),
}

#[derive(Debug, Clone)]
struct EventJson {
    kind: String,
    ts: u64,
    lock_id: u64,
    lease_id: u64,
    holder: String,
    expiry: u64,
}

impl EventJson {
    fn from_event(e: &JournalEvent) -> Self {
        let kind = match e.kind {
            journal::KIND_HOLD => "hold",
            journal::KIND_RENEW => "renew",
            journal::KIND_RELEASE => "release",
            _ => "unknown",
        };
        Self {
            kind: kind.to_string(),
            ts: e.ts,
            lock_id: e.lock_id,
            lease_id: e.lease_id,
            holder: hex_16(&e.holder),
            expiry: e.expiry,
        }
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "event",
            "kind": self.kind,
            "ts": self.ts,
            "lockId": self.lock_id,
            "leaseId": self.lease_id,
            "holder": self.holder,
            "expiry": self.expiry,
        })
    }
}

#[derive(Debug, Clone)]
struct RolledJson {
    file: String,
    op_min: u64,
    op_max: u64,
    expiry_min: u64,
    expiry_max: u64,
    count: u32,
    next: String,
}

impl RolledJson {
    fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "rolled",
            "file": self.file,
            "opMin": self.op_min,
            "opMax": self.op_max,
            "expiryMin": self.expiry_min,
            "expiryMax": self.expiry_max,
            "count": self.count,
            "next": self.next,
        })
    }
}

fn hex_16(bytes: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

// ---------------------------------------------------------------------------
// Directory scanning helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum FileEntry {
    Rolled { name: String, meta: Meta, size: u64 },
    Open { name: String, size: u64 },
}

impl FileEntry {
    fn to_json_value(&self) -> serde_json::Value {
        match self {
            Self::Rolled { name, meta, size } => serde_json::json!({
                "name": name,
                "opMin": meta.op_min,
                "opMax": meta.op_max,
                "expiryMin": meta.expiry_min,
                "expiryMax": meta.expiry_max,
                "count": meta.count,
                "size": size,
                "open": false,
            }),
            Self::Open { name, size } => serde_json::json!({
                "name": name,
                "size": size,
                "open": true,
            }),
        }
    }

    fn sort_key(&self) -> u64 {
        match self {
            Self::Rolled { meta, .. } => meta.op_min,
            Self::Open { .. } => u64::MAX,
        }
    }
}

fn scan_dir(dir: &Path) -> Vec<FileEntry> {
    let mut entries: Vec<FileEntry> = Vec::new();
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return entries,
    };

    let mut names: Vec<String> = Vec::new();
    for entry in rd.flatten() {
        if let Some(n) = entry.file_name().to_str() {
            names.push(n.to_string());
        }
    }

    for name in &names {
        if name.starts_with("ev-open-") && name.ends_with(".bin") {
            let path = dir.join(name);
            if let Ok(meta) = std::fs::metadata(&path) {
                entries.push(FileEntry::Open {
                    name: name.clone(),
                    size: meta.len(),
                });
            }
        } else if name.starts_with("ev-") && name.ends_with(".bin") && !name.starts_with("ev-open-")
        {
            let meta_name = name.strip_suffix(".bin").unwrap().to_string() + ".meta";
            if !names.iter().any(|n| n == &meta_name) {
                continue;
            }
            let bin_path = dir.join(name);
            let meta_path = dir.join(&meta_name);
            let bin_size = match std::fs::metadata(&bin_path) {
                Ok(m) => m.len(),
                Err(_) => continue,
            };
            let meta_bytes = match std::fs::read(&meta_path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let meta = match Meta::decode(&meta_bytes) {
                Some(m) => m,
                None => continue,
            };
            entries.push(FileEntry::Rolled {
                name: name.clone(),
                meta,
                size: bin_size,
            });
        }
    }

    entries.sort_by_key(|e| e.sort_key());
    entries
}

fn safe_file_path(dir: &Path, name: &str) -> Option<PathBuf> {
    if name.contains('/') || name.contains('\\') || name.contains("..") || name.is_empty() {
        return None;
    }
    let path = dir.join(name);
    if path.is_file() { Some(path) } else { None }
}

// ---------------------------------------------------------------------------
// PrefixedStream: prepends buffered bytes to an async stream
// ---------------------------------------------------------------------------

/// Wraps an async read/write stream with a prefix of bytes that are returned
/// first on read. Used to "unread" bytes consumed during HTTP request parsing
/// before handing the stream to the WebSocket acceptor.
struct PrefixedStream<S> {
    prefix: std::io::Cursor<Vec<u8>>,
    inner: S,
}

impl<S> PrefixedStream<S> {
    fn new(prefix: Vec<u8>, inner: S) -> Self {
        Self {
            prefix: std::io::Cursor::new(prefix),
            inner,
        }
    }
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for PrefixedStream<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // First drain the prefix.
        let pos = self.prefix.position();
        let len = self.prefix.get_ref().len() as u64;
        if pos < len {
            let remaining = &self.prefix.get_ref()[pos as usize..];
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            self.prefix.set_position(pos + to_copy as u64);
            return std::task::Poll::Ready(Ok(()));
        }
        // Then delegate to inner.
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for PrefixedStream<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// ---------------------------------------------------------------------------
// HTTP handling
// ---------------------------------------------------------------------------

async fn handle_connection(
    stream: tokio::net::TcpStream,
    dir: Arc<PathBuf>,
    tx: broadcast::Sender<FeedMsg>,
) {
    let mut stream = stream;
    let mut buf = vec![0u8; 8192];
    let n = match stream.read(&mut buf).await {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let request = match std::str::from_utf8(&buf[..n]) {
        Ok(s) => s.to_string(),
        Err(_) => {
            let _ = write_response(&mut stream, 400, "text/plain", b"Bad Request").await;
            return;
        }
    };

    let first_line = match request.lines().next() {
        Some(l) => l,
        None => {
            let _ = write_response(&mut stream, 400, "text/plain", b"Bad Request").await;
            return;
        }
    };
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() < 2 {
        let _ = write_response(&mut stream, 400, "text/plain", b"Bad Request").await;
        return;
    }
    let method = parts[0];
    let path = parts[1];

    // WebSocket upgrade on /ws.
    if path == "/ws" && is_ws_upgrade(&request) {
        // We already consumed bytes from the stream. Prepend them back
        // so accept_async can complete the handshake.
        let prefix = buf[..n].to_vec();
        let combined = PrefixedStream::new(prefix, stream);
        handle_ws(combined, Arc::clone(&dir), tx).await;
        return;
    }

    if method != "GET" {
        let _ = write_response(&mut stream, 405, "text/plain", b"Method Not Allowed").await;
        return;
    }

    if path == "/health" {
        let body = br#"{"ok":true}"#;
        let _ = write_response(&mut stream, 200, "application/json", body).await;
    } else if path == "/files" {
        let entries = scan_dir(&dir);
        let json_vals: Vec<serde_json::Value> = entries.iter().map(|e| e.to_json_value()).collect();
        let body = serde_json::to_vec(&json_vals).unwrap_or_default();
        let _ = write_response(&mut stream, 200, "application/json", &body).await;
    } else if let Some(name) = path.strip_prefix("/files/") {
        match safe_file_path(&dir, name) {
            Some(file_path) => match tokio::fs::read(&file_path).await {
                Ok(data) => {
                    let _ =
                        write_response(&mut stream, 200, "application/octet-stream", &data).await;
                }
                Err(_) => {
                    let _ = write_response(&mut stream, 404, "text/plain", b"Not Found").await;
                }
            },
            None => {
                let _ = write_response(&mut stream, 404, "text/plain", b"Not Found").await;
            }
        }
    } else {
        let _ = write_response(&mut stream, 404, "text/plain", b"Not Found").await;
    }
}

fn is_ws_upgrade(request: &str) -> bool {
    for line in request.lines() {
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("upgrade:") && lower.contains("websocket") {
            return true;
        }
    }
    false
}

async fn write_response(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Unknown",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

// ---------------------------------------------------------------------------
// WebSocket handling
// ---------------------------------------------------------------------------

async fn handle_ws<S>(stream: S, dir: Arc<PathBuf>, tx: broadcast::Sender<FeedMsg>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let ws = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("lock-feed: ws handshake failed: {e}");
            return;
        }
    };

    let (mut ws_sink, mut ws_stream) = ws.split();

    // Send backlog from current open file.
    let backlog = read_backlog(&dir);
    for event in &backlog {
        let json = event.to_json_value().to_string();
        if ws_sink.send(Message::Text(json.into())).await.is_err() {
            return;
        }
    }

    // Subscribe to live feed.
    let mut rx = tx.subscribe();

    // Forward broadcast messages to the WS client until disconnect.
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(feed_msg) => {
                        let json = match &feed_msg {
                            FeedMsg::Event(e) => e.to_json_value().to_string(),
                            FeedMsg::Rolled(r) => r.to_json_value().to_string(),
                        };
                        if ws_sink.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            frame = ws_stream.next() => {
                match frame {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => continue,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Series follower: inotify-driven on Linux, poll-driven everywhere
// ---------------------------------------------------------------------------

/// The wake channel the rescan loop selects on: directory events from the
/// inotify watcher on Linux, timer ticks from the poll loop everywhere.
type WakeRx = tokio::sync::mpsc::Receiver<()>;

/// Spawn the inotify watcher thread (Linux only): CREATE / MODIFY /
/// MOVED_TO / CLOSE_WRITE on the series directory push wakeups into the
/// rescan loop. The poll loop below stays as the safety net.
#[cfg(target_os = "linux")]
fn spawn_series_watcher(dir: &Path) -> WakeRx {
    let (tx, rx) = tokio::sync::mpsc::channel::<()>(64);
    let dir = dir.to_path_buf();
    std::thread::Builder::new()
        .name("aof-watcher".to_string())
        .spawn(move || {
            use inotify::{Inotify, WatchMask};

            let mut watcher = match Inotify::init() {
                Ok(w) => w,
                Err(_) => return, // the poll loop covers the series
            };
            if watcher
                .watches()
                .add(
                    &dir,
                    WatchMask::CREATE
                        | WatchMask::MODIFY
                        | WatchMask::MOVED_TO
                        | WatchMask::CLOSE_WRITE,
                )
                .is_err()
            {
                return;
            }
            let mut buf = [0u8; 4096];
            loop {
                // read_events is the blocking read: the thread sleeps in the
                // kernel until the series directory changes.
                match watcher.read_events(&mut buf) {
                    Ok(events) => {
                        if events.count() > 0 {
                            let _ = tx.try_send(());
                        }
                    }
                    Err(_) => return,
                }
            }
        })
        .expect("spawn aof-watcher");
    rx
}

/// No watcher off Linux: the poll loop is the follower. The retained
/// sender keeps the channel open so `recv` waits instead of spinning.
#[cfg(not(target_os = "linux"))]
fn spawn_series_watcher(_dir: &Path) -> WakeRx {
    let (tx, rx) = tokio::sync::mpsc::channel::<()>(1);
    std::mem::forget(tx);
    rx
}

// ---------------------------------------------------------------------------
// Rescan task
// ---------------------------------------------------------------------------

async fn rescan_task(
    dir: Arc<PathBuf>,
    tx: broadcast::Sender<FeedMsg>,
    poll_interval: Duration,
    mut wake: WakeRx,
) {
    let mut current_open: Option<String> = None;
    let mut current_offset: u64 = 0;
    // Track rolled files we've already reported to avoid duplicates.
    let mut reported_rolled: std::collections::HashSet<String> = std::collections::HashSet::new();

    loop {
        tokio::select! {
            _ = tokio::time::sleep(poll_interval) => {}
            _ = wake.recv() => {}
        }

        let open_name = match find_open_file_name(&dir) {
            Some(n) => n,
            None => continue,
        };

        // Detect roll: open file name changed, or first discovery with existing meta files.
        let roll_detected = match &current_open {
            Some(prev) if *prev != open_name => true,
            None => true,
            _ => false,
        };

        if roll_detected {
            // Scan for .meta files we haven't reported yet.
            if let Ok(rd) = std::fs::read_dir(&*dir) {
                for entry in rd.flatten() {
                    let fname = entry.file_name().to_string_lossy().to_string();
                    if !fname.ends_with(".meta") {
                        continue;
                    }
                    if reported_rolled.contains(&fname) {
                        continue;
                    }
                    let bin_name = fname.replace(".meta", ".bin");
                    if !dir.join(&bin_name).is_file() {
                        continue;
                    }
                    let meta_bytes = match std::fs::read(entry.path()) {
                        Ok(b) => b,
                        Err(_) => continue,
                    };
                    let meta = match Meta::decode(&meta_bytes) {
                        Some(m) => m,
                        None => continue,
                    };
                    let msg = FeedMsg::Rolled(RolledJson {
                        file: bin_name.clone(),
                        op_min: meta.op_min,
                        op_max: meta.op_max,
                        expiry_min: meta.expiry_min,
                        expiry_max: meta.expiry_max,
                        count: meta.count,
                        next: open_name.clone(),
                    });
                    let _ = tx.send(msg);
                    reported_rolled.insert(fname);
                }
            }
            current_offset = 0;
        }

        current_open = Some(open_name.clone());

        let open_path = dir.join(&open_name);
        let file_len = match std::fs::metadata(&open_path) {
            Ok(m) => m.len(),
            Err(_) => continue,
        };

        if file_len <= current_offset {
            continue;
        }

        let new_bytes = match read_file_range(&open_path, current_offset, file_len).await {
            Ok(b) => b,
            Err(_) => continue,
        };

        let mut off = 0;
        while off + RECORD_SIZE <= new_bytes.len() {
            if let Some((event, consumed)) = journal::parse_record(&new_bytes[off..]) {
                let msg = FeedMsg::Event(EventJson::from_event(&event));
                let _ = tx.send(msg);
                off += consumed;
            } else {
                break;
            }
        }

        current_offset += off as u64;
    }
}

fn find_open_file_name(dir: &Path) -> Option<String> {
    let rd = std::fs::read_dir(dir).ok()?;
    for entry in rd.flatten() {
        if let Some(n) = entry.file_name().to_str() {
            if n.starts_with("ev-open-") && n.ends_with(".bin") {
                return Some(n.to_string());
            }
        }
    }
    None
}

async fn read_file_range(path: &Path, start: u64, end: u64) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncSeekExt;
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let len = (end - start) as usize;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf).await?;
    Ok(buf)
}

fn read_backlog(dir: &Path) -> Vec<EventJson> {
    let open_name = match find_open_file_name(dir) {
        Some(n) => n,
        None => return Vec::new(),
    };
    let path = dir.join(&open_name);
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };
    journal::parse_file(&data)
        .iter()
        .map(EventJson::from_event)
        .collect()
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let config = parse_args();
    println!(
        "lock-feed: dir={} bind={}",
        config.dir.display(),
        config.bind
    );

    let dir = Arc::new(config.dir);
    let (tx, _rx) = broadcast::channel::<FeedMsg>(1024);

    let rescan_dir = Arc::clone(&dir);
    let rescan_tx = tx.clone();
    let poll_interval = Duration::from_millis(config.poll_ms);
    let wake = spawn_series_watcher(&dir);
    tokio::spawn(async move {
        rescan_task(rescan_dir, rescan_tx, poll_interval, wake).await;
    });

    let listener = match TcpListener::bind(config.bind).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("lock-feed: failed to bind {}: {e}", config.bind);
            process::exit(1);
        }
    };

    loop {
        let (stream, _addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("lock-feed: accept error: {e}");
                continue;
            }
        };
        let dir = Arc::clone(&dir);
        let tx = tx.clone();
        tokio::spawn(async move {
            handle_connection(stream, dir, tx).await;
        });
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use lunet_advisory_lock::aof::{AofConfig, AofWriter};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "lock-feed-test-{name}-{}-{}",
            std::process::id(),
            TEST_SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_event(kind: u8, ts: u64, lock_id: u64, expiry: u64) -> JournalEvent {
        JournalEvent {
            kind,
            ts,
            lock_id,
            lease_id: 42,
            holder: [0xAA; 16],
            expiry,
        }
    }

    fn write_rolled_file(dir: &Path, events: &[JournalEvent]) -> String {
        let mut op_min = u64::MAX;
        let mut op_max = u64::MIN;
        let mut expiry_min = u64::MAX;
        let mut expiry_max = u64::MIN;
        for e in events {
            op_min = op_min.min(e.ts);
            op_max = op_max.max(e.ts);
            expiry_min = expiry_min.min(e.expiry);
            expiry_max = expiry_max.max(e.expiry);
        }
        let meta = Meta {
            op_min,
            op_max,
            expiry_min,
            expiry_max,
            count: events.len() as u32,
        };
        let name = format!(
            "ev-{}-{}-{}-{}.bin",
            meta.op_min, meta.op_max, meta.expiry_min, meta.expiry_max
        );
        let mut data = Vec::new();
        for e in events {
            data.extend_from_slice(&e.encode());
        }
        fs::write(dir.join(&name), &data).unwrap();
        fs::write(dir.join(name.replace(".bin", ".meta")), meta.encode()).unwrap();
        name
    }

    fn write_open_file(dir: &Path, events: &[JournalEvent], ts: u64) -> String {
        let name = format!("ev-open-{ts}.bin");
        let mut data = Vec::new();
        for e in events {
            data.extend_from_slice(&e.encode());
        }
        fs::write(dir.join(&name), &data).unwrap();
        name
    }

    // ---- HTTP test helpers ----

    struct HttpResponse {
        status: u16,
        body: Vec<u8>,
    }

    async fn http_get(addr: SocketAddr, path: &str) -> HttpResponse {
        http_request(addr, "GET", path, b"").await
    }

    async fn http_request(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> HttpResponse {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        if !body.is_empty() {
            stream.write_all(body).await.unwrap();
        }
        stream.flush().await.unwrap();

        let mut resp_buf = Vec::new();
        stream.read_to_end(&mut resp_buf).await.unwrap();

        let resp_str = String::from_utf8_lossy(&resp_buf);
        let first_line = resp_str.lines().next().unwrap_or("");
        let status: u16 = first_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let body_start = resp_buf
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|i| i + 4)
            .unwrap_or(resp_buf.len());
        let body = resp_buf[body_start..].to_vec();

        HttpResponse { status, body }
    }

    /// Spawn a server that accepts one connection and handles it.
    async fn spawn_one_shot_server(
        dir: Arc<PathBuf>,
        tx: broadcast::Sender<FeedMsg>,
    ) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_connection(stream, dir, tx).await;
        });
        addr
    }

    // ---- REST tests ----

    #[tokio::test]
    async fn test_health() {
        let dir = temp_dir("health");
        let dir_arc = Arc::new(dir.clone());
        let (tx, _) = broadcast::channel(16);
        let addr = spawn_one_shot_server(dir_arc, tx).await;

        let resp = http_get(addr, "/health").await;
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, br#"{"ok":true}"#);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_files_listing() {
        let dir = temp_dir("files");
        let e1 = sample_event(journal::KIND_HOLD, 100, 1, 500);
        let e2 = sample_event(journal::KIND_RENEW, 200, 2, 600);
        write_rolled_file(&dir, &[e1, e2]);
        let e3 = sample_event(journal::KIND_HOLD, 300, 3, 700);
        write_open_file(&dir, &[e3], 1000);

        let dir_arc = Arc::new(dir.clone());
        let (tx, _) = broadcast::channel(16);
        let addr = spawn_one_shot_server(dir_arc, tx).await;

        let resp = http_get(addr, "/files").await;
        assert_eq!(resp.status, 200);
        let val: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        let arr = val.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["open"], false);
        assert_eq!(arr[0]["opMin"], 100);
        assert_eq!(arr[0]["opMax"], 200);
        assert_eq!(arr[0]["count"], 2);
        assert_eq!(arr[1]["open"], true);
        assert_eq!(arr[1]["name"], "ev-open-1000.bin");
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_files_get_exact_bytes() {
        let dir = temp_dir("files-get");
        let e1 = sample_event(journal::KIND_HOLD, 100, 1, 500);
        let name = write_rolled_file(&dir, std::slice::from_ref(&e1));

        let dir_arc = Arc::new(dir.clone());
        let (tx, _) = broadcast::channel(16);
        let addr = spawn_one_shot_server(dir_arc, tx).await;

        let resp = http_get(addr, &format!("/files/{name}")).await;
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, e1.encode());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_files_404_missing() {
        let dir = temp_dir("files-404");
        let dir_arc = Arc::new(dir.clone());
        let (tx, _) = broadcast::channel(16);
        let addr = spawn_one_shot_server(dir_arc, tx).await;

        let resp = http_get(addr, "/files/nope.bin").await;
        assert_eq!(resp.status, 404);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_files_traversal_404() {
        let dir = temp_dir("traversal");
        let dir_arc = Arc::new(dir.clone());
        let (tx, _) = broadcast::channel(16);
        let addr = spawn_one_shot_server(dir_arc, tx).await;

        let resp = http_get(addr, "/files/../etc/passwd").await;
        assert_eq!(resp.status, 404);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_method_not_allowed() {
        let dir = temp_dir("method");
        let dir_arc = Arc::new(dir.clone());
        let (tx, _) = broadcast::channel(16);
        let addr = spawn_one_shot_server(dir_arc, tx).await;

        let resp = http_request(addr, "POST", "/health", b"").await;
        assert_eq!(resp.status, 405);
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- WS test helpers ----

    /// Read WS messages until one matches the predicate, or timeout.
    async fn ws_wait_for<F>(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        timeout_dur: Duration,
        pred: F,
    ) -> serde_json::Value
    where
        F: Fn(&serde_json::Value) -> bool,
    {
        use futures_util::StreamExt;
        let deadline = tokio::time::Instant::now() + timeout_dur;
        loop {
            let remaining = deadline.duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                panic!("ws_wait_for: timed out");
            }
            let msg = tokio::time::timeout(remaining, ws.next())
                .await
                .expect("timed out")
                .expect("stream ended")
                .expect("ws error");
            let text = msg.into_text().unwrap();
            let val: serde_json::Value = serde_json::from_str(&text).unwrap();
            if pred(&val) {
                return val;
            }
            // Otherwise skip (duplicate or unrelated message).
        }
    }

    // ---- WS tests ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ws_backlog_and_append() {
        let dir = temp_dir("ws-backlog");
        let e1 = sample_event(journal::KIND_HOLD, 100, 1, 500);
        write_open_file(&dir, &[e1], 1000);

        let dir_arc = Arc::new(dir.clone());
        let (tx, _) = broadcast::channel(256);

        let rescan_dir = Arc::clone(&dir_arc);
        let rescan_tx = tx.clone();
        let wake = spawn_series_watcher(&dir);
        tokio::spawn(async move {
            rescan_task(rescan_dir, rescan_tx, Duration::from_millis(50), wake).await;
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serve_dir = Arc::clone(&dir_arc);
        let serve_tx = tx.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let d = Arc::clone(&serve_dir);
                let t = serve_tx.clone();
                tokio::spawn(async move {
                    handle_connection(stream, d, t).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let url = format!("ws://{addr}/ws");
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        // Receive an event with ts=100 (backlog or rescan, either is fine).
        let val = ws_wait_for(&mut ws, Duration::from_secs(2), |v| {
            v["type"] == "event" && v["ts"] == 100
        })
        .await;
        assert_eq!(val["kind"], "hold");

        // Append a new event.
        let e2 = sample_event(journal::KIND_RENEW, 200, 2, 600);
        let open_path = dir.join("ev-open-1000.bin");
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(&open_path)
            .unwrap();
        use std::io::Write;
        f.write_all(&e2.encode()).unwrap();
        f.flush().unwrap();
        drop(f);

        // Wait for the renew event with ts=200.
        let val = ws_wait_for(&mut ws, Duration::from_secs(3), |v| {
            v["type"] == "event" && v["ts"] == 200
        })
        .await;
        assert_eq!(val["kind"], "renew");

        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ws_roll_detection() {
        let dir = temp_dir("ws-roll");
        let e1 = sample_event(journal::KIND_HOLD, 100, 1, 500);
        write_open_file(&dir, &[e1], 1000);

        let dir_arc = Arc::new(dir.clone());
        let (tx, _) = broadcast::channel(256);

        let rescan_dir = Arc::clone(&dir_arc);
        let rescan_tx = tx.clone();
        let wake = spawn_series_watcher(&dir);
        tokio::spawn(async move {
            rescan_task(rescan_dir, rescan_tx, Duration::from_millis(50), wake).await;
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serve_dir = Arc::clone(&dir_arc);
        let serve_tx = tx.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let d = Arc::clone(&serve_dir);
                let t = serve_tx.clone();
                tokio::spawn(async move {
                    handle_connection(stream, d, t).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let url = format!("ws://{addr}/ws");
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        // Receive any event (backlog).
        let _val = ws_wait_for(&mut ws, Duration::from_secs(2), |v| v["type"] == "event").await;

        // Simulate roll.
        let old_open = dir.join("ev-open-1000.bin");
        let rolled_name = "ev-100-100-500-500.bin";
        let rolled_path = dir.join(rolled_name);
        fs::rename(&old_open, &rolled_path).unwrap();
        let meta = Meta {
            op_min: 100,
            op_max: 100,
            expiry_min: 500,
            expiry_max: 500,
            count: 1,
        };
        fs::write(dir.join("ev-100-100-500-500.meta"), meta.encode()).unwrap();
        let e2 = sample_event(journal::KIND_RELEASE, 300, 3, 700);
        let new_open_name = "ev-open-2000.bin";
        fs::write(dir.join(new_open_name), e2.encode()).unwrap();

        // Wait for rolled message.
        let val = ws_wait_for(&mut ws, Duration::from_secs(3), |v| v["type"] == "rolled").await;
        assert_eq!(val["file"], rolled_name);
        assert_eq!(val["opMin"], 100);
        assert_eq!(val["count"], 1);
        assert_eq!(val["next"], new_open_name);

        // Wait for event from new open file.
        let val = ws_wait_for(&mut ws, Duration::from_secs(3), |v| {
            v["type"] == "event" && v["ts"] == 300
        })
        .await;
        assert_eq!(val["kind"], "release");

        let _ = fs::remove_dir_all(&dir);
    }

    /// Feed over the AOF series: the async write-behind writer produces the
    /// file series, the follower tails it, and the server hands the events
    /// to clients — no journal anywhere in the path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_feed_over_aof_series() {
        let dir = temp_dir("aof-feed");
        let writer = AofWriter::open(
            &dir,
            AofConfig {
                flush_bytes: 1,
                flush_interval: None,
                queue_cap: 64,
            },
        )
        .unwrap();

        let dir_arc = Arc::new(dir.clone());
        let (tx, _) = broadcast::channel(256);

        let rescan_dir = Arc::clone(&dir_arc);
        let rescan_tx = tx.clone();
        let wake = spawn_series_watcher(&dir);
        tokio::spawn(async move {
            rescan_task(rescan_dir, rescan_tx, Duration::from_millis(50), wake).await;
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serve_dir = Arc::clone(&dir_arc);
        let serve_tx = tx.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let d = Arc::clone(&serve_dir);
                let t = serve_tx.clone();
                tokio::spawn(async move {
                    handle_connection(stream, d, t).await;
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let url = format!("ws://{addr}/ws");
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        // The writer lands bytes in the AOF; the feed picks them up.
        writer.enqueue(sample_event(journal::KIND_HOLD, 700, 21, 1200));
        writer.enqueue(sample_event(journal::KIND_RENEW, 800, 21, 1300));

        let val = ws_wait_for(&mut ws, Duration::from_secs(5), |v| {
            v["type"] == "event" && v["ts"] == 700
        })
        .await;
        assert_eq!(val["kind"], "hold");

        let val = ws_wait_for(&mut ws, Duration::from_secs(5), |v| {
            v["type"] == "event" && v["ts"] == 800
        })
        .await;
        assert_eq!(val["kind"], "renew");

        // The REST listing serves the same AOF series.
        let resp = http_get(addr, "/files").await;
        assert_eq!(resp.status, 200);
        let listing: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(listing.as_array().unwrap().len(), 1);
        assert_eq!(listing[0]["open"], true);

        // The file endpoint returns the raw AOF bytes.
        let name = listing[0]["name"].as_str().unwrap().to_string();
        let resp = http_get(addr, &format!("/files/{name}")).await;
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.len(), 2 * RECORD_SIZE);

        // Only two events through a 64-slot queue: nothing dropped.
        assert_eq!(writer.drops(), 0);
        drop(writer); // graceful shutdown: drains and fsyncs
        let _ = fs::remove_dir_all(&dir);
    }
}
