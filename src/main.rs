//! `horizon-web-relay`: the browser build's Play with Friends. A browser can't listen for
//! connections, so the host's page and each friend's page connect here, over WebSockets, and
//! the relay pairs them by a short code and carries the game's bytes between them unchanged.
//!
//! - `GET /host` (WebSocket): the host. The relay sends `{"code":"K7Q2MX"}`, then
//!   `{"guest":N}` for each friend who joins; the code lasts as long as this socket.
//! - `GET /join/CODE`: a friend. Once the host took it the relay sends `{"ok":true}`, then the
//!   game's bytes go both ways as binary messages; `{"error":"..."}` and a close otherwise.
//! - `GET /accept/CODE/N`: the host's end for friend `N`, opened when it was told of it.
//!
//! Up to `MAX_GUESTS` friends play in a world at once (4 players with the host). Listens on
//! `$PORT` (8000 when unset), as Koyeb and most hosts set it; TLS is the host's (`wss://`).

use futures_util::{SinkExt, StreamExt};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{
    tungstenite::{
        handshake::server::{Request, Response},
        protocol::WebSocketConfig,
        Message,
    },
    WebSocketStream,
};

/// Friends in one world at once: four players with the host.
const MAX_GUESTS: u32 = 3;
/// Worlds shared at once.
const MAX_HOSTS: usize = 500;
/// How long a friend waits for the host to take it.
const ACCEPT_WAIT: Duration = Duration::from_secs(20);
/// Pings keep idle connections open behind proxies that close quiet ones.
const PING_EVERY: Duration = Duration::from_secs(20);
/// Crockford's base32: no I, L, O or U.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const CODE_LEN: usize = 6;

type Ws = WebSocketStream<TcpStream>;

struct Host {
    control: mpsc::UnboundedSender<Message>,
    /// Friends waiting for the host's `/accept`, by number.
    waiting: HashMap<u32, oneshot::Sender<Ws>>,
    next: u32,
    /// Friends playing or waiting.
    guests: Arc<AtomicU32>,
}

#[derive(Default)]
struct Relay {
    hosts: Mutex<HashMap<String, Host>>,
}

/// A code as typed: case, spaces and dashes don't matter, O reads as 0, I and L as 1.
fn normalize_code(typed: &str) -> String {
    typed
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| match c.to_ascii_uppercase() {
            'O' => '0',
            'I' | 'L' => '1',
            c => c,
        })
        .collect()
}

fn new_code() -> String {
    let mut bytes = [0u8; CODE_LEN];
    getrandom::fill(&mut bytes).expect("the system's random numbers");
    bytes
        .iter()
        .map(|b| ALPHABET[usize::from(b % 32)] as char)
        .collect()
}

fn json_error(message: &str) -> Message {
    Message::text(format!("{{\"error\":\"{}\"}}", message.replace('"', "'")))
}

/// Which socket this is, from the request's path.
#[derive(Debug, PartialEq)]
enum Route {
    Host,
    Join(String),
    Accept(String, u32),
    Unknown,
}

fn route(path: &str) -> Route {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    match parts.as_slice() {
        ["host"] => Route::Host,
        ["join", code] => Route::Join(normalize_code(code)),
        ["accept", code, n] => match n.parse() {
            Ok(n) => Route::Accept(normalize_code(code), n),
            Err(_) => Route::Unknown,
        },
        _ => Route::Unknown,
    }
}

// The handshake callback's error type is tungstenite's (`ErrorResponse`), not ours to shrink.
#[allow(clippy::result_large_err)]
async fn serve(relay: Arc<Relay>, stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let mut path = String::new();
    let config = WebSocketConfig::default()
        .max_message_size(Some(16 << 20))
        .max_frame_size(Some(16 << 20));
    let ws = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        |request: &Request, response: Response| {
            path = request.uri().path().to_owned();
            Ok(response)
        },
        Some(config),
    )
    .await;
    let Ok(ws) = ws else {
        return;
    };
    match route(&path) {
        Route::Host => host(relay, ws).await,
        Route::Join(code) => join(relay, ws, code).await,
        Route::Accept(code, n) => accept(relay, ws, code, n),
        Route::Unknown => {
            let mut ws = ws;
            let _ = ws.send(json_error("unknown path")).await;
            let _ = ws.close(None).await;
        }
    }
}

/// A new code for a host, `None` when the relay has all the hosts it takes.
fn register(relay: &Relay, control: mpsc::UnboundedSender<Message>) -> Option<String> {
    let mut hosts = relay.hosts.lock().unwrap();
    if hosts.len() >= MAX_HOSTS {
        return None;
    }
    let code = loop {
        let code = new_code();
        if !hosts.contains_key(&code) {
            break code;
        }
    };
    hosts.insert(
        code.clone(),
        Host {
            control,
            waiting: HashMap::new(),
            next: 1,
            guests: Arc::new(AtomicU32::new(0)),
        },
    );
    Some(code)
}

/// A friend's place in the world, or why there is none.
fn take_place(
    relay: &Relay,
    code: &str,
) -> Result<(oneshot::Receiver<Ws>, Arc<AtomicU32>), &'static str> {
    let mut hosts = relay.hosts.lock().unwrap();
    let Some(host) = hosts.get_mut(code) else {
        return Err("no world is shared with that code");
    };
    if host.guests.load(Ordering::Relaxed) >= MAX_GUESTS {
        return Err("that world is full (4 players)");
    }
    host.guests.fetch_add(1, Ordering::Relaxed);
    let n = host.next;
    host.next += 1;
    let (tx, rx) = oneshot::channel();
    host.waiting.insert(n, tx);
    let _ = host
        .control
        .send(Message::text(format!("{{\"guest\":{n}}}")));
    Ok((rx, host.guests.clone()))
}

async fn host(relay: Arc<Relay>, ws: Ws) {
    let (control, mut outbox) = mpsc::unbounded_channel();
    let code = register(&relay, control.clone());
    let Some(code) = code else {
        let mut ws = ws;
        let _ = ws
            .send(json_error("the relay is full, try again later"))
            .await;
        return;
    };
    let (mut sink, mut source) = ws.split();
    let _ = control.send(Message::text(format!("{{\"code\":\"{code}\"}}")));
    let mut ping = tokio::time::interval(PING_EVERY);
    loop {
        tokio::select! {
            message = outbox.recv() => match message {
                Some(m) => if sink.send(m).await.is_err() { break },
                None => break,
            },
            incoming = source.next() => match incoming {
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            _ = ping.tick() => if sink.send(Message::Ping(Vec::new().into())).await.is_err() { break },
        }
    }
    // The code ends with the host's socket; friends still waiting are told by their sender
    // closing.
    relay.hosts.lock().unwrap().remove(&code);
}

async fn join(relay: Arc<Relay>, mut ws: Ws, code: String) {
    let (taken, guests) = match take_place(&relay, &code) {
        Ok(place) => place,
        Err(why) => {
            let _ = ws.send(json_error(why)).await;
            let _ = ws.close(None).await;
            return;
        }
    };
    // Counted from here: a friend that leaves (or never gets in) frees its place.
    struct Place(Arc<AtomicU32>);
    impl Drop for Place {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }
    let _place = Place(guests);
    let other = match tokio::time::timeout(ACCEPT_WAIT, taken).await {
        Ok(Ok(other)) => other,
        _ => {
            let _ = ws.send(json_error("the host didn't answer")).await;
            let _ = ws.close(None).await;
            return;
        }
    };
    if ws.send(Message::text("{\"ok\":true}")).await.is_err() {
        return;
    }
    carry(ws, other).await;
}

fn accept(relay: Arc<Relay>, ws: Ws, code: String, n: u32) {
    let waiting = relay
        .hosts
        .lock()
        .unwrap()
        .get_mut(&code)
        .and_then(|h| h.waiting.remove(&n));
    if let Some(waiting) = waiting {
        let _ = waiting.send(ws);
    }
}

/// The game's bytes between a friend and the host's end for it, until either closes.
async fn carry(a: Ws, b: Ws) {
    let (mut a_sink, mut a_source) = a.split();
    let (mut b_sink, mut b_source) = b.split();
    let mut ping = tokio::time::interval(PING_EVERY);
    loop {
        tokio::select! {
            m = a_source.next() => match m {
                Some(Ok(m @ Message::Binary(_))) => if b_sink.send(m).await.is_err() { break },
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            m = b_source.next() => match m {
                Some(Ok(m @ Message::Binary(_))) => if a_sink.send(m).await.is_err() { break },
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            _ = ping.tick() => {
                let ping = Message::Ping(Vec::new().into());
                if a_sink.send(ping.clone()).await.is_err() || b_sink.send(ping).await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = a_sink.close().await;
    let _ = b_sink.close().await;
}

async fn run(listener: TcpListener) {
    let relay = Arc::new(Relay::default());
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(serve(relay.clone(), stream));
    }
}

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(8000);
    let listener = TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap_or_else(|e| {
            eprintln!("horizon-web-relay: can't listen on port {port}: {e}");
            std::process::exit(1);
        });
    println!("horizon-web-relay listening on port {port}");
    run(listener).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::connect_async;

    #[test]
    fn codes_are_typed_loosely() {
        assert_eq!(normalize_code("k7q-2mx"), "K7Q2MX");
        assert_eq!(normalize_code(" O I L "), "011");
        assert_eq!(route("/join/k7q-2mx"), Route::Join("K7Q2MX".into()));
        assert_eq!(route("/accept/K7Q2MX/3"), Route::Accept("K7Q2MX".into(), 3));
        assert_eq!(route("/host"), Route::Host);
        assert_eq!(route("/elsewhere"), Route::Unknown);
        let code = new_code();
        assert_eq!(code.len(), CODE_LEN);
        assert!(code.bytes().all(|b| ALPHABET.contains(&b)));
    }

    async fn text(
        ws: &mut WebSocketStream<impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>,
    ) -> String {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(t) => return t.to_string(),
                _ => continue,
            }
        }
    }

    async fn binary(
        ws: &mut WebSocketStream<impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>,
    ) -> Vec<u8> {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Binary(b) => return b.to_vec(),
                _ => continue,
            }
        }
    }

    #[tokio::test]
    async fn a_host_and_three_friends_play_and_a_fourth_is_turned_away() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let at = listener.local_addr().unwrap();
        tokio::spawn(run(listener));
        let url = |path: &str| format!("ws://{at}{path}");

        let (mut host, _) = connect_async(url("/host")).await.unwrap();
        let hello = text(&mut host).await;
        let code = hello.split('"').nth(3).unwrap().to_owned();
        assert_eq!(code.len(), CODE_LEN);

        let mut friends = Vec::new();
        for i in 1..=3u8 {
            // Typed with a dash and in lower case, as a player would.
            let typed = format!("{}-{}", &code[..3], &code[3..]).to_lowercase();
            let (mut friend, _) = connect_async(url(&format!("/join/{typed}"))).await.unwrap();
            let told = text(&mut host).await;
            let n: u32 = told
                .trim_matches(|c: char| !c.is_ascii_digit())
                .parse()
                .unwrap();
            let (mut end, _) = connect_async(url(&format!("/accept/{code}/{n}")))
                .await
                .unwrap();
            assert_eq!(text(&mut friend).await, "{\"ok\":true}");
            // Bytes both ways, unchanged.
            friend.send(Message::binary(vec![i, 1, 2])).await.unwrap();
            assert_eq!(binary(&mut end).await, [i, 1, 2]);
            end.send(Message::binary(vec![9, i])).await.unwrap();
            assert_eq!(binary(&mut friend).await, [9, i]);
            friends.push((friend, end));
        }

        let (mut fourth, _) = connect_async(url(&format!("/join/{code}"))).await.unwrap();
        assert!(text(&mut fourth).await.contains("full"));

        // One leaves: its place is free again.
        let (friend, _end) = friends.pop().unwrap();
        drop(friend);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (mut again, _) = connect_async(url(&format!("/join/{code}"))).await.unwrap();
        assert!(text(&mut host).await.contains("guest"));
        drop(again.close(None).await);

        let (mut wrong, _) = connect_async(url("/join/ZZZZZZ")).await.unwrap();
        assert!(text(&mut wrong).await.contains("no world"));

        // The host leaves: the code ends.
        drop(host);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (mut late, _) = connect_async(url(&format!("/join/{code}"))).await.unwrap();
        assert!(text(&mut late).await.contains("no world"));
    }
}
