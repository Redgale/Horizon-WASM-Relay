//! The chess lobby, attached to the same port as the rest of the relay under `/chess`.
//!
//! One WebSocket per player, JSON text messages in both directions:
//!
//! - first message from a client: `{"t":"hi","name":..,"rating":..,"level":..,"hue":..,"eq":{..}}`; the
//!   relay answers `{"t":"welcome","me":{..},"users":[..],"games":[..]}`
//! - presence: `u+` (someone came), `u-` (someone left), `u~` (profile or status changed)
//! - challenges: `ch` (ask someone), `chx` (withdraw), `acc` / `dec` (answer); `chi` (you were asked),
//!   `chr` (your challenge ended), `chc` (a challenge to you was withdrawn)
//! - `match`: both players were paired. From then on `{"t":"g","d":"<text>"}` is passed to the
//!   opponent untouched; the relay also reads `start`, `move` and `abort` inside it so that
//!   spectators can follow, and `{"t":"over","r":..,"why":..}` from a player ends the record
//! - spectating: `spec` / `unspec`, answered by `snap`, then `sm` (a move), `sx` (game over)
//! - `list` -> `glist`: the games that can be watched
//!
//! - version: `hi` carries the page's build version (`ver`). The relay embeds the latest page
//!   (`static/blitz-chess.html`, served by a plain `GET /chess`); an older `ver` is answered with
//!   `{"t":"update","ver":..,"url":..}` and a missing one (copies made before versions existed) with an
//!   `err` saying where to get the new page. Either way the connection then ends.
//!
//! The relay never judges a game (it has no chess rules): the two players' pages do that, exactly as
//! in a LAN game. It keeps the list of moves so that someone arriving late can catch up.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

/// Biggest message accepted from a chess client.
pub const MAX_MSG: usize = 8 * 1024;
const MAX_USERS: usize = 400;
const MAX_SPECTATORS: usize = 100;
const MAX_PLIES: usize = 1000;
const MAX_INCOMING: usize = 5;
const CHALLENGE_TTL: Duration = Duration::from_secs(30);
const HELLO_WAIT: Duration = Duration::from_secs(10);
const PING_EVERY: Duration = Duration::from_secs(20);
const IDLE_LIMIT: Duration = Duration::from_secs(75);
/// More than this many messages in a second from one client and it is cut off.
const MAX_PER_SECOND: u32 = 60;

type Ws = WebSocketStream<TcpStream>;
type Tx = mpsc::UnboundedSender<Message>;

/// The latest Blitz Chess page (copied here by the page's build), served at `GET /chess`.
static PAGE: &[u8] = include_bytes!("../static/blitz-chess.html");

/// The embedded page's build version, from its `<meta name="bc-version" content="N">` (0 if it has none).
pub fn page_version() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        let head = String::from_utf8_lossy(&PAGE[..PAGE.len().min(4096)]);
        head.split("name=\"bc-version\" content=\"")
            .nth(1)
            .and_then(|r| r.split('"').next())
            .and_then(|n| n.trim().parse().ok())
            .unwrap_or(0)
    })
}

/// Where this relay's page is, as the client reached it: `https://<Host>/chess` (http for local addresses).
pub fn page_url(head: &str) -> String {
    let header = |name: &str| {
        head.lines()
            .skip(1)
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.trim().eq_ignore_ascii_case(name))
            .map(|(_, v)| v.trim().to_owned())
    };
    let Some(host) = header("host").filter(|h| !h.is_empty() && !h.contains(['/', ' ', '"', '<', '>'])) else {
        return String::new();
    };
    let local = {
        let h = host.trim_start_matches('[').split(']').next().unwrap_or("");
        let h = h.split(':').next().unwrap_or("");
        h == "localhost" || h.starts_with("127.") || h.starts_with("10.") || h.starts_with("192.168.") || h.is_empty()
    };
    let scheme = header("x-forwarded-proto")
        .filter(|p| p == "http" || p == "https")
        .unwrap_or_else(|| if local { "http".into() } else { "https".into() });
    format!("{scheme}://{host}/chess")
}

/// A plain `GET /chess` (or `HEAD`): the latest page. Any origin may fetch it (a page opened from a file
/// downloads its update this way).
pub async fn serve_page(stream: &mut TcpStream, head: &str) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut sink = [0u8; 4096];
    let _ = stream.read(&mut sink).await;
    let reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-cache\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Expose-Headers: X-Blitz-Version\r\nX-Blitz-Version: {}\r\nConnection: close\r\n\r\n",
        PAGE.len(),
        page_version()
    );
    let _ = stream.write_all(reply.as_bytes()).await;
    if !head.get(..5).is_some_and(|m| m.eq_ignore_ascii_case("head ")) {
        let _ = stream.write_all(PAGE).await;
    }
    let _ = stream.shutdown().await;
}

/// Does this (peeked) HTTP request line + headers ask for the chess socket?
pub fn wants_chess(head: &str) -> bool {
    let line = head.lines().next().unwrap_or("");
    let target = line.split_whitespace().nth(1).unwrap_or("");
    let path = target.split('?').next().unwrap_or("");
    path == "/chess" || path.starts_with("/chess/")
}

/* ---------------------------------------------------------------- who is who */

#[derive(Clone)]
struct Info {
    name: String,
    rating: u32,
    level: u32,
    hue: u32,
    pieces: String,
    acc: String,
    fx: String,
}

fn clean_name(raw: &str) -> String {
    let s: String = raw
        .chars()
        .filter(|c| !c.is_control() && !matches!(c, '<' | '>' | '"' | '\\'))
        .collect();
    let s: String = s.trim().chars().take(16).collect();
    if s.is_empty() {
        "Player".into()
    } else {
        s
    }
}

fn clean_id(v: Option<&Value>, default: &str) -> String {
    match v.and_then(Value::as_str) {
        Some(s) if s.len() <= 24 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') => s.to_owned(),
        _ => default.to_owned(),
    }
}

fn num(v: &Value, key: &str, lo: u32, hi: u32, default: u32) -> u32 {
    v.get(key)
        .and_then(Value::as_f64)
        .map(|f| f.clamp(lo as f64, hi as f64) as u32)
        .unwrap_or(default)
}

fn parse_info(v: &Value) -> Info {
    let eq = v.get("eq");
    let field = |k: &str, d: &str| clean_id(eq.and_then(|e| e.get(k)), d);
    Info {
        name: clean_name(v.get("name").and_then(Value::as_str).unwrap_or("")),
        rating: num(v, "rating", 0, 4000, 800),
        level: num(v, "level", 1, 999, 1),
        hue: num(v, "hue", 0, 359, 0),
        pieces: field("pieces", "classic"),
        acc: field("acc", ""),
        fx: field("fx", ""),
    }
}

fn info_json(i: &Info) -> Value {
    json!({"name": i.name, "rating": i.rating, "level": i.level, "hue": i.hue,
           "eq": {"pieces": i.pieces, "acc": i.acc, "fx": i.fx}})
}

struct User {
    info: Info,
    tx: Tx,
    /// The game this user is paired in.
    game: Option<u64>,
    watching: Option<u64>,
    /// The challenge this user has sent.
    out: Option<u64>,
}

struct Challenge {
    from: u64,
    to: u64,
    tc: (u32, u32),
    color: char,
}

struct Game {
    host: u64,
    guest: u64,
    white: u64,
    tc: (u32, u32),
    moves: Vec<String>,
    /// White's and black's clocks (ms) as of the last move.
    clk: [i64; 2],
    last: Instant,
    started: bool,
    listed: bool,
    over: Option<(String, String)>,
    specs: HashSet<u64>,
}

#[derive(Default)]
struct Inner {
    users: HashMap<u64, User>,
    games: HashMap<u64, Game>,
    chs: HashMap<u64, Challenge>,
    next_user: u64,
    next_game: u64,
    next_ch: u64,
}

#[derive(Default)]
pub struct Lobby {
    inner: Mutex<Inner>,
}

fn send(tx: &Tx, v: Value) {
    let _ = tx.send(Message::text(v.to_string()));
}

fn tc_ok(m: u64, i: u64) -> bool {
    (1..=60).contains(&m) && i <= 60
}

impl Inner {
    fn tell(&self, id: u64, v: Value) {
        if let Some(u) = self.users.get(&id) {
            send(&u.tx, v);
        }
    }

    fn all(&self, v: &Value, except: Option<u64>) {
        for (id, u) in &self.users {
            if Some(*id) != except {
                send(&u.tx, v.clone());
            }
        }
    }

    /// Does the user have a name no one else has? Names are case-insensitive and unique.
    fn unique_name(&self, wanted: &str) -> String {
        let taken = |n: &str| self.users.values().any(|u| u.info.name.eq_ignore_ascii_case(n));
        if !taken(wanted) {
            return wanted.to_owned();
        }
        for k in 2.. {
            let suffix = format!(" {k}");
            let base: String = wanted.chars().take(16 - suffix.len().min(15)).collect();
            let cand = format!("{base}{suffix}");
            if !taken(&cand) {
                return cand;
            }
        }
        unreachable!()
    }

    fn user_json(&self, id: u64) -> Value {
        let u = &self.users[&id];
        let mut v = info_json(&u.info);
        v["id"] = json!(id);
        v["st"] = json!(if u.game.is_some() { "play" } else { "idle" });
        v
    }

    fn side(&self, g: &Game, black: bool) -> u64 {
        if black == (g.white == g.host) {
            g.guest
        } else {
            g.host
        }
    }

    fn game_json(&self, gid: u64) -> Option<Value> {
        let g = self.games.get(&gid)?;
        let (w, b) = (self.users.get(&g.white)?, self.users.get(&self.side(g, true))?);
        Some(json!({"gid": gid, "tc": [g.tc.0, g.tc.1], "n": g.moves.len(), "sp": g.specs.len(),
            "w": {"id": g.white, "name": w.info.name, "rating": w.info.rating},
            "b": {"id": self.side(g, true), "name": b.info.name, "rating": b.info.rating}}))
    }

    fn glist(&self) -> Vec<Value> {
        let mut ids: Vec<u64> = self.games.iter().filter(|(_, g)| g.listed).map(|(i, _)| *i).collect();
        ids.sort_unstable();
        ids.into_iter().rev().take(60).filter_map(|i| self.game_json(i)).collect()
    }

    fn snap_json(&self, gid: u64) -> Option<Value> {
        let g = self.games.get(&gid)?;
        let (w, b) = (self.users.get(&g.white)?, self.users.get(&self.side(g, true))?);
        let mut wi = info_json(&w.info);
        wi["id"] = json!(g.white);
        let mut bi = info_json(&b.info);
        bi["id"] = json!(self.side(g, true));
        Some(json!({"t": "snap", "gid": gid, "w": wi, "b": bi, "tc": [g.tc.0, g.tc.1],
            "moves": g.moves, "clk": g.clk, "age": g.last.elapsed().as_millis() as u64,
            "run": g.moves.len() >= 2 && g.over.is_none(),
            "over": g.over.as_ref().map(|(r, w)| json!({"r": r, "why": w})),
            "sp": g.specs.len()}))
    }

    fn status_changed(&self, id: u64) {
        if self.users.contains_key(&id) {
            self.all(&json!({"t": "u~", "u": self.user_json(id)}), None);
        }
    }

    fn game_ends_listing(&mut self, gid: u64) {
        let was = self.games.get_mut(&gid).map(|g| std::mem::replace(&mut g.listed, false)).unwrap_or(false);
        if was {
            self.all(&json!({"t": "g-", "gid": gid}), None);
        }
    }

    fn spectators_changed(&self, gid: u64) {
        if let Some(g) = self.games.get(&gid) {
            let v = json!({"t": "sp", "n": g.specs.len()});
            self.tell(g.host, v.clone());
            self.tell(g.guest, v);
        }
    }

    fn stop_watching(&mut self, uid: u64) {
        let Some(gid) = self.users.get_mut(&uid).and_then(|u| u.watching.take()) else {
            return;
        };
        if let Some(g) = self.games.get_mut(&gid) {
            g.specs.remove(&uid);
        }
        self.spectators_changed(gid);
    }

    fn end_challenge(&mut self, chid: u64) -> Option<Challenge> {
        let c = self.chs.remove(&chid)?;
        if let Some(u) = self.users.get_mut(&c.from) {
            if u.out == Some(chid) {
                u.out = None;
            }
        }
        Some(c)
    }

    /// Challenges sent by or to this user end; the other side is told.
    fn drop_challenges_of(&mut self, uid: u64, why: &str) {
        let ids: Vec<u64> = self.chs.iter().filter(|(_, c)| c.from == uid || c.to == uid).map(|(i, _)| *i).collect();
        for id in ids {
            if let Some(c) = self.end_challenge(id) {
                if c.from == uid {
                    self.tell(c.to, json!({"t": "chc", "id": id}));
                } else {
                    self.tell(c.from, json!({"t": "chr", "id": id, "why": why}));
                }
            }
        }
    }

    /// The pairing of a game ends (someone left or went back to the lobby).
    fn end_game(&mut self, gid: u64, left: Option<u64>) {
        let Some(g) = self.games.remove(&gid) else {
            return;
        };
        if g.listed {
            self.all(&json!({"t": "g-", "gid": gid}), None);
        }
        for id in g.specs.iter() {
            if let Some(u) = self.users.get_mut(id) {
                u.watching = None;
            }
            if g.over.is_none() {
                self.tell(*id, json!({"t": "sx", "gid": gid, "r": "*", "why": "a player left"}));
            }
            self.tell(*id, json!({"t": "sstop", "gid": gid}));
        }
        for id in [g.host, g.guest] {
            if let Some(u) = self.users.get_mut(&id) {
                u.game = None;
            }
            if Some(id) != left {
                self.tell(id, json!({"t": "oppx"}));
            }
            self.status_changed(id);
        }
    }

    fn leave(&mut self, uid: u64) {
        if !self.users.contains_key(&uid) {
            return;
        }
        self.stop_watching(uid);
        self.drop_challenges_of(uid, "that player left");
        if let Some(gid) = self.users[&uid].game {
            self.end_game(gid, Some(uid));
        }
        self.users.remove(&uid);
        self.all(&json!({"t": "u-", "id": uid}), None);
    }

    fn challenge(&mut self, uid: u64, v: &Value) -> Result<(), &'static str> {
        let to = v.get("to").and_then(Value::as_u64).ok_or("who?")?;
        if to == uid {
            return Err("you can't challenge yourself");
        }
        let (m, i) = match v.get("tc").and_then(Value::as_array).map(|a| (a.first().and_then(Value::as_u64), a.get(1).and_then(Value::as_u64))) {
            Some((Some(m), Some(i))) if tc_ok(m, i) => (m as u32, i as u32),
            _ => return Err("bad time control"),
        };
        let color = match v.get("color").and_then(Value::as_str) {
            Some("w") => 'w',
            Some("b") => 'b',
            _ => 'r',
        };
        let me = self.users.get(&uid).ok_or("not registered")?;
        if me.game.is_some() {
            return Err("you are in a match already");
        }
        if me.out.is_some() {
            return Err("you already have a challenge out");
        }
        let target = self.users.get(&to).ok_or("that player is gone")?;
        if target.game.is_some() {
            return Err("that player is in a game");
        }
        if self.chs.values().filter(|c| c.to == to).count() >= MAX_INCOMING {
            return Err("that player has too many challenges");
        }
        self.next_ch += 1;
        let id = self.next_ch;
        self.chs.insert(id, Challenge { from: uid, to, tc: (m, i), color });
        self.users.get_mut(&uid).unwrap().out = Some(id);
        let from_json = self.user_json(uid);
        self.tell(to, json!({"t": "chi", "id": id, "from": from_json, "tc": [m, i], "color": color.to_string()}));
        self.tell(uid, json!({"t": "cho", "id": id, "to": to}));
        Ok(())
    }

    fn accept(&mut self, uid: u64, chid: u64) -> Result<(), &'static str> {
        let c = self.chs.get(&chid).filter(|c| c.to == uid).ok_or("that challenge is gone")?;
        let (from, tc, color) = (c.from, c.tc, c.color);
        if self.users.get(&uid).ok_or("not registered")?.game.is_some() {
            return Err("you are in a match already");
        }
        match self.users.get(&from) {
            Some(f) if f.game.is_none() => {}
            _ => return Err("that player is no longer free"),
        }
        self.end_challenge(chid);
        self.drop_challenges_of(from, "that player started another game");
        self.drop_challenges_of(uid, "that player started another game");
        self.stop_watching(uid);
        self.stop_watching(from);
        self.next_game += 1;
        let gid = self.next_game;
        self.games.insert(gid, Game {
            host: from, guest: uid, white: from, tc, moves: vec![], clk: [tc.0 as i64 * 60000; 2],
            last: Instant::now(), started: false, listed: false, over: None, specs: HashSet::new(),
        });
        for id in [from, uid] {
            self.users.get_mut(&id).unwrap().game = Some(gid);
        }
        let (fj, uj) = (info_json(&self.users[&from].info), info_json(&self.users[&uid].info));
        self.tell(from, json!({"t": "match", "gid": gid, "role": "host", "opp": uj, "tc": [tc.0, tc.1], "color": color.to_string()}));
        self.tell(uid, json!({"t": "match", "gid": gid, "role": "guest", "opp": fj, "tc": [tc.0, tc.1], "color": color.to_string()}));
        self.status_changed(from);
        self.status_changed(uid);
        Ok(())
    }

    fn finish(&mut self, gid: u64, r: &str, why: &str) {
        let Some(g) = self.games.get_mut(&gid) else {
            return;
        };
        if !g.started || g.over.is_some() {
            return;
        }
        let r = if matches!(r, "1-0" | "0-1" | "1/2-1/2") { r } else { "*" };
        let why: String = why.chars().filter(|c| !c.is_control()).take(40).collect();
        g.over = Some((r.to_owned(), why.clone()));
        let specs: Vec<u64> = g.specs.iter().copied().collect();
        for s in specs {
            self.tell(s, json!({"t": "sx", "gid": gid, "r": r, "why": why}));
        }
        self.game_ends_listing(gid);
    }

    /// Something a player sent to the opponent: pass it on, and note what spectators need.
    fn game_msg(&mut self, uid: u64, d: &str) {
        let Some(gid) = self.users.get(&uid).and_then(|u| u.game) else {
            return;
        };
        let (opp, is_host) = {
            let g = &self.games[&gid];
            (if g.host == uid { g.guest } else { g.host }, g.host == uid)
        };
        self.tell(opp, json!({"t": "g", "d": d}));
        let Ok(m) = serde_json::from_str::<Value>(d) else {
            return;
        };
        match m.get("t").and_then(Value::as_str) {
            Some("start") if is_host => {
                let g = self.games.get_mut(&gid).unwrap();
                if let Some(a) = m.get("tc").and_then(Value::as_array) {
                    if let (Some(x), Some(y)) = (a.first().and_then(Value::as_u64), a.get(1).and_then(Value::as_u64)) {
                        if tc_ok(x, y) {
                            g.tc = (x as u32, y as u32);
                        }
                    }
                }
                g.white = if m.get("hostColor").and_then(Value::as_str) == Some("b") { g.guest } else { g.host };
                g.moves.clear();
                g.clk = [g.tc.0 as i64 * 60000; 2];
                g.last = Instant::now();
                g.over = None;
                g.started = true;
                let newly = !std::mem::replace(&mut g.listed, true);
                if newly {
                    if let Some(v) = self.game_json(gid) {
                        self.all(&json!({"t": "g+", "g": v}), None);
                    }
                }
                if let Some(s) = self.snap_json(gid) {
                    let specs: Vec<u64> = self.games[&gid].specs.iter().copied().collect();
                    for id in specs {
                        self.tell(id, s.clone());
                    }
                }
            }
            Some("move") => {
                let g = self.games.get_mut(&gid).unwrap();
                let Some(u) = m.get("u").and_then(Value::as_str) else {
                    return;
                };
                if !g.started || g.over.is_some() || g.moves.len() >= MAX_PLIES || !(4..=5).contains(&u.len()) || !u.chars().all(|c| c.is_ascii_alphanumeric()) {
                    return;
                }
                let white_to_move = g.moves.len() % 2 == 0;
                let mover_is_white = g.white == uid;
                if white_to_move != mover_is_white {
                    return;
                }
                let c = m.get("c").and_then(Value::as_f64).map(|f| f.clamp(0.0, 7_200_000.0) as i64).unwrap_or(g.clk[if white_to_move { 0 } else { 1 }]);
                g.clk[if white_to_move { 0 } else { 1 }] = c;
                g.moves.push(u.to_owned());
                g.last = Instant::now();
                let (n, specs) = (g.moves.len(), g.specs.iter().copied().collect::<Vec<u64>>());
                let ev = json!({"t": "sm", "gid": gid, "u": u, "c": c, "n": n});
                for id in specs {
                    self.tell(id, ev.clone());
                }
            }
            Some("abort") => self.finish_aborted(gid),
            _ => {}
        }
    }

    fn finish_aborted(&mut self, gid: u64) {
        self.finish(gid, "*", "aborted");
    }

    fn spectate(&mut self, uid: u64, gid: u64) -> Result<(), &'static str> {
        if self.users.get(&uid).ok_or("not registered")?.game.is_some() {
            return Err("finish your own game first");
        }
        let g = self.games.get(&gid).filter(|g| g.started).ok_or("that game is over")?;
        if g.specs.len() >= MAX_SPECTATORS {
            return Err("too many people are watching that game");
        }
        self.stop_watching(uid);
        self.games.get_mut(&gid).unwrap().specs.insert(uid);
        self.users.get_mut(&uid).unwrap().watching = Some(gid);
        if let Some(s) = self.snap_json(gid) {
            self.tell(uid, s);
        }
        self.spectators_changed(gid);
        Ok(())
    }
}

impl Lobby {
    fn join(&self, info: Info, tx: Tx) -> Result<(u64, Value), &'static str> {
        let mut s = self.inner.lock().unwrap();
        if s.users.len() >= MAX_USERS {
            return Err("the lobby is full, try again later");
        }
        let mut info = info;
        info.name = s.unique_name(&info.name);
        s.next_user += 1;
        let id = s.next_user;
        s.users.insert(id, User { info, tx, game: None, watching: None, out: None });
        let me = s.user_json(id);
        let users: Vec<Value> = {
            let mut ids: Vec<u64> = s.users.keys().copied().filter(|i| *i != id).collect();
            ids.sort_unstable();
            ids.into_iter().map(|i| s.user_json(i)).collect()
        };
        s.all(&json!({"t": "u+", "u": me.clone()}), Some(id));
        let welcome = json!({"t": "welcome", "me": me, "users": users, "games": s.glist()});
        Ok((id, welcome))
    }

    fn leave(&self, uid: u64) {
        self.inner.lock().unwrap().leave(uid);
    }

    fn expire(&self, chid: u64) {
        let mut s = self.inner.lock().unwrap();
        if let Some(c) = s.end_challenge(chid) {
            s.tell(c.from, json!({"t": "chr", "id": chid, "why": "no answer"}));
            s.tell(c.to, json!({"t": "chc", "id": chid}));
        }
    }

    /// One message from a joined client. Returns a challenge id when a timer for it should start.
    fn message(&self, uid: u64, text: &str) -> Option<u64> {
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            return None;
        };
        let mut s = self.inner.lock().unwrap();
        let err = |s: &Inner, why: &str| s.tell(uid, json!({"t": "err", "why": why}));
        match v.get("t").and_then(Value::as_str).unwrap_or("") {
            "g" => {
                if let Some(d) = v.get("d").and_then(Value::as_str) {
                    s.game_msg(uid, d);
                }
            }
            "over" => {
                if let Some(gid) = s.users.get(&uid).and_then(|u| u.game) {
                    let r = v.get("r").and_then(Value::as_str).unwrap_or("*").to_owned();
                    let why = v.get("why").and_then(Value::as_str).unwrap_or("").to_owned();
                    s.finish(gid, &r, &why);
                }
            }
            "ch" => match s.challenge(uid, &v) {
                Ok(()) => return s.users.get(&uid).and_then(|u| u.out),
                Err(why) => s.tell(uid, json!({"t": "chr", "id": 0, "why": why})),
            },
            "chx" => {
                if let Some(id) = s.users.get(&uid).and_then(|u| u.out) {
                    if let Some(c) = s.end_challenge(id) {
                        s.tell(c.to, json!({"t": "chc", "id": id}));
                    }
                }
            }
            "dec" => {
                if let Some(id) = v.get("id").and_then(Value::as_u64) {
                    if s.chs.get(&id).map(|c| c.to) == Some(uid) {
                        if let Some(c) = s.end_challenge(id) {
                            s.tell(c.from, json!({"t": "chr", "id": id, "why": "declined"}));
                        }
                    }
                }
            }
            "acc" => {
                let id = v.get("id").and_then(Value::as_u64).unwrap_or(0);
                if let Err(why) = s.accept(uid, id) {
                    s.tell(uid, json!({"t": "chc", "id": id, "why": why}));
                }
            }
            "leave" => {
                if let Some(gid) = s.users.get(&uid).and_then(|u| u.game) {
                    s.finish(gid, "*", "a player left");
                    s.end_game(gid, Some(uid));
                }
            }
            "spec" => {
                let gid = v.get("gid").and_then(Value::as_u64).unwrap_or(0);
                if let Err(why) = s.spectate(uid, gid) {
                    s.tell(uid, json!({"t": "sstop", "gid": gid, "why": why}));
                }
            }
            "unspec" => s.stop_watching(uid),
            "list" => s.tell(uid, json!({"t": "glist", "games": s.glist()})),
            "upd" => {
                if let Some(u) = s.users.get_mut(&uid) {
                    let keep = u.info.name.clone();
                    u.info = parse_info(&v);
                    u.info.name = keep;
                }
                s.status_changed(uid);
            }
            "p" => s.tell(uid, json!({"t": "p"})),
            _ => err(&s, "unknown message"),
        }
        None
    }
}

/// Where copies from before versions existed are sent: a build that has the in-game updater, so every
/// later update reaches them through the update screen.
const LEGACY_UPDATE_URL: &str = "https://drive.google.com/uc?export=download&id=1ne9HHeoi4waAP9Vg7mQofh7q5T5pOVyq";

/// Why a client of this version can't come in, if it can't: `update` for an older version, `err` for one
/// without a version.
fn version_check(hello: &Value, page_url: &str) -> Option<Value> {
    let want = page_version();
    if want == 0 {
        return None;
    }
    let have = hello.get("ver").and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok())));
    match have {
        None => Some(json!({"t": "err", "why": format!("This copy of Blitz Chess is out of date. Download the new version here: {LEGACY_UPDATE_URL} then open it (you can replace your old file). Later updates install from inside the game.")})),
        Some(v) if v < want => Some(json!({"t": "update", "ver": want, "url": page_url})),
        _ => None,
    }
}

/// One chess client, from its handshake until it goes. `page_url` is where this relay's page is.
pub async fn handle(lobby: Arc<Lobby>, ws: Ws, page_url: String) {
    let (mut sink, mut source) = ws.split();
    // The first message says who this is.
    let first = tokio::time::timeout(HELLO_WAIT, async {
        loop {
            match source.next().await {
                Some(Ok(Message::Text(t))) if t.len() <= MAX_MSG => return Some(t.to_string()),
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                _ => return None,
            }
        }
    })
    .await;
    let Ok(Some(first)) = first else {
        return;
    };
    let hello: Value = serde_json::from_str(&first).unwrap_or(Value::Null);
    if hello.get("t").and_then(Value::as_str) != Some("hi") {
        let _ = sink.send(Message::text("{\"t\":\"err\",\"why\":\"say hi first\"}")).await;
        return;
    }
    if let Some(no) = version_check(&hello, &page_url) {
        let _ = sink.send(Message::text(no.to_string())).await;
        let _ = sink.close().await;
        return;
    }
    let (tx, mut outbox) = mpsc::unbounded_channel::<Message>();
    let (uid, welcome) = match lobby.join(parse_info(&hello), tx.clone()) {
        Ok(x) => x,
        Err(why) => {
            let _ = sink.send(Message::text(json!({"t": "err", "why": why}).to_string())).await;
            return;
        }
    };
    send(&tx, welcome);
    let mut ping = tokio::time::interval(PING_EVERY);
    let mut last_rx = Instant::now();
    let (mut window, mut count) = (Instant::now(), 0u32);
    loop {
        tokio::select! {
            m = outbox.recv() => match m {
                Some(m) => if sink.send(m).await.is_err() { break },
                None => break,
            },
            incoming = source.next() => {
                last_rx = Instant::now();
                match incoming {
                    Some(Ok(Message::Text(t))) => {
                        if window.elapsed() >= Duration::from_secs(1) { window = Instant::now(); count = 0; }
                        count += 1;
                        if t.len() > MAX_MSG || count > MAX_PER_SECOND { break; }
                        if let Some(chid) = lobby.message(uid, t.as_str()) {
                            let lobby = lobby.clone();
                            tokio::spawn(async move { tokio::time::sleep(CHALLENGE_TTL).await; lobby.expire(chid); });
                        }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            },
            _ = ping.tick() => {
                if last_rx.elapsed() > IDLE_LIMIT { break; }
                if sink.send(Message::Ping(Vec::new().into())).await.is_err() { break; }
            }
        }
    }
    lobby.leave(uid);
    let _ = sink.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run;
    use tokio::net::TcpListener;
    use tokio_tungstenite::connect_async;

    type Client = WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

    async fn start() -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let at = listener.local_addr().unwrap();
        tokio::spawn(run(listener));
        at
    }

    async fn client(at: std::net::SocketAddr, name: &str) -> (Client, Value) {
        let (mut ws, _) = connect_async(format!("ws://{at}/chess")).await.unwrap();
        ws.send(Message::text(json!({"t": "hi", "name": name, "rating": 900, "level": 3, "hue": 120, "ver": page_version(),
            "eq": {"pieces": "classic", "acc": "", "fx": ""}}).to_string())).await.unwrap();
        let w = next_of(&mut ws, "welcome").await;
        (ws, w)
    }

    /// The next message of this type, skipping others (with a time limit).
    async fn next_of(ws: &mut Client, t: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Message::Text(x) = ws.next().await.unwrap().unwrap() {
                    let v: Value = serde_json::from_str(x.as_str()).unwrap();
                    if v["t"] == t {
                        return v;
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no {t} message"))
    }

    async fn say(ws: &mut Client, v: Value) {
        ws.send(Message::text(v.to_string())).await.unwrap();
    }

    #[test]
    fn names_are_cleaned() {
        assert_eq!(clean_name("  <b>Zed\u{7}</b>  "), "bZed/b");
        assert_eq!(clean_name("<>"), "Player");
        assert_eq!(clean_name("ABCDEFGHIJKLMNOPQRSTUV").chars().count(), 16);
        assert_eq!(clean_id(Some(&json!("a b")), "x"), "x");
        assert_eq!(clean_id(Some(&json!("royal-1")), "x"), "royal-1");
    }

    #[tokio::test]
    async fn lobby_challenge_match_and_spectate() {
        let at = start().await;
        let (mut a, wa) = client(at, "Ann").await;
        assert_eq!(wa["users"].as_array().unwrap().len(), 0);
        let a_id = wa["me"]["id"].as_u64().unwrap();
        let (mut b, wb) = client(at, "ann").await; // same name: made unique
        assert_eq!(wb["me"]["name"], "ann 2");
        assert_eq!(wb["users"].as_array().unwrap().len(), 1);
        let b_id = wb["me"]["id"].as_u64().unwrap();
        assert_eq!(next_of(&mut a, "u+").await["u"]["name"], "ann 2");
        let (mut c, _) = client(at, "Cy").await;
        let _ = next_of(&mut a, "u+").await;
        let _ = next_of(&mut b, "u+").await;

        // A challenges B, B accepts.
        say(&mut a, json!({"t": "ch", "to": b_id, "tc": [3, 2], "color": "b"})).await;
        let ch = next_of(&mut b, "chi").await;
        assert_eq!(ch["from"]["name"], "Ann");
        assert_eq!(ch["tc"], json!([3, 2]));
        assert_eq!(ch["color"], "b");
        let chid = ch["id"].as_u64().unwrap();
        // A cannot have a second challenge out.
        say(&mut a, json!({"t": "ch", "to": b_id, "tc": [3, 0]})).await;
        assert!(next_of(&mut a, "chr").await["why"].as_str().unwrap().contains("already"));
        say(&mut b, json!({"t": "acc", "id": chid})).await;
        let (ma, mb) = (next_of(&mut a, "match").await, next_of(&mut b, "match").await);
        assert_eq!((ma["role"].as_str(), mb["role"].as_str()), (Some("host"), Some("guest")));
        assert_eq!(ma["opp"]["name"], "ann 2");
        let gid = ma["gid"].as_u64().unwrap();
        assert_eq!(next_of(&mut c, "u~").await["u"]["st"], "play");

        // Busy players can't be challenged.
        say(&mut c, json!({"t": "ch", "to": a_id, "tc": [3, 0]})).await;
        assert!(next_of(&mut c, "chr").await["why"].as_str().unwrap().contains("in a game"));

        // Game messages go to the opponent only; nothing is listed before `start`.
        let hello = json!({"t": "hello", "v": 1}).to_string();
        say(&mut a, json!({"t": "g", "d": hello})).await;
        assert_eq!(next_of(&mut b, "g").await["d"], hello);
        say(&mut c, json!({"t": "list"})).await;
        assert_eq!(next_of(&mut c, "glist").await["games"].as_array().unwrap().len(), 0);
        let start = json!({"t": "start", "tc": [3, 2], "hostColor": "b"}).to_string();
        say(&mut a, json!({"t": "g", "d": start})).await;
        let _ = next_of(&mut b, "g").await;
        let added = next_of(&mut c, "g+").await;
        // Ann chose black: "ann 2" has white.
        assert_eq!(added["g"]["w"]["name"], "ann 2");
        assert_eq!(added["g"]["b"]["name"], "Ann");

        // One move each, then Cy joins late and catches up.
        say(&mut b, json!({"t": "g", "d": json!({"t": "move", "u": "e2e4", "c": 181000}).to_string()})).await;
        let _ = next_of(&mut a, "g").await;
        // Out of turn: not recorded.
        say(&mut b, json!({"t": "g", "d": json!({"t": "move", "u": "d2d4", "c": 1}).to_string()})).await;
        let _ = next_of(&mut a, "g").await;
        say(&mut a, json!({"t": "g", "d": json!({"t": "move", "u": "e7e5", "c": 179000}).to_string()})).await;
        let _ = next_of(&mut b, "g").await;
        say(&mut c, json!({"t": "spec", "gid": gid})).await;
        let snap = next_of(&mut c, "snap").await;
        assert_eq!(snap["moves"], json!(["e2e4", "e7e5"]));
        assert_eq!(snap["clk"], json!([181000, 179000]));
        assert_eq!(snap["run"], true);
        assert_eq!(snap["w"]["name"], "ann 2");
        assert_eq!(next_of(&mut a, "sp").await["n"], 1);
        // A live move reaches the spectator.
        say(&mut b, json!({"t": "g", "d": json!({"t": "move", "u": "g1f3", "c": 180000}).to_string()})).await;
        let sm = next_of(&mut c, "sm").await;
        assert_eq!((sm["u"].as_str(), sm["n"].as_u64()), (Some("g1f3"), Some(3)));
        // Spectators can't talk into the game.
        say(&mut c, json!({"t": "g", "d": hello})).await;
        // The game ends: spectators and the list hear of it.
        say(&mut a, json!({"t": "over", "r": "0-1", "why": "resignation"})).await;
        let sx = next_of(&mut c, "sx").await;
        assert_eq!((sx["r"].as_str(), sx["why"].as_str()), (Some("0-1"), Some("resignation")));
        assert_eq!(next_of(&mut c, "g-").await["gid"], gid);
        // A rematch lists it again and restarts the spectators' view.
        say(&mut a, json!({"t": "g", "d": json!({"t": "start", "tc": [3, 2], "hostColor": "w"}).to_string()})).await;
        let again = next_of(&mut c, "snap").await;
        assert_eq!(again["moves"], json!([]));
        assert_eq!(again["w"]["name"], "Ann");
        assert_eq!(again["over"], Value::Null);

        // B leaves: A is told, the spectator's game stops, both are idle again.
        drop(b);
        let _ = next_of(&mut a, "oppx").await;
        assert_eq!(next_of(&mut c, "sstop").await["gid"], gid);
        say(&mut c, json!({"t": "ch", "to": a_id, "tc": [1, 0]})).await;
        let _ = next_of(&mut a, "chi").await;
    }

    #[tokio::test]
    async fn challenges_can_be_declined_withdrawn_and_expire() {
        let at = start().await;
        let (mut a, wa) = client(at, "A").await;
        let (mut b, wb) = client(at, "B").await;
        let (a_id, b_id) = (wa["me"]["id"].as_u64().unwrap(), wb["me"]["id"].as_u64().unwrap());
        say(&mut a, json!({"t": "ch", "to": b_id, "tc": [1, 0]})).await;
        let id = next_of(&mut b, "chi").await["id"].as_u64().unwrap();
        say(&mut b, json!({"t": "dec", "id": id})).await;
        assert_eq!(next_of(&mut a, "chr").await["why"], "declined");
        // Withdrawn
        say(&mut a, json!({"t": "ch", "to": b_id, "tc": [1, 0]})).await;
        let _ = next_of(&mut b, "chi").await;
        say(&mut a, json!({"t": "chx"})).await;
        let _ = next_of(&mut b, "chc").await;
        // Bad inputs
        say(&mut a, json!({"t": "ch", "to": a_id, "tc": [1, 0]})).await;
        assert!(next_of(&mut a, "chr").await["why"].as_str().unwrap().contains("yourself"));
        say(&mut a, json!({"t": "ch", "to": b_id, "tc": [999, 0]})).await;
        assert!(next_of(&mut a, "chr").await["why"].as_str().unwrap().contains("time control"));
        say(&mut a, json!({"t": "ch", "to": 9999, "tc": [3, 0]})).await;
        assert!(next_of(&mut a, "chr").await["why"].as_str().unwrap().contains("gone"));
        // Challenger leaves: the target hears the challenge was withdrawn.
        say(&mut a, json!({"t": "ch", "to": b_id, "tc": [1, 0]})).await;
        let _ = next_of(&mut b, "chi").await;
        drop(a);
        let _ = next_of(&mut b, "chc").await;
        assert_eq!(next_of(&mut b, "u-").await["id"], a_id);
    }

    #[tokio::test]
    async fn old_versions_are_sent_to_the_update_and_the_page_is_served() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let at = start().await;
        let want = page_version();
        assert!(want > 0, "the embedded page has a version");
        let hi = |ver: Value| json!({"t": "hi", "name": "Old", "ver": ver}).to_string();
        // No version at all (copies from before versions): told where the new page is, as text.
        let (mut ws, _) = connect_async(format!("ws://{at}/chess")).await.unwrap();
        ws.send(Message::text(json!({"t": "hi", "name": "Old"}).to_string())).await.unwrap();
        let m = next_of(&mut ws, "err").await;
        assert!(m["why"].as_str().unwrap().contains(LEGACY_UPDATE_URL), "{m}");
        // An older version: the update message.
        let (mut ws, _) = connect_async(format!("ws://{at}/chess")).await.unwrap();
        ws.send(Message::text(hi(json!(want - 1)))).await.unwrap();
        let m = next_of(&mut ws, "update").await;
        assert_eq!(m["ver"], want);
        // The same version (or a newer one, as a string) comes in.
        for v in [json!(want), json!((want + 1).to_string())] {
            let (mut ws, _) = connect_async(format!("ws://{at}/chess")).await.unwrap();
            ws.send(Message::text(hi(v))).await.unwrap();
            let _ = next_of(&mut ws, "welcome").await;
        }
        // GET /chess is the page; HEAD has no body; GET / is still the health check.
        let get = |req: &'static str| async move {
            let mut s = TcpStream::connect(at).await.unwrap();
            s.write_all(req.as_bytes()).await.unwrap();
            let mut out = Vec::new();
            s.read_to_end(&mut out).await.unwrap();
            out
        };
        let page = get("GET /chess HTTP/1.1\r\nHost: x\r\n\r\n").await;
        let text = String::from_utf8_lossy(&page);
        assert!(text.starts_with("HTTP/1.1 200") && text.contains("text/html") && text.contains("Access-Control-Allow-Origin: *"));
        assert!(page.ends_with(PAGE));
        let head = get("HEAD /chess HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(String::from_utf8_lossy(&head).ends_with("\r\n\r\n"));
        let ok = get("GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(String::from_utf8_lossy(&ok).ends_with("horizon-web-relay ok\n"));
        assert_eq!(page_url("GET /chess HTTP/1.1\r\nHost: a.koyeb.app\r\n\r\n"), "https://a.koyeb.app/chess");
        assert_eq!(page_url("GET /chess HTTP/1.1\r\nhost: 192.168.1.5:8000\r\n\r\n"), "http://192.168.1.5:8000/chess");
        assert_eq!(page_url("GET /chess HTTP/1.1\r\nHost: a.b\r\nX-Forwarded-Proto: http\r\n\r\n"), "http://a.b/chess");
    }

    #[tokio::test]
    async fn must_say_hi_and_plain_http_is_answered() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let at = start().await;
        let (mut ws, _) = connect_async(format!("ws://{at}/chess")).await.unwrap();
        ws.send(Message::text("{\"t\":\"list\"}")).await.unwrap();
        let m = ws.next().await.unwrap().unwrap();
        assert!(m.to_text().unwrap().contains("say hi"));
        let mut s = TcpStream::connect(at).await.unwrap();
        s.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        assert!(out.starts_with("HTTP/1.1 200") && out.contains("ok"));
    }
}
