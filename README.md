# horizon-web-relay

The browser build's Play with Friends. A web page can't listen for connections or open TCP ones,
so the host's page and each friend's page connect here over WebSockets: the relay gives the host a
short code (`K7Q-2MX`), pairs each friend who types it with the host, and carries the game between
them unchanged. Up to 3 friends per world (4 players); friends can be on any network.

It keeps nothing: a code lasts as long as the host shares, and the relay only forwards bytes.

## The chess lobby (`/chess`, same port)

The same process also serves the Blitz Chess page's online lobby, so one Koyeb service and one port
carry both. Nothing about the routes above changed: `/host`, `/join/CODE` and `/accept/CODE/N` work
exactly as before, and the chess side keeps its own state (`src/chess.rs`).

- **Players online**, **challenges** (accept / decline / withdraw, they expire after 30 s), and **live games** that
  anyone can **spectate** (they catch up from the move list, then follow live with clocks).
- Once two players are paired, their pages' game messages are passed between them untouched, the same
  messages as the page's LAN mode. The relay only reads `start` / `move` / `abort` (and a `over`
  message each page sends at the end) to keep the move list for spectators. It has no chess rules and
  never judges a game; the players' pages do.
- Names are unique (a second "Ann" becomes "Ann 2"), limited to 400 people online, 100 spectators per
  game, 8 KB per message and 60 messages per second per connection.
- A plain `GET /` (no WebSocket) answers `200 ok`, handy as an HTTP health check. TCP health checks
  keep working.

In the page: **Online** tab → relay address (`wss://<app>.koyeb.app`, or `ws://<this computer>:8000` on your
own network) → **Go online**. You can also bake the address into the page (`DEFAULT_RELAY` near the top
of its script).

A game is lost if a player's connection drops (there is no reconnect): the other player is told and
can claim the win.

## Run it

```sh
cargo run --release          # listens on $PORT, 8000 when unset
cargo test                   # a host, three friends, a fourth turned away; the chess lobby
```

In the page, set **Multiplayer relay** (on the start panel) to its address: `ws://<this computer>:8000`
on your own network, `wss://<app>.koyeb.app` once deployed.

## Deploy on Koyeb

The relay is a plain web service behind Koyeb's TLS: the page connects with `wss://`.

**From GitHub** (the repository holding this folder):

1. Koyeb dashboard → **Create Service** → **Web Service** → **GitHub**, and pick the repository.
2. Builder: **Dockerfile**. Work directory: `web/relay` (the folder with the `Dockerfile`).
3. Exposed port: `8000`, protocol **HTTP** (WebSockets go through it). Health check: TCP on 8000.
4. Instance: **Free**. Region: the one nearest the players (less delay: every packet goes through it).
5. Deploy. The service's address is `https://<name>-<org>.koyeb.app`: in the page, `wss://<name>-<org>.koyeb.app`.

**From this folder, without GitHub** (Koyeb's CLI, `koyeb login` first): `koyeb deploy` uploads a
folder and builds its Dockerfile; see `koyeb deploy --help` for the current flags (the app and
service names, port 8000 over HTTP, the free instance).

A free instance may sleep when nobody uses it: the first connection after a while wakes it, which
can take a few seconds.
