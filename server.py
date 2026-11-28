#!/usr/bin/env python3
"""
Zero-dependency Live Multiplayer + WebRTC Voice Signaling Server
for Game Platform (Snake Arena, Mega Obby, Money Tycoon, Disaster Survival).

Run:
  python3 server.py

Then open http://localhost:8080 in two browser tabs (or on two devices on the
same Wi-Fi using your Mac's local IP) and join the same Room Code!
"""

import gzip
import json
import os
import queue
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_FILE = os.path.join(BASE_DIR, "index.html")
PLAYERS_DB_FILE = os.path.join(BASE_DIR, "players_db.json")
PORT = int(os.environ.get("PORT", "8080"))

_index_cache = {"mtime": 0, "raw": b"", "gz": b""}
_index_lock = threading.Lock()
players_lock = threading.Lock()


def load_players_db():
    try:
        if os.path.exists(PLAYERS_DB_FILE):
            with open(PLAYERS_DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {}


players_db = load_players_db()


def save_players_db():
    try:
        with open(PLAYERS_DB_FILE, "w", encoding="utf-8") as f:
            json.dump(players_db, f)
    except Exception:
        pass


def get_index_payload(accept_gzip):
    st = os.stat(INDEX_FILE)
    with _index_lock:
        if st.st_mtime != _index_cache["mtime"] or not _index_cache["raw"]:
            with open(INDEX_FILE, "rb") as f:
                raw = f.read()
            _index_cache["raw"] = raw
            _index_cache["gz"] = gzip.compress(raw, compresslevel=6)
            _index_cache["mtime"] = st.st_mtime
        if accept_gzip:
            return _index_cache["gz"], True
        return _index_cache["raw"], False

# rooms[room_code][player_id] = { "id", "name", "avatar", "game", "x", "y", "speaking", "last_seen", "queue" }
rooms = {}
rooms_lock = threading.Lock()


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def broadcast(room_code, event_type, payload, exclude_id=None):
    msg = f"event: {event_type}\ndata: {json.dumps(payload)}\n\n".encode("utf-8")
    with rooms_lock:
        room = rooms.get(room_code, {})
        for pid, pinfo in list(room.items()):
            if exclude_id and pid == exclude_id:
                continue
            q = pinfo.get("queue")
            if q is not None:
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    pass


def send_to(room_code, target_id, event_type, payload):
    msg = f"event: {event_type}\ndata: {json.dumps(payload)}\n\n".encode("utf-8")
    with rooms_lock:
        pinfo = rooms.get(room_code, {}).get(target_id)
        if pinfo and pinfo.get("queue") is not None:
            try:
                pinfo["queue"].put_nowait(msg)
            except queue.Full:
                pass


def room_snapshot(room_code):
    with rooms_lock:
        room = rooms.get(room_code, {})
        return [
            {
                "id": p["id"],
                "name": p["name"],
                "avatar": p.get("avatar"),
                "game": p.get("game", "hub"),
                "x": p.get("x", 0),
                "y": p.get("y", 0),
                "speaking": p.get("speaking", False),
            }
            for p in room.values()
        ]


def cleanup_stale_players():
    while True:
        time.sleep(5)
        now = time.time()
        removed = []
        with rooms_lock:
            for rcode in list(rooms.keys()):
                room = rooms[rcode]
                for pid in list(room.keys()):
                    if now - room[pid].get("last_seen", now) > 25:
                        removed.append((rcode, pid))
                        del room[pid]
                if not room:
                    del rooms[rcode]
        for rcode, pid in removed:
            broadcast(rcode, "leave", {"id": pid})


class GameServerHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        # Quiet normal polling/state logs
        pass

    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_json(self, code, data):
        body = json.dumps(data).encode("utf-8")
        self.send_response(code)
        self._cors_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path in ("/", "/index.html", "/snake.html"):
            try:
                accept_enc = self.headers.get("Accept-Encoding", "")
                data, is_gz = get_index_payload("gzip" in accept_enc.lower())
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                if is_gz:
                    self.send_header("Content-Encoding", "gzip")
                    self.send_header("Vary", "Accept-Encoding")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(data)
            except Exception as e:
                self._send_json(500, {"error": str(e)})
            return

        if path == "/api/health":
            self._send_json(200, {"ok": True, "rooms": len(rooms), "players": len(players_db)})
            return

        if path == "/api/players":
            qs = parse_qs(parsed.query)
            q = (qs.get("q", [""])[0] or "").strip().lower()
            now = time.time()
            with players_lock:
                items = []
                for p in players_db.values():
                    last_s = p.get("last_seen", 0) or 0
                    is_online = bool(p.get("online", False)) and (now - last_s < 16)
                    items.append({
                        "name": p.get("name", ""),
                        "avatar": p.get("avatar"),
                        "last_seen": last_s,
                        "online": is_online,
                        "game": p.get("game", "hub") if is_online else "offline",
                    })
            if q:
                items = [p for p in items if q in p.get("name", "").lower()]
            items.sort(key=lambda x: (1 if x.get("online") else 0, x.get("last_seen", 0)), reverse=True)
            self._send_json(200, {"players": items[:50]})
            return

        if path == "/api/events":
            qs = parse_qs(parsed.query)
            room_code = (qs.get("room", ["ROOM1"])[0] or "ROOM1").strip().upper()[:16]
            pid = (qs.get("id", [""])[0] or "").strip()[:32]
            name = (qs.get("name", ["Player"])[0] or "Player").strip()[:24]
            friends_raw = (qs.get("friends", [""])[0] or "")
            my_friends = set(
                x.strip().lower() for x in friends_raw.split(",") if x.strip()
            )
            if not pid:
                self._send_json(400, {"error": "missing id"})
                return

            # Friends-only room check: you cannot join a room with a player you haven't added as a friend!
            blocked_by = None
            with rooms_lock:
                existing_room = rooms.get(room_code, {})
                for other_id, other_info in existing_room.items():
                    if other_id == pid:
                        continue
                    other_name = (other_info.get("name") or "").strip()
                    if other_name and other_name.lower() != name.lower() and other_name.lower() not in my_friends:
                        blocked_by = other_name
                        break

            if blocked_by:
                self.send_response(200)
                self._cors_headers()
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                msg = f"event: blocked\ndata: {json.dumps({'room': room_code, 'player': blocked_by, 'reason': f'You must add {blocked_by} first to join this room!'})}\n\n".encode("utf-8")
                try:
                    self.wfile.write(msg)
                    self.wfile.flush()
                except Exception:
                    pass
                return

            q = queue.Queue(maxsize=120)
            with rooms_lock:
                room = rooms.setdefault(room_code, {})
                existing = room.get(pid, {})
                room[pid] = {
                    "id": pid,
                    "name": name,
                    "avatar": existing.get("avatar"),
                    "game": existing.get("game", "hub"),
                    "x": existing.get("x", 0),
                    "y": existing.get("y", 0),
                    "speaking": existing.get("speaking", False),
                    "last_seen": time.time(),
                    "queue": q,
                }

            self.send_response(200)
            self._cors_headers()
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            # Send initial room snapshot to this player & notify others
            snap = room_snapshot(room_code)
            try:
                self.wfile.write(
                    f"event: snapshot\ndata: {json.dumps({'room': room_code, 'players': snap})}\n\n".encode("utf-8")
                )
                self.wfile.flush()
            except Exception:
                return

            broadcast(
                room_code,
                "join",
                {"id": pid, "name": name, "players": snap},
                exclude_id=pid,
            )

            try:
                while True:
                    try:
                        msg = q.get(timeout=12)
                        self.wfile.write(msg)
                        self.wfile.flush()
                    except queue.Empty:
                        # Keepalive comment
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                    with rooms_lock:
                        if pid in rooms.get(room_code, {}):
                            rooms[room_code][pid]["last_seen"] = time.time()
            except Exception:
                pass
            finally:
                with rooms_lock:
                    room = rooms.get(room_code, {})
                    if pid in room and room[pid].get("queue") is q:
                        del room[pid]
                    if not room and room_code in rooms:
                        del rooms[room_code]
                broadcast(room_code, "leave", {"id": pid})
            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        body = self._read_json()

        if path == "/api/register_player":
            name = (body.get("name") or "").strip()[:24]
            avatar = body.get("avatar")
            game = (body.get("game") or "hub").strip()[:24]
            if not name:
                self._send_json(400, {"error": "missing name"})
                return
            key = name.lower()
            with players_lock:
                prev = players_db.get(key, {})
                players_db[key] = {
                    "name": name,
                    "avatar": avatar if avatar is not None else prev.get("avatar"),
                    "game": game,
                    "online": True,
                    "last_seen": time.time(),
                }
                save_players_db()
            self._send_json(200, {"ok": True})
            return

        if path == "/api/presence":
            name = (body.get("name") or "").strip()[:24]
            game = (body.get("game") or "hub").strip()[:24]
            online = body.get("online", True)
            if not name:
                self._send_json(400, {"error": "missing name"})
                return
            key = name.lower()
            with players_lock:
                prev = players_db.get(key, {})
                players_db[key] = {
                    "name": prev.get("name") or name,
                    "avatar": body.get("avatar") if body.get("avatar") is not None else prev.get("avatar"),
                    "game": game if online else "offline",
                    "online": bool(online),
                    "last_seen": time.time() if online else 0,
                }
                save_players_db()
            self._send_json(200, {"ok": True})
            return

        if path == "/api/state":
            room_code = (body.get("room") or "ROOM1").strip().upper()[:16]
            pid = (body.get("id") or "").strip()[:32]
            if not pid:
                self._send_json(400, {"error": "missing id"})
                return
            with rooms_lock:
                pinfo = rooms.get(room_code, {}).get(pid)
                if pinfo:
                    pinfo["name"] = (body.get("name") or pinfo["name"])[:24]
                    if "avatar" in body:
                        pinfo["avatar"] = body["avatar"]
                    if "game" in body:
                        pinfo["game"] = body["game"]
                    if "x" in body:
                        pinfo["x"] = body["x"]
                    if "y" in body:
                        pinfo["y"] = body["y"]
                    if "speaking" in body:
                        pinfo["speaking"] = bool(body["speaking"])
                    pinfo["last_seen"] = time.time()
            broadcast(
                room_code,
                "state",
                {
                    "id": pid,
                    "name": body.get("name", "Player"),
                    "avatar": body.get("avatar"),
                    "game": body.get("game", "hub"),
                    "x": body.get("x", 0),
                    "y": body.get("y", 0),
                    "speaking": bool(body.get("speaking", False)),
                    "segs": body.get("segs"),
                    "skin": body.get("skin"),
                    "score": body.get("score", 0),
                    "alive": body.get("alive", True),
                },
                exclude_id=pid,
            )
            self._send_json(200, {"ok": True})
            return

        if path == "/api/signal":
            room_code = (body.get("room") or "ROOM1").strip().upper()[:16]
            from_id = (body.get("from") or "").strip()[:32]
            to_id = (body.get("to") or "").strip()[:32]
            sig_type = body.get("type")
            data = body.get("data")
            if not from_id or not to_id or not sig_type:
                self._send_json(400, {"error": "missing signal fields"})
                return
            send_to(
                room_code,
                to_id,
                "signal",
                {"from": from_id, "type": sig_type, "data": data},
            )
            self._send_json(200, {"ok": True})
            return

        if path == "/api/leave":
            room_code = (body.get("room") or "ROOM1").strip().upper()[:16]
            pid = (body.get("id") or "").strip()[:32]
            with rooms_lock:
                room = rooms.get(room_code, {})
                if pid in room:
                    del room[pid]
                if not room and room_code in rooms:
                    del rooms[room_code]
            broadcast(room_code, "leave", {"id": pid})
            self._send_json(200, {"ok": True})
            return

        self._send_json(404, {"error": "not found"})


def main():
    t = threading.Thread(target=cleanup_stale_players, daemon=True)
    t.start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), GameServerHandler)
    local_ip = get_local_ip()
    print("=" * 60)
    print("  LIVE MULTIPLAYER + VOICE SERVER RUNNING!")
    print(f"  This computer:  http://localhost:{PORT}")
    print(f"  Same Wi-Fi:     http://{local_ip}:{PORT}")
    print("=" * 60)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
