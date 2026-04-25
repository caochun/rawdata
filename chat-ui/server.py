#!/usr/bin/env python3
"""
chat-ui server — lightweight proxy-based chat UI.

Proxies to any OpenAI-compatible agent (default: hermes at port 8642).
No RAG, no local models, no heavy deps — just stdlib + PyYAML.

Usage:
    python server.py [--port PORT] [--agent-url URL]
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import threading
import time
import urllib.request
import urllib.error
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# ── Config ─────────────────────────────────────────────────────────────────────
AGENT_URL     = os.environ.get("AGENT_URL", "http://127.0.0.1:8642/v1")
STATIC_DIR    = Path(__file__).parent / "static"
DB_PATH       = Path(__file__).parent / "chat.db"
JWT_SECRET    = os.environ.get("JWT_SECRET", secrets.token_hex(32))
JWT_EXPIRE_S  = 7 * 24 * 3600  # 7 days

# ── Database ───────────────────────────────────────────────────────────────────
_db_lock = threading.Lock()

def get_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id       TEXT PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL,
                role     TEXT NOT NULL DEFAULT 'user',
                created  INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                id      TEXT PRIMARY KEY,
                user_id TEXT NOT NULL DEFAULT '' REFERENCES users(id) ON DELETE CASCADE,
                title   TEXT NOT NULL DEFAULT 'New chat',
                model   TEXT NOT NULL DEFAULT '',
                created INTEGER NOT NULL,
                updated INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                role       TEXT NOT NULL,
                content    TEXT NOT NULL,
                created    INTEGER NOT NULL
            );
        """)
        # Migration: add user_id column if upgrading from pre-auth schema
        cols = {row[1] for row in conn.execute("PRAGMA table_info(sessions)")}
        if "user_id" not in cols:
            conn.execute("ALTER TABLE sessions ADD COLUMN user_id TEXT NOT NULL DEFAULT ''")

def _now():
    return int(time.time() * 1000)

def _gen_id():
    return str(uuid.uuid4())

# ── Auth helpers ───────────────────────────────────────────────────────────────

def _hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260000)
    return f"{salt}${dk.hex()}"

def _verify_password(password: str, stored: str) -> bool:
    try:
        salt, dk_hex = stored.split("$", 1)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260000)
        return hmac.compare_digest(dk.hex(), dk_hex)
    except Exception:
        return False

def _make_token(user_id: str) -> str:
    header  = base64.urlsafe_b64encode(b'{"alg":"HS256","typ":"JWT"}').rstrip(b"=").decode()
    exp     = int(time.time()) + JWT_EXPIRE_S
    payload = base64.urlsafe_b64encode(
        json.dumps({"sub": user_id, "exp": exp}).encode()
    ).rstrip(b"=").decode()
    sig = base64.urlsafe_b64encode(
        hmac.new(JWT_SECRET.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest()
    ).rstrip(b"=").decode()
    return f"{header}.{payload}.{sig}"

def _verify_token(token: str):
    """Return user_id string or None."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header, payload, sig = parts
        expected_sig = base64.urlsafe_b64encode(
            hmac.new(JWT_SECRET.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest()
        ).rstrip(b"=").decode()
        if not hmac.compare_digest(sig, expected_sig):
            return None
        pad = lambda s: s + "=" * (-len(s) % 4)
        data = json.loads(base64.urlsafe_b64decode(pad(payload)))
        if data.get("exp", 0) < int(time.time()):
            return None
        return data.get("sub")
    except Exception:
        return None

# ── User CRUD ──────────────────────────────────────────────────────────────────

def create_user(username: str, password: str, role: str = "user"):
    uid = _gen_id()
    with _db_lock, get_db() as conn:
        try:
            conn.execute(
                "INSERT INTO users (id, username, password, role, created) VALUES (?,?,?,?,?)",
                (uid, username, _hash_password(password), role, _now())
            )
        except sqlite3.IntegrityError:
            return None
    return {"id": uid, "username": username, "role": role}

def get_user_by_username(username: str):
    with _db_lock, get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        return dict(row) if row else None

def get_user_by_id(uid: str):
    with _db_lock, get_db() as conn:
        row = conn.execute("SELECT id, username, role, created FROM users WHERE id=?", (uid,)).fetchone()
        return dict(row) if row else None

def count_users() -> int:
    with _db_lock, get_db() as conn:
        return conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]

# ── Session CRUD ───────────────────────────────────────────────────────────────

def list_sessions(user_id: str):
    with _db_lock, get_db() as conn:
        rows = conn.execute(
            "SELECT id, title, model, created, updated FROM sessions WHERE user_id=? ORDER BY updated DESC",
            (user_id,)
        ).fetchall()
        return [dict(r) for r in rows]

def create_session(user_id: str, title="New chat", model=""):
    sid = _gen_id()
    now = _now()
    with _db_lock, get_db() as conn:
        conn.execute(
            "INSERT INTO sessions (id, user_id, title, model, created, updated) VALUES (?,?,?,?,?,?)",
            (sid, user_id, title, model, now, now)
        )
    return {"id": sid, "title": title, "model": model, "created": now, "updated": now}

def get_session(sid, user_id: str):
    with _db_lock, get_db() as conn:
        row = conn.execute("SELECT * FROM sessions WHERE id=? AND user_id=?", (sid, user_id)).fetchone()
        if not row:
            return None
        msgs = conn.execute(
            "SELECT role, content, created FROM messages WHERE session_id=? ORDER BY id",
            (sid,)
        ).fetchall()
        return {**dict(row), "messages": [dict(m) for m in msgs]}

def update_session(sid, user_id: str, **kwargs):
    allowed = {"title", "model"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return
    fields["updated"] = _now()
    sets = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [sid, user_id]
    with _db_lock, get_db() as conn:
        conn.execute(f"UPDATE sessions SET {sets} WHERE id=? AND user_id=?", vals)

def delete_session(sid, user_id: str):
    with _db_lock, get_db() as conn:
        conn.execute("DELETE FROM sessions WHERE id=? AND user_id=?", (sid, user_id))

def append_message(sid, role, content):
    with _db_lock, get_db() as conn:
        conn.execute(
            "INSERT INTO messages (session_id, role, content, created) VALUES (?,?,?,?)",
            (sid, role, content, _now())
        )
        # Auto-title from first user message
        if role == "user":
            title_row = conn.execute("SELECT title FROM sessions WHERE id=?", (sid,)).fetchone()
            if title_row and title_row["title"] == "New chat":
                title = content.strip()[:60]
                conn.execute("UPDATE sessions SET title=?, updated=? WHERE id=?",
                             (title, _now(), sid))
            else:
                conn.execute("UPDATE sessions SET updated=? WHERE id=?", (_now(), sid))

# ── Agent proxy ────────────────────────────────────────────────────────────────

def _agent_base() -> str:
    """Return the agent base URL without the /v1 suffix."""
    base = AGENT_URL.rstrip("/")
    return base[:-3] if base.endswith("/v1") else base

def proxy_models():
    """Fetch model list from agent."""
    try:
        req = urllib.request.Request(
            f"{AGENT_URL}/models",
            headers={"Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as e:
        # Return a minimal fallback
        return {"object": "list", "data": [{"id": "hermes", "object": "model"}]}

def _friendly_error(e) -> str:
    """Convert urllib/socket exceptions to a human-readable string."""
    s = str(e)
    if "Connection refused" in s or "Errno 111" in s:
        return f"无法连接到 agent（{AGENT_URL}）—— 请确认 agent 已启动"
    if "timed out" in s.lower():
        return "连接 agent 超时"
    if "urlopen error" in s:
        # strip the ugly wrapper: <urlopen error ...>
        inner = s.split("urlopen error")[-1].strip().strip("<>")
        return inner or s
    return s


def stream_chat(messages, model, on_chunk, on_error, on_done):
    """Stream chat completion from agent.

    Callbacks:
      on_chunk(text)  — called for each streamed delta
      on_error(msg)   — called once on failure (before on_done)
      on_done(full)   — called when stream ends (full = accumulated text)
    """
    payload = json.dumps({
        "model": model,
        "messages": messages,
        "stream": True,
    }).encode()
    req = urllib.request.Request(
        f"{AGENT_URL}/chat/completions",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        },
        method="POST",
    )
    full = []
    chunk_count = 0
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            for raw_line in resp:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\n\r")
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    print(f"[stream] done after {chunk_count} chunks, full_len={sum(len(t) for t in full)}", flush=True)
                    break
                try:
                    obj = json.loads(data)
                    delta = obj.get("choices", [{}])[0].get("delta", {})
                    text = delta.get("content", "")
                    if text:
                        chunk_count += 1
                        full.append(text)
                        on_chunk(text)
                except (json.JSONDecodeError, IndexError, KeyError):
                    pass
    except Exception as e:
        print(f"[stream] error after {chunk_count} chunks: {e}", flush=True)
        on_error(_friendly_error(e))
    on_done("".join(full))

# ── HTTP Handler ───────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass  # suppress default access log

    # ── routing ───────────────────────────────────────────────────────────────

    def _current_user(self):
        """Extract and verify Bearer token. Returns user dict or None."""
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return None
        token = auth[7:].strip()
        uid = _verify_token(token)
        if not uid:
            return None
        return get_user_by_id(uid)

    def _require_user(self):
        """Return user dict or send 401 and return None."""
        user = self._current_user()
        if not user:
            self._error(401, "Unauthorized")
        return user

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path

        if path == "/" or path == "/index.html":
            return self._serve_file(STATIC_DIR / "index.html", "text/html; charset=utf-8")
        if path.startswith("/static/"):
            rel = path[len("/static/"):]
            return self._serve_file(STATIC_DIR / rel)

        if path == "/api/auth/me":
            user = self._require_user()
            if not user: return
            return self._json({"id": user["id"], "username": user["username"], "role": user["role"]})

        if path == "/api/models":
            if not self._require_user(): return
            return self._json(proxy_models())
        if path == "/api/sessions":
            user = self._require_user()
            if not user: return
            return self._json({"sessions": list_sessions(user["id"])})
        if path.startswith("/api/sessions/"):
            user = self._require_user()
            if not user: return
            sid = path[len("/api/sessions/"):]
            sess = get_session(sid, user["id"])
            if not sess:
                return self._error(404, "Session not found")
            return self._json(sess)

        # ── Plugin panels (proxy to agent, rewrite URLs) ─────────────────────
        if path == "/api/panels":
            if not self._require_user(): return
            agent_base = _agent_base()
            try:
                req = urllib.request.Request(
                    f"{agent_base}/chat-ui/panels",
                    headers={"Accept": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=3) as resp:
                    data = json.loads(resp.read())
                    for panel in data.get("panels", []):
                        url = panel.get("url", "")
                        if url.startswith(agent_base):
                            panel["url"] = url[len(agent_base):]
                    return self._json(data)
            except Exception:
                return self._json({"panels": []})

        # ── Transparent proxy to agent (panel assets, workspace API, etc.) ───
        return self._proxy_to_agent(parsed)

    def do_POST(self):
        parsed = urlparse(self.path)
        path   = parsed.path
        body   = self._read_body()

        # ── Auth endpoints (no token required) ───────────────────────────────
        if path == "/api/auth/register":
            data = json.loads(body) if body else {}
            username = (data.get("username") or "").strip()
            password = data.get("password") or ""
            if not username or not password:
                return self._error(400, "username and password required")
            role = "admin" if count_users() == 0 else "user"
            user = create_user(username, password, role)
            if not user:
                return self._error(409, "Username already taken")
            token = _make_token(user["id"])
            return self._json({"token": token, "user": user}, 201)

        if path == "/api/auth/login":
            data = json.loads(body) if body else {}
            username = (data.get("username") or "").strip()
            password = data.get("password") or ""
            user = get_user_by_username(username)
            if not user or not _verify_password(password, user["password"]):
                return self._error(401, "Invalid username or password")
            token = _make_token(user["id"])
            return self._json({"token": token, "user": {"id": user["id"], "username": user["username"], "role": user["role"]}})

        # ── Session endpoints (token required) ───────────────────────────────
        if path == "/api/sessions":
            user = self._require_user()
            if not user: return
            data  = json.loads(body) if body else {}
            sess  = create_session(user["id"], data.get("title", "New chat"), data.get("model", ""))
            return self._json(sess, 201)

        if path.startswith("/api/sessions/") and path.endswith("/chat"):
            user = self._require_user()
            if not user: return
            sid = path[len("/api/sessions/"):-len("/chat")]
            return self._handle_chat(sid, body, user["id"])

        return self._error(404, "Not found")

    def do_PATCH(self):
        parsed = urlparse(self.path)
        path   = parsed.path
        body   = self._read_body()

        if path.startswith("/api/sessions/"):
            user = self._require_user()
            if not user: return
            sid  = path[len("/api/sessions/"):]
            data = json.loads(body) if body else {}
            update_session(sid, user["id"], **data)
            sess = get_session(sid, user["id"])
            return self._json(sess)

        return self._error(404, "Not found")

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path   = parsed.path

        if path.startswith("/api/sessions/"):
            user = self._require_user()
            if not user: return
            sid = path[len("/api/sessions/"):]
            delete_session(sid, user["id"])
            return self._json({"ok": True})

        return self._error(404, "Not found")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ── chat streaming ─────────────────────────────────────────────────────────

    def _handle_chat(self, sid, body, user_id: str):
        data    = json.loads(body) if body else {}
        content = data.get("content", "").strip()
        model   = data.get("model", "")
        images  = data.get("images", [])  # list of base64 data URLs
        if not content and not images:
            return self._error(400, "content required")

        sess = get_session(sid, user_id)
        if not sess:
            return self._error(404, "Session not found")

        # Persist user message (text only in DB)
        append_message(sid, "user", content)
        if model:
            update_session(sid, user_id, model=model)

        # Build message history for agent
        sess = get_session(sid, user_id)
        msgs = []
        for m in sess["messages"][:-1]:  # all but the last (just-appended) user msg
            msgs.append({"role": m["role"], "content": m["content"]})

        # Last user message: attach images if present
        if images:
            last_content = [{"type": "text", "text": content}]
            for img in images:
                last_content.append({"type": "image_url", "image_url": {"url": img}})
            msgs.append({"role": "user", "content": last_content})
        else:
            msgs.append({"role": "user", "content": content})

        # SSE response
        self.send_response(200)
        self._cors_headers()
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        def send_sse(event, data_str):
            try:
                msg = f"event: {event}\ndata: {data_str}\n\n".encode()
                # chunked encoding
                self.wfile.write(f"{len(msg):x}\r\n".encode())
                self.wfile.write(msg)
                self.wfile.write(b"\r\n")
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass

        def on_chunk(text):
            send_sse("delta", json.dumps({"content": text}))

        def on_error(msg):
            send_sse("error", json.dumps({"message": msg}))

        def on_done(full_text):
            if full_text:
                append_message(sid, "assistant", full_text)
            send_sse("done", json.dumps({"session_id": sid}))
            # Terminate chunked transfer
            try:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            except Exception:
                pass

        stream_chat(msgs, model or sess.get("model", ""), on_chunk, on_error, on_done)

    # ── agent proxy ────────────────────────────────────────────────────────────

    def _proxy_to_agent(self, parsed):
        """Transparently forward a GET request to the agent and return its response."""
        target = _agent_base() + parsed.path
        if parsed.query:
            target += "?" + parsed.query
        try:
            req = urllib.request.Request(target, headers={"Accept": "*/*"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read()
                self.send_response(resp.status)
                self._cors_headers()
                self.send_header("Content-Type",
                                 resp.headers.get("Content-Type", "application/octet-stream"))
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        except urllib.error.HTTPError as e:
            self._error(e.code, e.reason or str(e))
        except Exception as e:
            self._error(502, _friendly_error(e))

    # ── helpers ────────────────────────────────────────────────────────────────

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")

    def _json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self._cors_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, msg):
        self._json({"error": msg}, status)

    def _serve_file(self, path: Path, mime: str = None):
        if not path.exists() or not path.is_file():
            return self._error(404, "File not found")
        MIME = {
            ".html": "text/html; charset=utf-8",
            ".css":  "text/css; charset=utf-8",
            ".js":   "application/javascript; charset=utf-8",
            ".json": "application/json",
            ".png":  "image/png",
            ".svg":  "image/svg+xml",
            ".ico":  "image/x-icon",
        }
        if mime is None:
            mime = MIME.get(path.suffix.lower(), "application/octet-stream")
        data = path.read_bytes()
        self.send_response(200)
        self._cors_headers()
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    global AGENT_URL
    parser = argparse.ArgumentParser(description="chat-ui server")
    parser.add_argument("--port", "-p", type=int, default=int(os.environ.get("PORT", 9191)))
    parser.add_argument("--agent-url", default=AGENT_URL, help="OpenAI-compatible agent base URL")
    parser.add_argument("--no-open", action="store_true", help="don't open browser")
    args = parser.parse_args()
    AGENT_URL = args.agent_url.rstrip("/")

    init_db()

    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    url = f"http://127.0.0.1:{args.port}"
    print(f"chat-ui   →  {url}")
    print(f"agent     →  {AGENT_URL}")
    print("Ctrl-C to quit")

    if not args.no_open:
        import webbrowser, threading
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")

if __name__ == "__main__":
    main()
