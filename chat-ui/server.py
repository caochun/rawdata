#!/usr/bin/env python3
"""
chat-ui server — lightweight proxy-based chat UI.

Proxies to any OpenAI-compatible agent (default: hermes at port 8642).
No RAG, no local models, no heavy deps — just stdlib + PyYAML.

Usage:
    python server.py [--port PORT] [--agent-url URL]
"""
import argparse
import json
import os
import sqlite3
import threading
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# ── Config ─────────────────────────────────────────────────────────────────────
AGENT_URL     = os.environ.get("AGENT_URL", "http://127.0.0.1:8642/v1")
STATIC_DIR    = Path(__file__).parent / "static"
DB_PATH       = Path(__file__).parent / "chat.db"

# ── Database ───────────────────────────────────────────────────────────────────
_db_lock = threading.Lock()

def get_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                id      TEXT PRIMARY KEY,
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

def _now():
    return int(time.time() * 1000)

def _gen_id():
    import uuid
    return str(uuid.uuid4())

# ── Session CRUD ───────────────────────────────────────────────────────────────

def list_sessions():
    with _db_lock, get_db() as conn:
        rows = conn.execute(
            "SELECT id, title, model, created, updated FROM sessions ORDER BY updated DESC"
        ).fetchall()
        return [dict(r) for r in rows]

def create_session(title="New chat", model=""):
    sid = _gen_id()
    now = _now()
    with _db_lock, get_db() as conn:
        conn.execute(
            "INSERT INTO sessions (id, title, model, created, updated) VALUES (?,?,?,?,?)",
            (sid, title, model, now, now)
        )
    return {"id": sid, "title": title, "model": model, "created": now, "updated": now}

def get_session(sid):
    with _db_lock, get_db() as conn:
        row = conn.execute("SELECT * FROM sessions WHERE id=?", (sid,)).fetchone()
        if not row:
            return None
        msgs = conn.execute(
            "SELECT role, content, created FROM messages WHERE session_id=? ORDER BY id",
            (sid,)
        ).fetchall()
        return {**dict(row), "messages": [dict(m) for m in msgs]}

def update_session(sid, **kwargs):
    allowed = {"title", "model"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return
    fields["updated"] = _now()
    sets = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [sid]
    with _db_lock, get_db() as conn:
        conn.execute(f"UPDATE sessions SET {sets} WHERE id=?", vals)

def delete_session(sid):
    with _db_lock, get_db() as conn:
        conn.execute("DELETE FROM sessions WHERE id=?", (sid,))

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
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            for raw_line in resp:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\n\r")
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    break
                try:
                    obj = json.loads(data)
                    delta = obj.get("choices", [{}])[0].get("delta", {})
                    text = delta.get("content", "")
                    if text:
                        full.append(text)
                        on_chunk(text)
                except (json.JSONDecodeError, IndexError, KeyError):
                    pass
    except Exception as e:
        on_error(_friendly_error(e))
    on_done("".join(full))

# ── HTTP Handler ───────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass  # suppress default access log

    # ── routing ───────────────────────────────────────────────────────────────

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path

        if path == "/" or path == "/index.html":
            return self._serve_file(STATIC_DIR / "index.html", "text/html; charset=utf-8")
        if path.startswith("/static/"):
            rel = path[len("/static/"):]
            return self._serve_file(STATIC_DIR / rel)

        if path == "/api/models":
            return self._json(proxy_models())
        if path == "/api/sessions":
            return self._json({"sessions": list_sessions()})
        if path.startswith("/api/sessions/"):
            sid = path[len("/api/sessions/"):]
            sess = get_session(sid)
            if not sess:
                return self._error(404, "Session not found")
            return self._json(sess)

        # ── Plugin panels (proxy to agent, rewrite URLs) ─────────────────────
        if path == "/api/panels":
            agent_base = _agent_base()
            try:
                req = urllib.request.Request(
                    f"{agent_base}/chat-ui/panels",
                    headers={"Accept": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=3) as resp:
                    data = json.loads(resp.read())
                    # Rewrite absolute panel URLs → relative paths so they work
                    # through any public URL (Cloudflare Tunnel, reverse proxy, etc.)
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

        if path == "/api/sessions":
            data  = json.loads(body) if body else {}
            sess  = create_session(data.get("title", "New chat"), data.get("model", ""))
            return self._json(sess, 201)

        if path.startswith("/api/sessions/") and path.endswith("/chat"):
            sid = path[len("/api/sessions/"):-len("/chat")]
            return self._handle_chat(sid, body)

        return self._error(404, "Not found")

    def do_PATCH(self):
        parsed = urlparse(self.path)
        path   = parsed.path
        body   = self._read_body()

        if path.startswith("/api/sessions/"):
            sid  = path[len("/api/sessions/"):]
            data = json.loads(body) if body else {}
            update_session(sid, **data)
            sess = get_session(sid)
            return self._json(sess)

        return self._error(404, "Not found")

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path   = parsed.path

        if path.startswith("/api/sessions/"):
            sid = path[len("/api/sessions/"):]
            delete_session(sid)
            return self._json({"ok": True})

        return self._error(404, "Not found")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ── chat streaming ─────────────────────────────────────────────────────────

    def _handle_chat(self, sid, body):
        data    = json.loads(body) if body else {}
        content = data.get("content", "").strip()
        model   = data.get("model", "")
        if not content:
            return self._error(400, "content required")

        sess = get_session(sid)
        if not sess:
            return self._error(404, "Session not found")

        # Persist user message
        append_message(sid, "user", content)
        if model:
            update_session(sid, model=model)

        # Build message history for agent
        sess = get_session(sid)
        msgs = [{"role": m["role"], "content": m["content"]} for m in sess["messages"]]

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
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

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
