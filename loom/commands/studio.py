"""lm studio — visual schema/catalog modeling editor"""
from __future__ import annotations

import json
import threading
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import click
import yaml

# ── AI endpoint discovery ─────────────────────────────────────────
# Try hermes gateway first (full agent with loom tools), then raw LLM
_AI_ENDPOINTS = [
    "http://127.0.0.1:8642/v1/chat/completions",  # hermes gateway
    "http://127.0.0.1:8081/v1/chat/completions",  # local LLM direct
]

_SYSTEM_PROMPT = """\
你是 Loom 数据建模助手。Loom 是一个以 CSV 为存储的数据库系统，\
schema 定义了表结构，catalog 提供自然语言描述。

当用户描述数据模型需求时，请输出一个 JSON 代码块（```json ... ```），\
格式如下：
{
  "description": "整体数据集描述",
  "tables": {
    "表名": {
      "description": "这张表的用途",
      "columns": [
        {"name": "id",         "type": "uuid",     "primary": true,  "auto": true,  "description": "唯一标识"},
        {"name": "name",       "type": "string",   "required": true,              "description": "名称"},
        {"name": "status",     "type": "enum",     "values": ["active","inactive"],"description": "状态：active=启用，inactive=停用"},
        {"name": "created_at", "type": "datetime", "auto": true,                  "description": "创建时间"},
        {"name": "updated_at", "type": "datetime", "auto": true, "on_update": true,"description": "更新时间"}
      ],
      "merge_strategy": {}
    }
  },
  "relationships": [
    {"from": "orders", "fromCol": "user_id", "to": "users", "toCol": "id", "description": "订单所属用户"}
  ],
  "notes": ["补充说明..."]
}

字段类型共五种：uuid、string、number、datetime、enum。
- number：数值型，用于金额、比例、数量、分数等数字字段。
每张新表都应有 id（uuid, primary, auto）、created_at（datetime, auto）。
如果用户只是在对话而非要求生成模型，正常回答即可，不必输出 JSON。
当前画布状态会在用户消息中以 [当前模型] 块附带。\
"""


def _repo_root() -> Path:
    return Path.cwd()


def _static_html() -> bytes:
    p = Path(__file__).parent.parent / "static" / "studio.html"
    return p.read_bytes()


# ── YAML serialization ────────────────────────────────────────────

def _state_to_schema(state: dict) -> dict:
    schema: dict = {"tables": {}}
    for tname, tdata in state.get("tables", {}).items():
        cols: dict = {}
        for col in tdata.get("columns", []):
            col_def: dict = {"type": col["type"]}
            if col.get("description"): col_def["description"] = col["description"]
            if col.get("primary"):     col_def["primary"] = True
            if col.get("auto"):        col_def["auto"] = True
            if col.get("required"):    col_def["required"] = True
            if col.get("on_update"):   col_def["on_update"] = True
            if col.get("pattern"):     col_def["pattern"] = col["pattern"]
            if col.get("values"):      col_def["values"] = col["values"]
            cols[col["name"]] = col_def
        entry: dict = {"columns": cols}
        ms = tdata.get("merge_strategy") or {}
        if ms:
            entry["merge_strategy"] = ms
        schema["tables"][tname] = entry
    return schema


def _state_to_catalog(state: dict) -> dict:
    rels = []
    for r in state.get("relationships", []):
        rel: dict = {
            "from": r["from"], "fromCol": r["fromCol"],
            "to":   r["to"],   "toCol":   r["toCol"],
        }
        if r.get("description"):
            rel["description"] = r["description"]
        rels.append(rel)
    return {
        "description": state.get("description", ""),
        "tables": {
            t: d.get("description", "")
            for t, d in state.get("tables", {}).items()
        },
        "relationships": rels,
        "notes": state.get("notes", []),
    }


def _read_schema_state(root: Path) -> dict:
    """Read schema.yaml + catalog.yaml and return frontend-compatible state."""
    schema_path = root / "schema.yaml"
    catalog_path = root / "catalog.yaml"

    schema = {}
    if schema_path.exists():
        schema = yaml.safe_load(schema_path.read_text(encoding="utf-8")) or {}

    catalog = {}
    if catalog_path.exists():
        catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8")) or {}

    # Parse relationships from catalog (support both string and object format)
    import re as _re2
    rels = []
    for rel in (catalog.get("relationships") or []):
        if isinstance(rel, dict):
            rels.append({
                "from": rel.get("from", ""), "fromCol": rel.get("fromCol", ""),
                "to":   rel.get("to",   ""), "toCol":   rel.get("toCol",   ""),
                "description": rel.get("description", ""),
            })
        elif isinstance(rel, str):
            m = _re2.match(r"(\w+)\.(\w+)\s*(?:→|->)\s*(\w+)\.(\w+)", rel)
            if m:
                rels.append({"from": m[1], "fromCol": m[2], "to": m[3], "toCol": m[4], "description": ""})

    # Build tables merging schema + catalog descriptions
    schema_tables = schema.get("tables") or {}
    catalog_tables = catalog.get("tables") or {}
    all_names = list(dict.fromkeys(list(schema_tables.keys()) + list(catalog_tables.keys())))

    tables = {}
    xi, yi, ci = 60, 60, 0
    for tname in all_names:
        st = schema_tables.get(tname) or {}
        cols_raw = st.get("columns") or {}
        cols = []
        for cname, cdef in cols_raw.items():
            if not isinstance(cdef, dict):
                cdef = {"type": str(cdef)}
            cols.append({
                "name": cname,
                "type": cdef.get("type", "string"),
                "description": cdef.get("description", ""),
                "primary": bool(cdef.get("primary")),
                "auto":    bool(cdef.get("auto")),
                "required": bool(cdef.get("required")),
                "on_update": bool(cdef.get("on_update")),
                "pattern": cdef.get("pattern", ""),
                "values": list(cdef.get("values") or []),
            })
        tables[tname] = {
            "description": catalog_tables.get(tname, ""),
            "pos": {"x": xi, "y": yi},
            "columns": cols,
            "merge_strategy": dict(st.get("merge_strategy") or {}),
        }
        xi += 260
        ci += 1
        if ci % 4 == 0:
            xi = 60
            yi += 320

    return {
        "description": catalog.get("description", ""),
        "tables": tables,
        "relationships": rels,
        "notes": list(catalog.get("notes") or []),
    }


# ── HTTP Handler ──────────────────────────────────────────────────

class _Handler(BaseHTTPRequestHandler):
    root: Path  # set by factory

    def log_message(self, fmt, *args):  # suppress default access log
        pass

    def _send_json(self, data: dict, status: int = 200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _proxy_ai_stream(self, messages: list, context: dict):
        """Forward chat to hermes gateway or local LLM, stream SSE back to client."""
        # inject current canvas state into the last user message
        if messages and messages[-1]["role"] == "user" and context:
            ctx_str = json.dumps(context, ensure_ascii=False, indent=2)
            messages[-1]["content"] += f"\n\n[当前模型]\n```json\n{ctx_str}\n```"

        payload = json.dumps({
            "model": "hermes-agent",
            "messages": [{"role": "system", "content": _SYSTEM_PROMPT}] + messages,
            "stream": True,
            "temperature": 0.3,
        }).encode("utf-8")

        last_err = None
        for endpoint in _AI_ENDPOINTS:
            try:
                req = urllib.request.Request(
                    endpoint, data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=120) as resp:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    while True:
                        chunk = resp.read(512)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        self.wfile.flush()
                return  # success
            except Exception as exc:
                last_err = exc
                continue

        # all endpoints failed — send error as SSE
        err_msg = f"AI 不可用：{last_err}"
        err_chunk = (
            f'data: {json.dumps({"choices":[{"delta":{"content": err_msg},"finish_reason":"stop","index":0}]})}\n\n'
            'data: [DONE]\n\n'
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.end_headers()
        self.wfile.write(err_chunk.encode("utf-8"))

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            html = _static_html()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(html))
            self.end_headers()
            self.wfile.write(html)

        elif path == "/api/schema":
            try:
                state = _read_schema_state(self.root)
                self._send_json(state)
            except Exception as exc:
                self._send_json({"error": str(exc)}, 500)

        elif path == "/api/health":
            self._send_json({"ok": True})

        elif path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()

        else:
            self._send_json({"error": "not found"}, 404)

    def do_POST(self):
        path = self.path.split("?")[0]

        if path == "/api/ai":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
                self._proxy_ai_stream(data.get("messages", []), data.get("context", {}))
            except Exception as exc:
                self._send_json({"error": str(exc)}, 400)
            return

        if path == "/api/import":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                payload = json.loads(body)
                schema_yaml  = payload.get("schema_yaml",  "") or ""
                catalog_yaml = payload.get("catalog_yaml", "") or ""
                schema  = yaml.safe_load(schema_yaml)  if schema_yaml.strip()  else {}
                catalog = yaml.safe_load(catalog_yaml) if catalog_yaml.strip() else {}
                schema  = schema  if isinstance(schema,  dict) else {}
                catalog = catalog if isinstance(catalog, dict) else {}
                # build a temporary merged state (same logic as _read_schema_state)
                import re as _re
                rels = []
                for rel in (catalog.get("relationships") or []):
                    if isinstance(rel, dict):
                        rels.append({"from": rel.get("from",""), "fromCol": rel.get("fromCol",""),
                                     "to": rel.get("to",""),   "toCol": rel.get("toCol",""),
                                     "description": rel.get("description","")})
                    elif isinstance(rel, str):
                        m = _re.match(r"(\w+)\.(\w+)\s*(?:→|->)\s*(\w+)\.(\w+)", rel)
                        if m:
                            rels.append({"from": m[1], "fromCol": m[2], "to": m[3], "toCol": m[4], "description": ""})
                schema_tables  = schema.get("tables") or {}
                catalog_tables = catalog.get("tables") or {}
                all_names = list(dict.fromkeys(list(schema_tables.keys()) + list(catalog_tables.keys())))
                tables: dict = {}
                xi, yi, ci = 60, 60, 0
                for tname in all_names:
                    st = schema_tables.get(tname) or {}
                    cols_raw = st.get("columns") or {}
                    cols = []
                    for cname, cdef in cols_raw.items():
                        if not isinstance(cdef, dict):
                            cdef = {"type": str(cdef)}
                        cols.append({
                            "name": cname, "type": cdef.get("type", "string"),
                            "description": cdef.get("description", ""),
                            "primary": bool(cdef.get("primary")), "auto": bool(cdef.get("auto")),
                            "required": bool(cdef.get("required")), "on_update": bool(cdef.get("on_update")),
                            "pattern": cdef.get("pattern", ""),
                            "values": list(cdef.get("values") or []),
                        })
                    tables[tname] = {
                        "description": catalog_tables.get(tname, ""),
                        "pos": {"x": xi, "y": yi},
                        "columns": cols,
                        "merge_strategy": dict(st.get("merge_strategy") or {}),
                    }
                    xi += 260; ci += 1
                    if ci % 4 == 0: xi = 60; yi += 320
                self._send_json({
                    "description": catalog.get("description", ""),
                    "tables": tables, "relationships": rels,
                    "notes": list(catalog.get("notes") or []),
                })
            except Exception as exc:
                self._send_json({"error": str(exc)}, 400)
            return

        if path == "/api/schema":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                state = json.loads(body)
                schema = _state_to_schema(state)
                catalog = _state_to_catalog(state)

                # preserve positions in a .loom_studio_layout.json file
                layout = {t: d["pos"] for t, d in state.get("tables", {}).items()}
                (self.root / ".loom_studio_layout.json").write_text(
                    json.dumps(layout, indent=2), encoding="utf-8"
                )

                (self.root / "schema.yaml").write_text(
                    yaml.dump(schema, allow_unicode=True, sort_keys=False, default_flow_style=False),
                    encoding="utf-8",
                )
                (self.root / "catalog.yaml").write_text(
                    yaml.dump(catalog, allow_unicode=True, sort_keys=False, default_flow_style=False),
                    encoding="utf-8",
                )
                self._send_json({"ok": True})
            except Exception as exc:
                self._send_json({"ok": False, "error": str(exc)}, 400)
        else:
            self._send_json({"error": "not found"}, 404)


def _make_handler(root: Path):
    class H(_Handler):
        pass
    H.root = root
    return H


# ── CLI Command ───────────────────────────────────────────────────

@click.command()
@click.option("--port", "-p", default=9090, show_default=True, help="Port to listen on")
@click.option("--no-open", "no_open", is_flag=True, default=False, help="Do not open browser automatically")
def studio(port: int, no_open: bool):
    """Start the visual schema/catalog modeling studio."""
    root = _repo_root()
    schema_path = root / "schema.yaml"
    catalog_path = root / "catalog.yaml"

    if not schema_path.exists() and not catalog_path.exists():
        click.echo(f"  No schema.yaml or catalog.yaml found in {root}")
        click.echo("  Starting with an empty canvas.")
    else:
        found = []
        if schema_path.exists():  found.append("schema.yaml")
        if catalog_path.exists(): found.append("catalog.yaml")
        click.echo(f"  Found: {', '.join(found)}")

    url = f"http://localhost:{port}"
    server = HTTPServer(("127.0.0.1", port), _make_handler(root))

    click.echo(f"  Loom Studio → {url}")
    click.echo("  Press Ctrl+C to stop.\n")

    if not no_open:
        t = threading.Timer(0.4, lambda: webbrowser.open(url))
        t.daemon = True
        t.start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        click.echo("\n  Studio stopped.")
