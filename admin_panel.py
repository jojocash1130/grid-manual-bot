# -*- coding: utf-8 -*-
"""
Grid Manual Bot Admin Panel — FastAPI multi-user management.
Two views: Admin (/) and User (/dashboard/{id}).
Bots run as independent processes that survive admin restarts.
Manual grid only — no auto-direction mode.
"""
import asyncio
import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse
import uvicorn

import user_manager

app = FastAPI(title="Grid Manual Bot Admin")

# Backfill invite codes for existing users on startup
user_manager.ensure_invite_codes()

# --- Private Network Access header (Chrome security) ---
class PrivateNetworkMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method == "OPTIONS" and request.headers.get("access-control-request-private-network"):
            return StarletteResponse(status_code=200, headers={
                "Access-Control-Allow-Private-Network": "true",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "*",
                "Access-Control-Allow-Headers": "*",
            })
        response = await call_next(request)
        response.headers["Access-Control-Allow-Private-Network"] = "true"
        return response

app.add_middleware(PrivateNetworkMiddleware)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DATA_DIR = Path(__file__).parent / "data"
GRID_SCRIPT = str(Path(__file__).parent / "grid_manual.py")


# ============================================================
# PID file management
# ============================================================

def _pid_file(user_id: str) -> Path:
    return DATA_DIR / user_id / "bot.pid"

def _save_pid(user_id: str, pid: int):
    path = _pid_file(user_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(pid))

def _read_pid(user_id: str) -> int | None:
    path = _pid_file(user_id)
    if not path.exists():
        return None
    try:
        return int(path.read_text().strip())
    except (ValueError, OSError):
        return None

def _clear_pid(user_id: str):
    path = _pid_file(user_id)
    if path.exists():
        path.unlink()

def _is_pid_alive(pid: int) -> bool:
    try:
        if sys.platform == "win32":
            import ctypes
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000, False, pid)
            if handle:
                exit_code = ctypes.c_ulong()
                kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
                kernel32.CloseHandle(handle)
                return exit_code.value == 259
            return False
        else:
            os.kill(pid, 0)
            return True
    except (OSError, PermissionError):
        return False

def _is_bot_running(user_id: str) -> tuple[bool, int | None]:
    pid = _read_pid(user_id)
    if pid and _is_pid_alive(pid):
        return True, pid
    if pid:
        _clear_pid(user_id)
    return False, None


# ============================================================
# Notification system
# ============================================================
_NOTICE_FILE = DATA_DIR / "notice.json"

def _get_notice() -> dict | None:
    if not _NOTICE_FILE.exists():
        return None
    try:
        data = json.loads(_NOTICE_FILE.read_text(encoding="utf-8"))
        return data if data.get("message") else None
    except Exception:
        return None

def _set_notice(message: str):
    _NOTICE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _NOTICE_FILE.write_text(
        json.dumps({"message": message, "ts": __import__("time").time()}, ensure_ascii=False),
        encoding="utf-8",
    )

def _clear_notice():
    if _NOTICE_FILE.exists():
        _NOTICE_FILE.unlink()

@app.get("/api/notice")
async def api_get_notice():
    return _get_notice() or {"message": ""}

@app.post("/api/notice")
async def api_post_notice(request: Request):
    body = await request.json()
    msg = body.get("message", "").strip()
    if msg:
        _set_notice(msg)
    else:
        _clear_notice()
    return {"status": "ok"}

@app.delete("/api/notice")
async def api_delete_notice():
    _clear_notice()
    return {"status": "ok"}


# ============================================================
# API — shared
# ============================================================

@app.get("/api/users")
async def api_list_users():
    users = user_manager.list_users()
    for u in users:
        running, pid = _is_bot_running(u["user_id"])
        u["running"] = running
        u["pid"] = pid
    return users


@app.post("/api/auth/invite")
async def api_auth_invite(request: Request):
    """Validate invite code, return user_id if valid."""
    body = await request.json()
    code = body.get("code", "").strip()
    if not code:
        raise HTTPException(400, "Invite code required")
    cfg = user_manager.get_user_by_invite(code)
    if not cfg:
        raise HTTPException(401, "Invalid invite code")
    return {"status": "ok", "user_id": cfg["user_id"]}


@app.get("/api/users/{user_id}")
async def api_get_user(user_id: str):
    cfg = user_manager.get_user(user_id)
    if not cfg:
        raise HTTPException(404, "User not found")
    running, pid = _is_bot_running(user_id)
    return {
        "user_id": cfg["user_id"],
        "api_key": (cfg["api_key"][:8] + "...") if cfg.get("api_key") else "",
        "key_configured": bool(cfg.get("api_key")),
        "source_ip": cfg.get("source_ip", ""),
        "public_ip": cfg.get("public_ip", ""),
        "port": cfg["port"],
        "order_qty": cfg.get("order_qty", "0.001"),
        "running": running,
        "pid": pid,
    }


# ============================================================
# API — admin only
# ============================================================

@app.post("/api/admin/slots")
async def api_create_slot(request: Request):
    """Admin creates a user slot (Source IP + Port, no API key)."""
    body = await request.json()
    user_id = body.get("user_id", "").strip()
    source_ip = body.get("source_ip", "").strip()
    public_ip = body.get("public_ip", "").strip()
    port = int(body.get("port", 5560))
    if not user_id:
        raise HTTPException(400, "user_id is required")
    user_manager.create_slot(user_id, source_ip, port, public_ip=public_ip)
    return {"status": "ok", "user_id": user_id}


@app.delete("/api/admin/users/{user_id}")
async def api_admin_delete_user(user_id: str):
    running, _ = _is_bot_running(user_id)
    if running:
        await api_stop_bot(user_id)
    user_manager.delete_user(user_id)
    return {"status": "ok"}


@app.post("/api/users/{user_id}/start")
async def api_start_bot(user_id: str):
    running, _ = _is_bot_running(user_id)
    if running:
        raise HTTPException(400, f"Bot {user_id} is already running")

    cfg = user_manager.get_user(user_id)
    if not cfg:
        raise HTTPException(404, f"User {user_id} not found")

    env = {**os.environ}
    env["CRO_API_KEY"] = cfg.get("api_key", "")
    env["CRO_SECRET_KEY"] = cfg.get("secret_key", "")
    env["SOURCE_IP"] = cfg.get("source_ip", "")
    env["DASHBOARD_PORT"] = str(cfg["port"])
    env["USER_ID"] = user_id

    log_dir = DATA_DIR / user_id
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = open(log_dir / "process.log", "a", encoding="utf-8")

    kwargs = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    else:
        kwargs["start_new_session"] = True

    proc = subprocess.Popen(
        [sys.executable, GRID_SCRIPT],
        env=env,
        cwd=str(Path(__file__).parent),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        **kwargs,
    )

    _save_pid(user_id, proc.pid)
    return {"status": "started", "pid": proc.pid, "port": cfg["port"]}


@app.post("/api/users/{user_id}/stop")
async def api_stop_bot(user_id: str):
    running, pid = _is_bot_running(user_id)
    if not running:
        _clear_pid(user_id)
        return {"status": "already_stopped"}

    try:
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)
        else:
            os.kill(pid, signal.SIGTERM)
            import time
            for _ in range(20):
                if not _is_pid_alive(pid):
                    break
                time.sleep(0.5)
            if _is_pid_alive(pid):
                os.kill(pid, signal.SIGKILL)
    except (OSError, PermissionError):
        pass

    _clear_pid(user_id)
    return {"status": "stopped"}


@app.post("/api/admin/restart-all")
async def api_restart_all():
    """Stop all running bots, wait, then start them again. For deploying code updates."""
    import time as _time
    users = user_manager.list_users()
    results = []

    # 1. Collect running bots
    running_ids = []
    for u in users:
        uid = u["user_id"]
        running, _ = _is_bot_running(uid)
        if running:
            running_ids.append(uid)

    if not running_ids:
        return {"status": "ok", "message": "No running bots", "results": []}

    # 2. Stop all
    for uid in running_ids:
        try:
            await api_stop_bot(uid)
            results.append({"user_id": uid, "stop": "ok"})
        except Exception as e:
            results.append({"user_id": uid, "stop": f"error: {e}"})

    # 3. Wait for processes to fully exit
    _time.sleep(2)

    # 4. Start all
    for r in results:
        uid = r["user_id"]
        try:
            resp = await api_start_bot(uid)
            r["start"] = "ok"
            r["pid"] = resp.get("pid")
        except Exception as e:
            r["start"] = f"error: {e}"

    return {"status": "ok", "message": f"Restarted {len(running_ids)} bots", "results": results}


@app.get("/api/users/{user_id}/status")
async def api_user_status(user_id: str):
    cfg = user_manager.get_user(user_id)
    if not cfg:
        raise HTTPException(404, f"User {user_id} not found")
    running, pid = _is_bot_running(user_id)
    if not running:
        return {"running": False, "user_id": user_id}
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"http://127.0.0.1:{cfg['port']}/api/stats")
            stats = resp.json()
    except Exception:
        stats = {}
    return {"running": True, "user_id": user_id, "port": cfg["port"], "pid": pid, "stats": stats}


@app.get("/dashboard/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_landing():
    """Invite code entry page."""
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Grid Bot — Enter Invite Code</title>
<style>{_CSS}
.invite-wrap{{display:flex;align-items:center;justify-content:center;min-height:100vh}}
.invite-card{{background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:40px;width:100%;max-width:420px;text-align:center}}
.invite-card h1{{font-size:24px;font-weight:700;margin-bottom:8px}}
.invite-card p{{color:var(--text2);font-size:14px;margin-bottom:24px}}
.invite-input{{width:100%;padding:14px 18px;background:var(--bg3);border:2px solid var(--border);border-radius:10px;color:var(--text);font-size:18px;font-weight:600;text-align:center;letter-spacing:3px;outline:none;text-transform:uppercase}}
.invite-input:focus{{border-color:var(--blue)}}
.invite-input::placeholder{{letter-spacing:1px;font-weight:400;text-transform:none}}
.invite-btn{{width:100%;margin-top:16px;padding:14px;background:var(--blue);color:#fff;border:none;border-radius:10px;font-size:16px;font-weight:700;cursor:pointer;transition:opacity 0.2s}}
.invite-btn:hover{{opacity:0.85}}
.invite-btn:disabled{{opacity:0.4;cursor:not-allowed}}
.invite-err{{color:var(--red);font-size:14px;margin-top:12px;min-height:20px}}
.invite-ok{{color:var(--green);font-size:14px;margin-top:12px}}
</style></head><body>
<div class="invite-wrap">
<div class="invite-card">
  <h1>Grid Bot</h1>
  <p>Enter your invitation code to access the dashboard</p>
  <input class="invite-input" id="invite-code" placeholder="Enter code" maxlength="12" autofocus
    onkeydown="if(event.key==='Enter')submitCode()">
  <button class="invite-btn" id="invite-btn" onclick="submitCode()">Enter</button>
  <div class="invite-err" id="invite-msg"></div>
</div>
</div>
<script>
async function submitCode() {{
  var code = document.getElementById('invite-code').value.trim();
  if (!code) return;
  var btn = document.getElementById('invite-btn');
  var msg = document.getElementById('invite-msg');
  btn.disabled = true; btn.textContent = 'Verifying...';
  msg.textContent = ''; msg.className = 'invite-err';
  try {{
    var r = await fetch('/api/auth/invite', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{code: code}})
    }});
    if (!r.ok) {{
      var e = await r.json();
      msg.textContent = e.detail || 'Invalid code';
      btn.disabled = false; btn.textContent = 'Enter';
      return;
    }}
    var d = await r.json();
    localStorage.setItem('grid_invite_' + d.user_id, '1');
    msg.textContent = 'Welcome! Redirecting...';
    msg.className = 'invite-ok';
    setTimeout(function() {{ window.location.href = '/dashboard/' + d.user_id; }}, 500);
  }} catch(err) {{
    msg.textContent = 'Network error';
    btn.disabled = false; btn.textContent = 'Enter';
  }}
}}
</script></body></html>"""


@app.post("/api/dashboard/{user_id}/save-keys")
async def api_dashboard_save_keys(user_id: str, request: Request):
    """User saves API key/secret from the not-running setup page."""
    cfg = user_manager.get_user(user_id)
    if not cfg:
        raise HTTPException(404, "User not found")
    body = await request.json()
    ak = body.get("api_key", "").strip()
    sk = body.get("secret_key", "").strip()
    if not ak or not sk:
        raise HTTPException(400, "Both API Key and Secret Key are required")
    cfg["api_key"] = ak
    cfg["secret_key"] = sk
    f = user_manager._get_fernet()
    encrypted = f.encrypt(json.dumps(cfg).encode())
    user_manager._config_path(user_id).write_bytes(encrypted)
    return {"status": "ok"}


@app.get("/dashboard/{user_id}")
async def dashboard_page(user_id: str, request: Request):
    cfg = user_manager.get_user(user_id)
    if not cfg:
        raise HTTPException(404, f"User {user_id} not found")

    running, _ = _is_bot_running(user_id)
    has_key = bool(cfg.get("api_key"))
    if not running:
        return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Grid Bot — {user_id}</title>
<style>{_CSS}
.setup-wrap{{display:flex;align-items:center;justify-content:center;min-height:100vh}}
.setup-card{{background:var(--bg2);border:1px solid var(--border);border-radius:16px;padding:40px;width:100%;max-width:480px}}
.setup-card h1{{font-size:24px;font-weight:700;margin-bottom:4px}}
.setup-card .sub{{color:var(--text2);font-size:14px;margin-bottom:24px}}
.status-badge{{display:inline-block;padding:4px 12px;border-radius:20px;font-size:13px;font-weight:600;margin-bottom:20px}}
.status-badge.stopped{{background:rgba(239,68,68,0.15);color:var(--red);border:1px solid rgba(239,68,68,0.3)}}
.status-badge.nokey{{background:rgba(245,158,11,0.15);color:var(--yellow);border:1px solid rgba(245,158,11,0.3)}}
.status-badge.ready{{background:rgba(16,185,129,0.15);color:var(--green);border:1px solid rgba(16,185,129,0.3)}}
.key-section{{background:var(--bg3);border:1px solid var(--border);border-radius:10px;padding:16px;margin-bottom:16px}}
.key-section h3{{font-size:14px;font-weight:600;margin-bottom:12px}}
.setup-msg{{font-size:14px;margin-top:12px;min-height:20px}}
.setup-msg.ok{{color:var(--green)}}.setup-msg.err{{color:var(--red)}}
</style></head><body>
<div class="setup-wrap">
<div class="setup-card">
  <h1>Grid Bot — {user_id}</h1>
  <p class="sub">Configure your bot and start trading</p>
  <span class="status-badge {'nokey' if not has_key else 'stopped'}" id="status-badge">
    {'API Key Not Set' if not has_key else 'Bot Stopped'}
  </span>

  <div class="key-section">
    <h3>API Key Settings</h3>
    <div class="form-group"><label>API Key</label>
      <input id="f_ak" placeholder="{'Enter your Crypto.com API Key' if not has_key else 'Key configured (enter new to change)'}" type="password"></div>
    <div class="form-group"><label>Secret Key</label>
      <input id="f_sk" placeholder="{'Enter your Secret Key' if not has_key else 'Secret configured (enter new to change)'}" type="password"></div>
    <button class="btn btn-start" onclick="saveKeys()" style="margin-top:8px">Save Keys</button>
  </div>

  <div style="display:flex;gap:8px">
    <button class="btn btn-start" id="btn-start" onclick="startBot()" style="flex:1;padding:14px;font-size:16px" {'disabled' if not has_key else ''}>
      Start Bot</button>
  </div>
  <div class="setup-msg" id="msg"></div>

  <div style="margin-top:20px;padding-top:16px;border-top:1px solid var(--border);font-size:13px;color:var(--text2)">
    <strong>Whitelist IP:</strong> <span style="color:var(--blue);font-family:monospace">{cfg.get('public_ip', 'N/A')}</span><br>
    Add this IP to your Crypto.com API key whitelist.
  </div>
</div>
</div>
<script>
const UID = '{user_id}';
if (!localStorage.getItem('grid_invite_' + UID)) {{
  window.location.replace('/dashboard/');
}}
async function saveKeys() {{
  const ak = document.getElementById('f_ak').value.trim();
  const sk = document.getElementById('f_sk').value.trim();
  const msg = document.getElementById('msg');
  if (!ak || !sk) {{ msg.textContent = 'Please enter both API Key and Secret Key'; msg.className = 'setup-msg err'; return; }}
  try {{
    const r = await fetch('/api/dashboard/' + UID + '/save-keys', {{
      method: 'POST', headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{api_key: ak, secret_key: sk}})
    }});
    if (!r.ok) {{ const e = await r.json(); throw new Error(e.detail); }}
    msg.textContent = 'Keys saved! You can now start the bot.'; msg.className = 'setup-msg ok';
    document.getElementById('btn-start').disabled = false;
    document.getElementById('status-badge').textContent = 'Bot Stopped';
    document.getElementById('status-badge').className = 'status-badge stopped';
  }} catch(e) {{ msg.textContent = e.message; msg.className = 'setup-msg err'; }}
}}
async function startBot() {{
  const msg = document.getElementById('msg');
  const btn = document.getElementById('btn-start');
  btn.disabled = true; btn.textContent = 'Starting...';
  try {{
    const r = await fetch('/api/users/' + UID + '/start', {{method: 'POST'}});
    const d = await r.json();
    if (!r.ok) {{ throw new Error(d.detail || JSON.stringify(d)); }}
    msg.textContent = 'Bot started! Waiting for dashboard...'; msg.className = 'setup-msg ok';
    btn.textContent = 'Starting...';
    let tries = 0;
    const poll = setInterval(async () => {{
      tries++;
      try {{
        const chk = await fetch('/bot-proxy/' + UID + '/api/stats', {{method:'GET'}});
        if (chk.ok) {{ clearInterval(poll); location.reload(); }}
      }} catch(e) {{}}
      if (tries > 15) {{ clearInterval(poll); msg.textContent = 'Bot started but dashboard slow to respond. Try refreshing in a few seconds.'; }}
    }}, 2000);
  }} catch(e) {{
    msg.textContent = 'Start failed: ' + e.message; msg.className = 'setup-msg err';
    btn.disabled = false; btn.textContent = 'Start Bot';
  }}
}}
</script></body></html>""")
    port = cfg["port"]
    return HTMLResponse(f'''<html><head><meta charset="utf-8"><style>
body{{margin:0;overflow:hidden;background:#0f1419}}
.dash-notice{{background:rgba(245,158,11,0.15);border-bottom:2px solid rgba(245,158,11,0.4);
  padding:10px 20px;color:#f59e0b;font-family:-apple-system,sans-serif;font-size:14px;font-weight:600;
  display:none;text-align:center}}
.dash-notice.show{{display:block}}
iframe{{width:100%;border:none}}
</style></head><body>
<div id="dash-notice" class="dash-notice"><span id="dash-notice-text"></span>
<button onclick="dismissDashNotice()" style="background:none;border:none;color:#f59e0b;cursor:pointer;font-size:18px;margin-left:12px">&times;</button></div>
<iframe id="dash-frame" style="display:none"></iframe>
<script>
if (!localStorage.getItem('grid_invite_{user_id}')) {{
  window.location.replace('/dashboard/');
}} else {{
  document.getElementById('dash-frame').src = '/bot-proxy/{user_id}/';
  document.getElementById('dash-frame').style.display = 'block';
}}
function resizeFrame() {{
  const notice = document.getElementById('dash-notice');
  const frame = document.getElementById('dash-frame');
  frame.style.height = (window.innerHeight - notice.offsetHeight) + 'px';
}}
async function checkNotice() {{
  try {{
    const r = await fetch('/api/notice');
    const d = await r.json();
    const el = document.getElementById('dash-notice');
    const txt = document.getElementById('dash-notice-text');
    if (d.ts && (Date.now()/1000 - d.ts) > 86400) {{ resizeFrame(); return; }}
    const dismissed = localStorage.getItem('notice_dismissed');
    if (d.message && d.message !== dismissed) {{
      txt.textContent = d.message; el.classList.add('show');
    }} else {{
      el.classList.remove('show');
      if (!d.message) localStorage.removeItem('notice_dismissed');
    }}
    resizeFrame();
  }} catch(e) {{}}
}}
function dismissDashNotice() {{
  const txt = document.getElementById('dash-notice-text').textContent;
  localStorage.setItem('notice_dismissed', txt);
  document.getElementById('dash-notice').classList.remove('show');
  resizeFrame();
}}
checkNotice();
setInterval(checkNotice, 5000);
window.addEventListener('resize', resizeFrame);
resizeFrame();
</script></body></html>''')


def _get_host():
    return os.getenv("EXTERNAL_HOST", "localhost")


# ============================================================
# Bot dashboard reverse proxy — all traffic stays on port 80
# ============================================================

@app.api_route("/bot-proxy/{user_id}/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
async def bot_proxy(user_id: str, path: str, request: Request):
    cfg = user_manager.get_user(user_id)
    if not cfg:
        raise HTTPException(404, "User not found")
    port = cfg["port"]
    target = f"http://127.0.0.1:{port}/{path}"
    qs = str(request.url.query)
    if qs:
        target += f"?{qs}"
    body = await request.body()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.request(
                method=request.method,
                url=target,
                headers={k: v for k, v in request.headers.items()
                         if k.lower() not in ("host", "transfer-encoding")},
                content=body if body else None,
            )
    except (httpx.ConnectError, httpx.ConnectTimeout):
        return HTMLResponse(
            f"<h2>Bot {user_id} is not responding</h2>"
            f"<p>The bot may have crashed or is still starting up.</p>"
            f"<a href='/user/{user_id}'>&#8592; Back to control panel</a>",
            status_code=502,
        )
    excluded = {"transfer-encoding", "content-encoding", "content-length"}
    headers = {k: v for k, v in resp.headers.items() if k.lower() not in excluded}

    # Rewrite Socket.IO path in HTML so client connects via proxy
    content_type = resp.headers.get("content-type", "")
    if "text/html" in content_type:
        html = resp.content.decode("utf-8", errors="replace")
        # Fix Socket.IO path
        html = html.replace(
            "const socket = io();",
            f"const socket = io({{path: '/bot-proxy/{user_id}/socket.io/'}});",
            1,
        )
        # Inject fetch interceptor so all /api/* calls go through proxy
        inject = f"""<script>
(function(){{
  const _fetch = window.fetch;
  const PROXY = '/bot-proxy/{user_id}';
  window.PROXY = PROXY;
  window.fetch = function(url, opts) {{
    if (typeof url === 'string' && url.startsWith('/') && !url.startsWith(PROXY)) {{
      url = PROXY + url;
    }}
    return _fetch(url, opts);
  }};
}})();
</script>"""
        html = html.replace("<head>", "<head>" + inject, 1)
        headers["content-type"] = "text/html; charset=utf-8"
        return StarletteResponse(content=html.encode("utf-8"), status_code=resp.status_code, headers=headers)

    return StarletteResponse(content=resp.content, status_code=resp.status_code, headers=headers)


# --- WebSocket proxy for Socket.IO ---
from starlette.websockets import WebSocket, WebSocketDisconnect
import websockets as _ws_lib

@app.websocket("/bot-proxy/{user_id}/socket.io/")
async def bot_ws_proxy(user_id: str, websocket: WebSocket):
    cfg = user_manager.get_user(user_id)
    if not cfg:
        await websocket.close(code=1008)
        return
    port = cfg["port"]
    qs = str(websocket.url.query)
    target = f"ws://127.0.0.1:{port}/socket.io/?{qs}"

    await websocket.accept()
    try:
        async with _ws_lib.connect(target, ping_interval=25, ping_timeout=10) as upstream:
            async def client_to_upstream():
                try:
                    while True:
                        data = await websocket.receive_text()
                        await upstream.send(data)
                except WebSocketDisconnect:
                    pass

            async def upstream_to_client():
                try:
                    async for msg in upstream:
                        await websocket.send_text(msg)
                except _ws_lib.exceptions.ConnectionClosed:
                    pass

            await asyncio.gather(client_to_upstream(), upstream_to_client())
    except Exception:
        pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


# ============================================================
# Shared CSS
# ============================================================
_CSS = """
:root{
  --bg:#0f1419;--bg2:#1a1d29;--bg3:#252936;
  --text:#e5e7eb;--text2:#9ca3af;--muted:#6b7280;
  --green:#10b981;--red:#ef4444;--blue:#3b82f6;--yellow:#f59e0b;
  --border:#2d3748;
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
.navbar{background:var(--bg2);border-bottom:1px solid var(--border);padding:16px 24px;display:flex;justify-content:space-between;align-items:center}
.navbar h1{font-size:22px;font-weight:700}
.main{max-width:900px;margin:0 auto;padding:24px}
.card{background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:16px}
.card-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.card-title{font-size:18px;font-weight:700}
.badge{padding:4px 12px;border-radius:20px;font-size:13px;font-weight:600}
.badge.running{background:rgba(16,185,129,0.15);color:var(--green);border:1px solid rgba(16,185,129,0.3)}
.badge.stopped{background:rgba(239,68,68,0.15);color:var(--red);border:1px solid rgba(239,68,68,0.3)}
.badge.nokey{background:rgba(245,158,11,0.15);color:var(--yellow);border:1px solid rgba(245,158,11,0.3)}
.info-row{display:flex;gap:24px;margin-bottom:8px;font-size:14px;color:var(--text2)}
.info-row span{color:var(--text)}
.btn-group{display:flex;gap:8px;margin-top:12px}
.btn{padding:8px 20px;border:none;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;transition:all 0.2s}
.btn-start{background:var(--green);color:#fff}.btn-start:hover{opacity:0.85}
.btn-stop{background:var(--red);color:#fff}.btn-stop:hover{opacity:0.85}
.btn-dash{background:var(--blue);color:#fff}.btn-dash:hover{opacity:0.85}
.btn-del{background:transparent;color:var(--muted);border:1px solid var(--border)}.btn-del:hover{color:var(--red);border-color:var(--red)}
.btn:disabled{opacity:0.4;cursor:not-allowed}
h2{font-size:18px;font-weight:700;margin-bottom:16px}
.form-group{margin-bottom:12px}
.form-group label{display:block;font-size:13px;color:var(--text2);margin-bottom:4px}
.form-group input{width:100%;padding:10px 14px;background:var(--bg3);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:14px;outline:none}
.form-group input:focus{border-color:var(--blue)}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:12px}
#msg{margin-top:12px;padding:10px;border-radius:8px;font-size:14px;display:none}
#msg.ok{display:block;background:rgba(16,185,129,0.1);color:var(--green);border:1px solid rgba(16,185,129,0.3)}
#msg.err{display:block;background:rgba(239,68,68,0.1);color:var(--red);border:1px solid rgba(239,68,68,0.3)}
.notice-banner{background:rgba(245,158,11,0.12);border:1px solid rgba(245,158,11,0.4);border-radius:12px;padding:16px 20px;margin-bottom:16px;display:none;align-items:center;justify-content:space-between;gap:12px}
.notice-banner.show{display:flex}
.notice-banner .notice-text{color:var(--yellow);font-size:15px;font-weight:600;flex:1}
.notice-banner .notice-close{background:none;border:none;color:var(--muted);cursor:pointer;font-size:18px;padding:4px 8px}
.notice-admin{background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:16px 20px;margin-bottom:16px}
.notice-admin h3{font-size:15px;font-weight:700;margin-bottom:8px;color:var(--yellow)}
.notice-admin .notice-input-row{display:flex;gap:8px}
.notice-admin input{flex:1;padding:10px 14px;background:var(--bg3);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:14px;outline:none}
.notice-admin input:focus{border-color:var(--yellow)}
.btn-notify{background:var(--yellow);color:#000;font-weight:600}
.btn-clear-notice{background:transparent;color:var(--muted);border:1px solid var(--border)}
.url-box{background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:8px 14px;font-family:monospace;font-size:13px;color:var(--blue);word-break:break-all;margin-top:4px}
"""


# ============================================================
# Admin page (/) — full control + user list
# ============================================================

ADMIN_HTML = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Grid Manual Bot Admin</title>
<style>{_CSS}</style></head><body>
<div class="navbar"><h1>Grid Manual Bot Admin</h1><span style="color:var(--text2);font-size:14px">Management Console</span></div>
<div class="main">
  <div id="notice-banner" class="notice-banner">
    <span class="notice-text" id="notice-text"></span>
    <button class="notice-close" onclick="dismissNotice()">&times;</button>
  </div>

  <div class="notice-admin">
    <h3>Broadcast Notification</h3>
    <div class="notice-input-row">
      <input id="notice-input" placeholder="e.g. System updated, please Stop then Start your bot">
      <button class="btn btn-notify" onclick="sendNotice()">Send</button>
      <button class="btn btn-clear-notice" onclick="clearNotice()">Clear</button>
    </div>
  </div>

  <div class="card" style="display:flex;align-items:center;justify-content:space-between">
    <div>
      <h2 style="margin-bottom:4px">Deploy Update</h2>
      <div style="color:var(--text2);font-size:13px">Stop all running bots, reload new code, then start them back. State file preserved (direction, lots, PnL). Dashboard auto-reconnects within seconds.</div>
    </div>
    <button class="btn" id="btn-restart-all" onclick="restartAll()" style="background:var(--yellow);color:#000;font-weight:700;white-space:nowrap;margin-left:16px;padding:10px 24px;font-size:15px">Restart All</button>
  </div>
  <div id="restart-result" style="display:none;margin-bottom:16px"></div>

  <div id="users"></div>

  <div class="card">
    <h2>Create User Slot</h2>
    <div class="form-row">
      <div class="form-group"><label>User ID</label><input id="f_uid" placeholder="e.g. kevin"></div>
      <div class="form-group"><label>Source IP (EC2 Private IP)</label><input id="f_sip" placeholder="e.g. 10.0.1.11"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label>Public IP (Elastic IP)</label><input id="f_pip" placeholder="e.g. 52.198.23.140"></div>
      <div class="form-group"><label>Dashboard Port</label><input id="f_port" placeholder="5560" value="5560"></div>
    </div>
    <button class="btn btn-start" onclick="createSlot()">Create Slot</button>
    <div id="msg"></div>
  </div>
</div>

<script>
const API = '';
const HOST = location.origin;

async function load() {{
  const resp = await fetch(API + '/api/users');
  const users = await resp.json();
  const el = document.getElementById('users');
  if (!users.length) {{ el.innerHTML = '<div class="card" style="text-align:center;color:var(--muted)">No user slots yet</div>'; return; }}
  el.innerHTML = users.map(u => {{
    let badge = '';
    if (u.running) badge = `<span class="badge running">Running (PID ${{u.pid}})</span>`;
    else badge = '<span class="badge stopped">Stopped</span>';

    return `
    <div class="card">
      <div class="card-header">
        <span class="card-title">${{u.user_id}}</span>
        ${{badge}}
      </div>
      <div class="info-row">Source IP: <span>${{u.source_ip || '\\u2014'}}</span> &nbsp; Port: <span>${{u.port}}</span></div>
      <div class="info-row">Invite Code: <span style="font-family:monospace;font-size:16px;font-weight:700;color:var(--yellow);letter-spacing:2px">${{u.invite_code || '\\u2014'}}</span></div>
      <div class="info-row">Dashboard Link:</div>
      <div class="url-box">${{HOST}}/dashboard/</div>
      <div style="font-size:12px;color:var(--muted);margin-top:4px">Share link + invite code with user. They enter the code once to access their dashboard.</div>
      <div class="btn-group">
        <button class="btn btn-start" onclick="startBot('${{u.user_id}}')" ${{u.running ? 'disabled' : ''}}>Start</button>
        <button class="btn btn-stop" onclick="stopBot('${{u.user_id}}')" ${{!u.running ? 'disabled' : ''}}>Stop</button>
        <button class="btn btn-dash" onclick="window.open('/dashboard/${{u.user_id}}','_blank')" ${{!u.running ? 'disabled' : ''}}>Dashboard</button>
        <button class="btn btn-del" onclick="delUser('${{u.user_id}}')" ${{u.running ? 'disabled' : ''}}>Delete</button>
      </div>
    </div>`;
  }}).join('');
}}

async function createSlot() {{
  const body = {{
    user_id: document.getElementById('f_uid').value,
    source_ip: document.getElementById('f_sip').value,
    public_ip: document.getElementById('f_pip').value,
    port: parseInt(document.getElementById('f_port').value) || 5560,
  }};
  const msg = document.getElementById('msg');
  try {{
    const r = await fetch(API + '/api/admin/slots', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify(body)}});
    if (!r.ok) {{ const e = await r.json(); throw new Error(e.detail); }}
    msg.textContent = 'Slot created! Share the user link with your friend.'; msg.className = 'ok';
    load();
  }} catch(e) {{ msg.textContent = e.message; msg.className = 'err'; }}
}}

async function startBot(uid) {{
  try {{
    const r = await fetch(API + `/api/users/${{uid}}/start`, {{method:'POST'}});
    const data = await r.json();
    if (!r.ok) {{ alert('Start failed: ' + (data.detail || JSON.stringify(data))); return; }}
    alert('Bot started! PID: ' + data.pid);
  }} catch(e) {{ alert('Start error: ' + e.message); }}
  setTimeout(load, 1500);
}}

async function stopBot(uid) {{
  try {{
    const r = await fetch(API + `/api/users/${{uid}}/stop`, {{method:'POST'}});
    const data = await r.json();
    if (!r.ok) {{ alert('Stop failed: ' + (data.detail || JSON.stringify(data))); return; }}
    alert('Bot stopped');
  }} catch(e) {{ alert('Stop error: ' + e.message); }}
  setTimeout(load, 500);
}}

async function delUser(uid) {{
  if (!confirm(`Delete user ${{uid}} and all data?`)) return;
  await fetch(API + `/api/admin/users/${{uid}}`, {{method:'DELETE'}});
  load();
}}

// --- Restart All ---
async function restartAll() {{
  if (!confirm('Restart all running bots? They will be stopped then started with new code.')) return;
  const btn = document.getElementById('btn-restart-all');
  const resEl = document.getElementById('restart-result');
  btn.disabled = true; btn.textContent = 'Restarting...';
  resEl.style.display = 'none';
  try {{
    const r = await fetch(API + '/api/admin/restart-all', {{method:'POST'}});
    const data = await r.json();
    let html = `<div class="card" style="border-color:var(--yellow)"><h2 style="color:var(--yellow)">${{data.message}}</h2>`;
    if (data.results && data.results.length) {{
      html += '<div style="margin-top:8px">';
      data.results.forEach(r => {{
        const ok = r.start === 'ok';
        html += `<div style="font-size:14px;margin:4px 0;color:${{ok ? 'var(--green)' : 'var(--red)'}}">` +
          `${{r.user_id}}: stop=${{r.stop}}, start=${{r.start}}${{r.pid ? ' (PID '+r.pid+')' : ''}}</div>`;
      }});
      html += '</div>';
    }}
    html += '</div>';
    resEl.innerHTML = html; resEl.style.display = 'block';
    setTimeout(() => {{ resEl.style.display = 'none'; }}, 15000);
  }} catch(e) {{ alert('Restart error: ' + e.message); }}
  btn.disabled = false; btn.textContent = 'Restart All';
  setTimeout(load, 2000);
}}

// --- Notification ---
async function loadNotice() {{
  const r = await fetch(API + '/api/notice');
  const data = await r.json();
  const banner = document.getElementById('notice-banner');
  const text = document.getElementById('notice-text');
  const dismissed = localStorage.getItem('notice_dismissed');
  if (data.message && data.message !== dismissed) {{
    text.textContent = data.message;
    banner.classList.add('show');
  }} else if (!data.message) {{
    banner.classList.remove('show');
    localStorage.removeItem('notice_dismissed');
  }}
}}

function dismissNotice() {{
  const text = document.getElementById('notice-text').textContent;
  localStorage.setItem('notice_dismissed', text);
  document.getElementById('notice-banner').classList.remove('show');
}}

async function sendNotice() {{
  const msg = document.getElementById('notice-input').value.trim();
  if (!msg) {{ alert('Please enter a message'); return; }}
  await fetch(API + '/api/notice', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify({{message:msg}})}});
  document.getElementById('notice-input').value = '';
  localStorage.removeItem('notice_dismissed');
  loadNotice();
}}

async function clearNotice() {{
  await fetch(API + '/api/notice', {{method:'DELETE'}});
  localStorage.removeItem('notice_dismissed');
  loadNotice();
}}

load();
loadNotice();
setInterval(load, 5000);
setInterval(loadNotice, 5000);
</script>
</body></html>"""



@app.get("/", response_class=HTMLResponse)
async def index():
    return ADMIN_HTML


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
