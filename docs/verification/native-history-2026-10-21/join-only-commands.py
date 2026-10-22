import base64
from datetime import datetime, timezone
import hashlib
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

binary = Path(sys.argv[1]).resolve()
root = Path(sys.argv[2]).resolve()
root.mkdir(parents=True, exist_ok=False)
workspace = root / "workspace"
workspace.mkdir()
env = dict(os.environ)
for key, relative in {"HOME": "home", "XDG_CONFIG_HOME": "home/.config", "XDG_DATA_HOME": "data", "XDG_STATE_HOME": "state", "XDG_CACHE_HOME": "cache", "TMPDIR": "tmp", "BUN_TMPDIR": "tmp"}.items():
    target = root / relative
    target.mkdir(parents=True, exist_ok=True)
    env[key] = str(target)
env.update(OPENCODE_DB=str(root / "session.sqlite"), OPENCODE_SERVER_PASSWORD="isolated-join-only-probe", OPENCODE_DISABLE_MODELS_FETCH="1", OPENCODE_DISABLE_AUTOUPDATE="1", OPENCODE_PURE="1", OPENCODE_LOG_LEVEL="INFO", BUN_JSC_useDFGJIT="false", BUN_JSC_useFTLJIT="false")
for name in ["BUN_OPTIONS", "OPENCODE_CONFIG", "OPENCODE_CONFIG_DIR", "OPENCODE_CONFIG_CONTENT"]:
    env.pop(name, None)
subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
(workspace / "fixture.txt").write_text("join-only release fixture\n")
subprocess.run(["git", "-C", str(workspace), "add", "fixture.txt"], check=True)
subprocess.run(["git", "-C", str(workspace), "-c", "user.name=Brent Yates", "-c", "user.email=brent.yates@swxtch.io", "commit", "--quiet", "-m", "fixture"], check=True)
report = {"date": datetime.now(timezone.utc).isoformat(), "executable": str(binary), "database": env["OPENCODE_DB"], "architecture": os.uname().machine, "runtime_environment": {name: env[name] for name in ["BUN_JSC_useDFGJIT", "BUN_JSC_useFTLJIT"]}, "requests": [], "provider_requests": [], "checks": []}
authorization = "Basic " + base64.b64encode(b"opencode:isolated-join-only-probe").decode()
started = threading.Event()
release = threading.Event()
lock = threading.Lock()
server = None
url = ""

def save():
    (root / "evidence.json").write_text(json.dumps(report, indent=2) + "\n")

def check(name, condition):
    report["checks"].append({"name": name, "passed": bool(condition)})
    save()
    if not condition:
        raise RuntimeError(name)

class Provider(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if body.get("tools") and "case:crash-control" in json.dumps(body):
            with lock:
                report["provider_requests"].append({"path": self.path, "body": body})
            started.set()
            release.wait(600)
        chunks = [{"id": "join-only-fixture", "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": delta, **({"finish_reason": "stop"} if delta == {} else {})}]} for delta in [{"role": "assistant"}, {"content": "fixture response"}, {}]]
        payload = ("".join("data: " + json.dumps(chunk) + "\n\n" for chunk in chunks) + "data: [DONE]\n\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

provider = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Provider)
provider.daemon_threads = True
provider_thread = threading.Thread(target=provider.serve_forever, daemon=True)
provider_thread.start()
configuration = {"providers": {"test": {"name": "Join-only fixture", "api": {"type": "aisdk", "package": "@ai-sdk/openai-compatible", "url": f"http://127.0.0.1:{provider.server_port}/v1", "settings": {}}, "request": {"body": {"apiKey": "fixture"}}, "models": {"test-model": {"name": "Fixture", "api": {"id": "test-model"}, "capabilities": {"tools": True, "input": ["text"], "output": ["text"]}, "limit": {"context": 100000, "output": 10000}, "cost": {"input": 0, "output": 0}}}}}}
config = Path(env["XDG_CONFIG_HOME"]) / "opencode"
config.mkdir(parents=True)
(config / "opencode.jsonc").write_text(json.dumps(configuration))
(config / "opencode.json").write_text(json.dumps({"permission": {"bash": "allow", "read": "allow"}}))

def api(route, method="GET", body=None, budget=180):
    request = urllib.request.Request(url + route, method=method, data=None if body is None else json.dumps(body).encode(), headers={"Authorization": authorization, "x-opencode-directory": str(workspace), "Content-Type": "application/json"})
    try:
        response = urllib.request.urlopen(request, timeout=budget)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        text = response.read().decode()
        result = {"status": response.status, "body": json.loads(text) if text else None, "text": text}
    report["requests"].append({"route": route, "method": method, "request": body, "response": result})
    return result

def data(route, method="GET", body=None):
    response = api(route, method, body)
    if response["status"] != 200:
        raise RuntimeError(json.dumps(response))
    return response["body"]["data"]

def start_server():
    global server, url
    with socket.socket() as selected:
        selected.bind(("127.0.0.1", 0))
        port = selected.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    ordinal = len(report.get("servers", []))
    logfile = root / f"server-{ordinal}.log"
    output = logfile.open("w")
    command = [str(binary), "serve", "--pure", "--print-logs", "--hostname", "127.0.0.1", "--port", str(port)]
    server = subprocess.Popen(command, cwd=workspace, env=env, stdout=output, stderr=output, start_new_session=True)
    report.setdefault("servers", []).append({"pid": server.pid, "command": command, "database": env["OPENCODE_DB"]})
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise RuntimeError(logfile.read_text())
        try:
            catalog = data("/api/model")
            if any(model["id"] == "test-model" and model["providerID"] == "test" for model in catalog):
                return
        except (OSError, TimeoutError):
            pass
        time.sleep(0.1)
    raise TimeoutError("configured native server readiness")

def stop_server(crash=False):
    global server
    if server is None:
        return
    if server.poll() is None:
        os.killpg(server.pid, signal.SIGKILL if crash else signal.SIGTERM)
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(server.pid, signal.SIGKILL)
            server.wait(timeout=10)
    report["servers"][-1]["reaped_exit"] = server.returncode
    server = None

def rows(session):
    with sqlite3.connect("file:" + env["OPENCODE_DB"] + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        return {name: [dict(row) for row in db.execute(query, (session,))] for name, query in {"session_input": "SELECT * FROM session_input WHERE session_id=? ORDER BY admitted_seq", "session_message": "SELECT * FROM session_message WHERE session_id=? ORDER BY seq", "event": "SELECT * FROM event WHERE aggregate_id=? ORDER BY seq"}.items()}

def create():
    return data("/api/session", "POST", {"model": {"id": "test-model", "providerID": "test"}, "location": {"directory": str(workspace)}})["id"]

try:
    version = subprocess.run([str(binary), "--version"], cwd=workspace, env=env, capture_output=True, text=True, timeout=180)
    report["version"] = {"exit": version.returncode, "stdout": version.stdout, "stderr": version.stderr}
    report["digests"] = {str(file): hashlib.sha256(file.read_bytes()).hexdigest() for file in binary.parent.iterdir() if file.is_file()}
    check("exact published version", version.returncode == 0 and version.stdout.strip() == "1.18.32-swxtch.d1b232e9")
    start_server()
    ambiguous = create()
    admission = data(f"/api/session/{ambiguous}/prompt", "POST", {"id": "msg_crash_control_20261021", "prompt": {"text": "case:crash-control hold provider before any response"}, "delivery": "steer"})
    check("real provider request is positively held", started.wait(180) and not release.is_set())
    before_active = data("/api/session/active")
    before_user = data(f"/api/session/{ambiguous}/message/{admission['id']}")
    check("admission was promoted into active work", ambiguous in before_active and before_user["id"] == admission["id"] and before_user["type"] == "user")
    pending = create()
    pending_admission = data(f"/api/session/{pending}/prompt", "POST", {"id": "msg_join_pending_20261021", "prompt": {"text": "admit-only post-crash control"}, "delivery": "steer", "resume": False})
    stop_server(crash=True)
    check("owned release process was actually crash-reaped", report["servers"][-1]["reaped_exit"] == -signal.SIGKILL)
    stopped_rows = {session: rows(session) for session in [ambiguous, pending]}
    check("ambiguous work is promoted but not terminal", stopped_rows[ambiguous]["session_input"][0]["promoted_seq"] is not None and not any(json.loads(row["data"]).get("time", {}).get("completed") is not None for row in stopped_rows[ambiguous]["session_message"] if row["type"] == "assistant"))
    physical = os.stat(env["OPENCODE_DB"])
    report["physical_before"] = {"device": physical.st_dev, "inode": physical.st_ino}
    report["ambiguous"] = {"sessionID": ambiguous, "admission": admission, "active_before_crash": before_active, "user_before_crash": before_user}
    report["pending"] = {"sessionID": pending, "admission": pending_admission}
    report["stopped_rows"] = stopped_rows
    save()
    start_server()
    after_active = data("/api/session/active")
    native_session = data(f"/api/session/{ambiguous}")
    native_user = data(f"/api/session/{ambiguous}/message/{admission['id']}")
    unavailable = api(f"/api/session/{ambiguous}/wait", "POST")
    report["ambiguous"]["wait_after_restart"] = unavailable
    check("wait reports unavailable for ambiguous post-crash work", unavailable["status"] == 503 and unavailable["body"].get("service") == "session.wait" and unavailable["body"].get("_tag") == "ServiceUnavailableError")
    pending_wait = api(f"/api/session/{pending}/wait", "POST")
    report["pending"]["wait_after_restart"] = pending_wait
    check("admit-only input stays pending after restart wait", pending_wait["status"] == 200 and pending_wait["body"]["data"] == {"type": "pending", "admittedSeq": pending_admission["admittedSeq"], "messageID": pending_admission["id"]})
    final_active = data("/api/session/active")
    after_rows = {session: rows(session) for session in [ambiguous, pending]}
    report["after_rows"] = after_rows
    report["active_after_restart"] = after_active
    report["active_after_waits"] = final_active
    report["provider_request_count_after_waits"] = len(report["provider_requests"])
    physical_after = os.stat(env["OPENCODE_DB"])
    report["physical_after"] = {"device": physical_after.st_dev, "inode": physical_after.st_ino}
    check("same physical native database and Session/message identity", report["physical_before"] == report["physical_after"] and native_session["id"] == ambiguous and native_user["id"] == admission["id"])
    check("wait started no local drain or provider redispatch", after_active == {} and final_active == {} and len(report["provider_requests"]) == 1)
    check("wait promoted no input and changed no native history", stopped_rows == after_rows)
    report["verdict"] = "passed"
except BaseException as error:
    report["verdict"] = "failed"
    report["failure"] = {"type": type(error).__name__, "message": str(error)}
    raise
finally:
    stop_server()
    release.set()
    provider.shutdown()
    provider_thread.join(timeout=5)
    save()
    print(json.dumps({"verdict": report.get("verdict"), "failure": report.get("failure"), "checks": report["checks"], "evidence": str(root / "evidence.json")}, indent=2))
