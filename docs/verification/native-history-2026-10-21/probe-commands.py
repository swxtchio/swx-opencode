import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import http.server
import json
import os
from pathlib import Path
import re
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
env = dict(os.environ)
for key, name in {"HOME": "home", "XDG_CONFIG_HOME": "home/.config", "XDG_DATA_HOME": "data", "XDG_STATE_HOME": "state", "XDG_CACHE_HOME": "cache", "TMPDIR": "tmp", "BUN_TMPDIR": "tmp"}.items():
    target = root / name
    target.mkdir(parents=True, exist_ok=True)
    env[key] = str(target)
workspace.mkdir()
subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
(workspace / "needle-target.txt").write_text("native release fixture\n")
subprocess.run(["git", "-C", str(workspace), "add", "needle-target.txt"], check=True)
subprocess.run(["git", "-C", str(workspace), "-c", "user.name=Brent Yates", "-c", "user.email=brent.yates@swxtch.io", "commit", "--quiet", "-m", "fixture"], check=True)
env.update(OPENCODE_DB=str(root / "session.sqlite"), OPENCODE_SERVER_PASSWORD="isolated-native-probe", OPENCODE_DISABLE_MODELS_FETCH="1", OPENCODE_DISABLE_AUTOUPDATE="1", OPENCODE_PURE="1", OPENCODE_LOG_LEVEL="INFO")
env["BUN_JSC_useDFGJIT"] = "false"
env["BUN_JSC_useFTLJIT"] = "false"
for key in ["BUN_OPTIONS", "OPENCODE_DISABLE_FFF", "OPENCODE_CONFIG", "OPENCODE_CONFIG_CONTENT", "OPENCODE_CONFIG_DIR"]:
    env.pop(key, None)
authorization = "Basic " + base64.b64encode(b"opencode:isolated-native-probe").decode()
report = {"date": datetime.now(timezone.utc).isoformat(), "executable": str(binary), "host_arch": os.uname().machine, "database": env["OPENCODE_DB"], "runtime_environment": {key: env[key] for key in ["BUN_JSC_useDFGJIT", "BUN_JSC_useFTLJIT"]}, "requests": [], "provider_requests": [], "checks": []}
lock = threading.Lock()
counts = {}
holds = {name: threading.Event() for name in ["initial", "completed", "failed"]}
provider_signals = {name: threading.Event() for name in ["initial", "completed", "failed", "interrupted"]}
pool = ThreadPoolExecutor(max_workers=4)
server = None
server_logs = []
url = ""

def save():
    (root / "evidence.json").write_text(json.dumps(report, indent=2) + "\n")

def check(name, condition):
    report["checks"].append({"name": name, "passed": bool(condition)})
    save()
    if not condition:
        raise RuntimeError(name)

def until(name, read, budget=180):
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        value = read()
        if value:
            return value
        time.sleep(0.1)
    raise TimeoutError(name)

def stream(text=None, tool=None):
    def chunk(delta, finish=None):
        return {"id": "chatcmpl-native-fixture", "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": delta, **({"finish_reason": finish} if finish else {})}]}
    parts = [chunk({"role": "assistant"})]
    if text is not None:
        parts.extend([chunk({"content": text}), chunk({}, "stop")])
    if tool is not None:
        name, arguments, call_id = tool
        parts.extend([chunk({"tool_calls": [{"index": 0, "id": call_id, "type": "function", "function": {"name": name, "arguments": ""}}]}), chunk({"tool_calls": [{"index": 0, "function": {"arguments": json.dumps(arguments)}}]}), chunk({}, "tool_calls")])
    return ("".join("data: " + json.dumps(part) + "\n\n" for part in parts) + "data: [DONE]\n\n").encode()

class Provider(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        messages = body.get("messages", [])
        serialized = json.dumps(body)
        if "Generate a title for this conversation" in serialized:
            result = stream(text="Native history verification")
        else:
            users = [json.dumps(message) for message in messages if message.get("role") == "user"]
            latest = users[-1] if users else serialized
            scenario = next((name for name in ["completed", "failed", "interrupted", "second"] if "case:" + name in latest), "other")
            with lock:
                ordinal = counts.get(scenario, 0) + 1
                counts[scenario] = ordinal
                report["provider_requests"].append({"scenario": scenario, "ordinal": ordinal, "body": body})
            if scenario == "completed" and ordinal == 1:
                provider_signals["initial"].set()
                holds["initial"].wait(300)
            if scenario in {"completed", "failed", "interrupted"} and ordinal == 1:
                name = "not_registered" if scenario == "failed" else "bash"
                arguments = {} if scenario == "failed" else {"command": f'while [ ! -f "{workspace / (scenario + "-release")}" ]; do sleep 0.05; done; printf "{scenario} released\\n"', "timeout": 120000}
                result = stream(tool=(name, arguments, "call-" + scenario))
                if scenario == "interrupted":
                    provider_signals["interrupted"].set()
            elif scenario in {"completed", "failed"} and ordinal == 2:
                provider_signals[scenario].set()
                holds[scenario].wait(300)
                result = stream(text=scenario + " terminal continuation")
            else:
                result = stream(text="second native response" if scenario == "second" else "native terminal response")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(result)))
        self.end_headers()
        try:
            self.wfile.write(result)
        except (BrokenPipeError, ConnectionResetError):
            pass

provider = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Provider)
provider.daemon_threads = True
provider_thread = threading.Thread(target=provider.serve_forever, daemon=True)
provider_thread.start()
configuration = {"providers": {"test": {"name": "Native fixture", "api": {"type": "aisdk", "package": "@ai-sdk/openai-compatible", "url": f"http://127.0.0.1:{provider.server_port}/v1", "settings": {}}, "request": {"body": {"apiKey": "fixture"}}, "models": {"test-model": {"name": "Native fixture", "api": {"id": "test-model"}, "capabilities": {"tools": True, "input": ["text"], "output": ["text"]}, "limit": {"context": 100000, "output": 10000}, "cost": {"input": 0, "output": 0}}}}}}
config_dir = Path(env["XDG_CONFIG_HOME"]) / "opencode"
config_dir.mkdir(parents=True)
(config_dir / "opencode.jsonc").write_text(json.dumps(configuration))
(config_dir / "opencode.json").write_text(json.dumps({"permission": {"bash": "allow", "read": "allow"}}))

def api(route, method="GET", body=None, budget=60):
    request = urllib.request.Request(url + route, method=method, data=None if body is None else json.dumps(body).encode(), headers={"Authorization": authorization, "x-opencode-directory": str(workspace), "Content-Type": "application/json"})
    try:
        response = urllib.request.urlopen(request, timeout=budget)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read().decode()
        result = {"status": response.status, "text": raw, "body": json.loads(raw) if raw else None}
    with lock:
        report["requests"].append({"route": route, "method": method, "request": body, "response": result})
    return result

def data(route, method="GET", body=None):
    response = api(route, method, body)
    if response["status"] != 200:
        raise RuntimeError(json.dumps(response))
    return response["body"]["data"]

def messages(session):
    return data(f"/api/session/{session}/message?limit=200&order=asc")

def tool(session, name):
    return next((part for message in messages(session) if message["type"] == "assistant" for part in message.get("content", []) if part["type"] == "tool" and part.get("name") == name), None)

def create():
    return data("/api/session", "POST", {"model": {"id": "test-model", "providerID": "test"}, "location": {"directory": str(workspace)}})["id"]

def admit(session, identifier, text, resume=True):
    return data(f"/api/session/{session}/prompt", "POST", {"id": identifier, "prompt": {"text": text}, "delivery": "steer", "resume": resume})

def binary_command(args):
    completed = subprocess.run([str(binary), *args], cwd=workspace, env=env, capture_output=True, text=True, timeout=180)
    return {"args": args, "exit": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr}

def start_server():
    global server, url
    with socket.socket() as selected:
        selected.bind(("127.0.0.1", 0))
        port = selected.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    logfile = root / f"server-{len(server_logs)}.log"
    server_logs.append(logfile)
    handle = logfile.open("w")
    command = [str(binary), "serve", "--pure", "--print-logs", "--hostname", "127.0.0.1", "--port", str(port)]
    server = subprocess.Popen(command, cwd=workspace, env=env, stdout=handle, stderr=handle, start_new_session=True)
    report.setdefault("server_processes", []).append({"pid": server.pid, "command": command, "environment": {key: env[key] for key in ["HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME", "TMPDIR", "OPENCODE_DB"]}})
    def ready():
        if server.poll() is not None:
            raise RuntimeError("server exited during startup: " + logfile.read_text())
        try:
            catalog = api("/api/model", budget=180)
            return catalog["status"] == 200 and any(model["id"] == "test-model" and model["providerID"] == "test" for model in catalog["body"]["data"])
        except (OSError, TimeoutError):
            return False
    until("configured release server readiness", ready, 900)
    save()

def stop_server():
    global server
    if server is None:
        return
    if server.poll() is None:
        os.killpg(server.pid, signal.SIGTERM)
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(server.pid, signal.SIGKILL)
            server.wait(timeout=10)
    report["server_processes"][-1]["reaped_exit"] = server.returncode
    server = None

def registered(session, admission, future, phase):
    def record():
        logfile = root / "data/opencode/log/opencode.log"
        logs = logfile.read_text() if logfile.exists() else ""
        return next((line for line in logs.splitlines() if "event=session.wait.registered" in line and f"session_id={session}" in line and f"message_id={admission['id']}" in line and f"admitted_seq={admission['admittedSeq']}" in line), None)
    line = until("producer wait registration " + phase, record)
    wait_id = re.search(r"\bwait_id=(\S+)", line).group(1)
    active = data("/api/session/active")
    readback = data(f"/api/session/{session}/message/{admission['id']}")
    held = {"phase": phase, "registration": line, "waitID": wait_id, "active": active, "user": readback, "wait_unsettled": not future.done()}
    report.setdefault("held_phases", []).append(held)
    check("registered wait stays unresolved during " + phase, session in active and readback["id"] == admission["id"] and readback["type"] == "user" and not future.done())
    return wait_id

def page_history(session, limit=3, after=None):
    pages = []
    events = []
    while True:
        route = f"/api/session/{session}/history?limit={limit}" + ("" if after is None else f"&after={after}")
        response = api(route)
        if response["status"] != 200:
            raise RuntimeError(json.dumps(response))
        page = response["body"]
        pages.append({"after": after, "page": page})
        for event in page["data"]:
            if after is not None and event["durable"]["seq"] <= after:
                raise RuntimeError("nonexclusive native event page")
            after = event["durable"]["seq"]
            events.append(event)
        if not page["hasMore"]:
            return {"pages": pages, "events": events}
        if not page["data"] or len(pages) > 100:
            raise RuntimeError("native history page made no bounded progress")

def event_stream(session, after=None):
    route = f"/api/session/{session}/event" + ("" if after is None else f"?after={after}")
    response = urllib.request.urlopen(urllib.request.Request(url + route, headers={"Authorization": authorization}), timeout=90)
    return response

def next_event(response):
    values = []
    while True:
        line = response.readline().decode().rstrip("\r\n")
        if not line and values:
            return json.loads("\n".join(values))
        if line.startswith("data:"):
            values.append(line[5:].lstrip())
        if line == "" and response.isclosed():
            raise RuntimeError("SSE ended before required native event")

def native_rows(session):
    with sqlite3.connect("file:" + env["OPENCODE_DB"] + "?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        return {name: [dict(row) for row in connection.execute(query, (session,))] for name, query in {
            "session_input": "SELECT * FROM session_input WHERE session_id=? ORDER BY admitted_seq",
            "session_message": "SELECT * FROM session_message WHERE session_id=? ORDER BY seq",
            "event": "SELECT * FROM event WHERE aggregate_id=? ORDER BY seq",
        }.items()}

try:
    report["version"] = binary_command(["--version"])
    check("published executable version", report["version"]["exit"] == 0 and report["version"]["stdout"].strip() == "1.18.32-swxtch.d1b232e9")
    report["digests"] = {str(file): hashlib.sha256(file.read_bytes()).hexdigest() for file in binary.parent.iterdir() if file.is_file()}
    report["db_path"] = binary_command(["db", "path"])
    check("absolute native store selector", report["db_path"]["stdout"].strip() == env["OPENCODE_DB"])
    report["fff_observation_location"] = str(root.parent / "native-guest-run-3/evidence.json")
    start_server()
    idle = create()
    report["idle"] = {"sessionID": idle, "wait": api(f"/api/session/{idle}/wait", "POST")}
    check("fresh idle control", report["idle"]["wait"]["body"]["data"]["type"] == "idle")
    pending = create()
    pending_admission = admit(pending, "msg_pending_native_20261021", "admit-only native control", False)
    report["pending"] = {"sessionID": pending, "admission": pending_admission, "wait": api(f"/api/session/{pending}/wait", "POST")}
    pending_wait = report["pending"]["wait"]["body"]["data"]
    check("admit-only pending is not completed", pending_wait["type"] == "pending" and pending_wait["messageID"] == pending_admission["id"] and pending_wait["admittedSeq"] == pending_admission["admittedSeq"])
    completed = create()
    text = "case:completed native sentinel; run gated bash, then final continuation"
    admission = admit(completed, "msg_zzzz_native_20261021", text)
    check("initial provider positively held", provider_signals["initial"].wait(180))
    waiter = pool.submit(api, f"/api/session/{completed}/wait", "POST", None, 300)
    first_wait = registered(completed, admission, waiter, "initial provider")
    holds["initial"].set()
    until("native local bash running", lambda: (part if (part := tool(completed, "bash")) and part["state"]["status"] == "running" else None))
    check("same registered waiter while local tool held", registered(completed, admission, waiter, "local bash") == first_wait)
    live = event_stream(completed)
    prefix = []
    (workspace / "completed-release").write_text("release")
    check("final continuation positively held", provider_signals["completed"].wait(180))
    while True:
        event = next_event(live)
        prefix.append(event)
        if event["type"] == "session.next.tool.success":
            break
    live.close()
    cursor = prefix[-1]["durable"]["seq"]
    check("same registered waiter while required continuation held", registered(completed, admission, waiter, "final provider continuation") == first_wait)
    check("native tool settled before continuation completion", tool(completed, "bash")["state"]["status"] == "completed")
    holds["completed"].set()
    outcome_response = waiter.result(timeout=180)
    outcome = outcome_response["body"]["data"]
    check("completed wait covers original admission", outcome_response["status"] == 200 and outcome["type"] == "completed" and outcome["admittedSeq"] == admission["admittedSeq"])
    terminal = data(f"/api/session/{completed}/message/{outcome['assistantMessageID']}")
    check("terminal native assistant is readable and successful", terminal["type"] == "assistant" and terminal["time"].get("completed") is not None and terminal.get("error") is None and any(part.get("text") == "completed terminal continuation" for part in terminal["content"]))
    first_history = page_history(completed)
    suffix = [event for event in first_history["events"] if event["durable"]["seq"] > cursor]
    reconnect = event_stream(completed, cursor)
    replay = [next_event(reconnect) for _ in suffix]
    reconnect.close()
    check("reconnect equals exclusive finite history suffix", replay == suffix and prefix + replay == first_history["events"])
    before_retry = native_rows(completed)
    retry = admit(completed, admission["id"], text)
    after_retry = native_rows(completed)
    check("exact top-level identity retry retains admission", all(retry[key] == admission[key] for key in ["id", "sessionID", "admittedSeq", "delivery"]))
    check("exact retry creates no duplicate native user turn", before_retry["session_input"] == after_retry["session_input"] and before_retry["session_message"] == after_retry["session_message"] and sum(row["type"] == "user" for row in after_retry["session_message"]) == 1)
    second = admit(completed, "msg_0000_native_20261021", "case:second sentinel; preserve native order")
    second_wait = data(f"/api/session/{completed}/wait", "POST")
    check("second native turn completed", second_wait["type"] == "completed" and second_wait["admittedSeq"] == second["admittedSeq"])
    report["completed"] = {"sessionID": completed, "admission": admission, "waitID": first_wait, "wait": outcome_response, "terminal": terminal, "retry": retry, "rows_before_retry": before_retry, "rows_after_retry": after_retry, "second_admission": second, "second_wait": second_wait, "disconnect_cursor": cursor, "stream_prefix": prefix, "stream_replay": replay, "first_history": first_history}
    for scenario in ["failed", "interrupted"]:
        session = create()
        accepted = admit(session, "msg_" + scenario + "_native_20261021", "case:" + scenario + " exercise native terminal outcome")
        if scenario == "failed":
            check("failed-tool continuation positively held", provider_signals["failed"].wait(180))
            until("native failed tool recorded", lambda: (part if (part := tool(session, "not_registered")) and part["state"]["status"] == "error" else None))
        else:
            check("interruptible provider started", provider_signals["interrupted"].wait(180))
            until("native interruptible tool running", lambda: (part if (part := tool(session, "bash")) and part["state"]["status"] == "running" else None))
        waiting = pool.submit(api, f"/api/session/{session}/wait", "POST", None, 300)
        wait_id = registered(session, accepted, waiting, scenario)
        interrupt = None
        if scenario == "failed":
            holds["failed"].set()
        else:
            interrupt = api(f"/api/session/{session}/interrupt", "POST")
            check("native active interrupt acknowledged", interrupt["status"] == 204)
        response = waiting.result(timeout=180)
        result = response["body"]["data"]
        check(scenario + " wait is truthful", response["status"] == 200 and result["type"] == scenario and result["admittedSeq"] == accepted["admittedSeq"])
        report[scenario] = {"sessionID": session, "admission": accepted, "waitID": wait_id, "wait": response, "interrupt": interrupt, "messages": messages(session), "history": page_history(session), "rows": native_rows(session)}
        check(scenario + " terminal tool fact persisted", tool(session, "not_registered" if scenario == "failed" else "bash")["state"]["status"] == "error")
    all_sessions = [completed, report["failed"]["sessionID"], report["interrupted"]["sessionID"], idle, pending]
    before = {session: {"session": data(f"/api/session/{session}"), "messages": messages(session), "history": page_history(session), "rows": native_rows(session)} for session in all_sessions}
    physical = os.stat(env["OPENCODE_DB"])
    report["physical_store_before"] = {"device": physical.st_dev, "inode": physical.st_ino, "size": physical.st_size}
    check("probe writes selected physical native store", not (root / "data/opencode/opencode.db").exists() and before[completed]["rows"]["session_input"][0]["id"] == admission["id"])
    stop_server()
    start_server()
    after = {session: {"session": data(f"/api/session/{session}"), "messages": messages(session), "history": page_history(session), "rows": native_rows(session)} for session in all_sessions}
    physical_after = os.stat(env["OPENCODE_DB"])
    check("same physical database after server restart", physical.st_dev == physical_after.st_dev and physical.st_ino == physical_after.st_ino)
    for session in all_sessions:
        check("native identity messages and full history survive restart " + session, before[session]["session"]["id"] == after[session]["session"]["id"] and before[session]["messages"] == after[session]["messages"] and before[session]["history"]["events"] == after[session]["history"]["events"])
        events = after[session]["history"]["events"]
        sequences = [event["durable"]["seq"] for event in events]
        check("strict unique durable order " + session, sequences == sorted(set(sequences)) and len({event["id"] for event in events}) == len(events))
        rows_by_id = {row["id"]: row for row in after[session]["rows"]["event"]}
        check("public history matches producer native rows " + session, all(event["id"] in rows_by_id and rows_by_id[event["id"]]["seq"] == event["durable"]["seq"] for event in events))
    check("finite history crossed actual page boundaries", len(after[completed]["history"]["pages"]) > 1 and after[completed]["history"]["pages"][0]["page"]["hasMore"])
    full = after[completed]["history"]["events"]
    saved_suffix = page_history(completed, after=cursor)
    exhausted = page_history(completed, after=full[-1]["durable"]["seq"])
    restart_stream = event_stream(completed, cursor)
    restart_replay = [next_event(restart_stream) for _ in saved_suffix["events"]]
    restart_stream.close()
    check("saved cursor excludes anchor after restart", saved_suffix["events"] == [event for event in full if event["durable"]["seq"] > cursor])
    check("exhausted native cursor", exhausted["events"] == [] and exhausted["pages"][-1]["page"]["hasMore"] is False)
    check("restart stream replay equals finite suffix", restart_replay == saved_suffix["events"])
    users = [row["id"] for row in after[completed]["rows"]["session_message"] if row["type"] == "user"]
    check("native durable message ordering is not lexical IDs", users == [admission["id"], second["id"]] and users != sorted(users))
    check("V2 ascending message API follows native durable order", [message["id"] for message in after[completed]["messages"]] == [row["id"] for row in after[completed]["rows"]["session_message"]])
    report["restart"] = {"before": before, "after": after, "cursor": cursor, "suffix": saved_suffix, "exhausted": exhausted, "replayed": restart_replay}
    report["verdict"] = "passed"
except BaseException as error:
    report["verdict"] = "failed"
    report["failure"] = {"type": type(error).__name__, "message": str(error)}
    raise
finally:
    for hold in holds.values():
        hold.set()
    for scenario in ["completed", "interrupted"]:
        (workspace / (scenario + "-release")).write_text("release")
    stop_server()
    provider.shutdown()
    provider_thread.join(timeout=5)
    pool.shutdown(wait=True, cancel_futures=True)
    save()
    print(json.dumps({"verdict": report.get("verdict"), "failure": report.get("failure"), "evidence": str(root / "evidence.json"), "checks": report["checks"]}, indent=2))
