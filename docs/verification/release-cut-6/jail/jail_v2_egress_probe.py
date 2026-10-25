#!/usr/bin/env python3
"""Jail-boundary acceptance probe for swxtchio/swx-abbe#1952 (V2 egress) and #1953 (V2 compaction).

Runs the swx-abbe jail integration rig: the real jail daemon started by the image's own entrypoint,
the real egress proxy, and a fake provider. The client is opencode_client.OpenCodeClient itself,
run in a container on the rig's internal network, as the jail integration tests run it.

Usage, from the root of a swx-abbe checkout (or with SWX_ABBE_CHECKOUT set to one):
    python3 jail_v2_egress_probe.py v1-control v2 v2-named compact

Modes:
    v1-control  a V1 turn (OpenCodeClient.open_turn); shows the rig's egress path works
    v2          the V2 calls exactly as opencode_client.py sends them: create_session_v2(), then
                prompt_session_v2(session, "msg_...", {"text": ...})          <- the #1952 case
    v2-named    the same create with the session's model named (not sent by opencode_client.py today)
    compact     a V2 turn as in v2, settled with wait_session_v2, then the V2 compact call,
                POST /api/session/{id}/compact with no body                    <- the #1953 case

v1-control, v2 and v2-named run on the rig's default provider. They pass when it received a
chat-completions request for the egress model.

compact runs on this probe's own provider, which answers every request with text and logs what each
request is: the turn, a title, or a summary request (one whose user text holds OpenCode's
<conversation> block). It passes only when all of these hold:
    - the turn reached the provider and wait_session_v2 reported it completed;
    - the compact call answered 200 or 204;
    - the provider received a summary request carrying this session's turn;
    - the session history records a manual session.next.compaction.ended whose text is the
      provider's summary.
A 503, and a 204 that asked no model, both fail.

Each mode prints RESULT: PASS or RESULT: FAIL. The exit status is 0 only when every requested mode
passed.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.environ.get("SWX_ABBE_CHECKOUT", ".")).resolve()
VARIANT = ROOT / "components" / "orch" / "services" / "opencode-variant"
sys.path[:0] = [str(VARIANT / "tests"), str(VARIANT)]

import test_opencode_jail_integration as jail  # noqa: E402  the rig

MODEL = "accounts/fireworks/models/deepseek-v4p1-flash"
SIGNAL = f"/v1/chat/completions model={MODEL}"
MODES = ("v1-control", "v2", "v2-named", "compact")

QUESTION = "Please describe an SRT input."
ANSWERS = {
    "turn": "An SRT input listens on one UDP port and admits one caller.",
    "title": "SRT input description",
    "summary": "## Objective\n- Describe an SRT input.\n\n## Progress\n"
               "- Answered: an SRT input listens on one UDP port and admits one caller.",
}

# The compact mode's provider. Every request gets a text answer, streamed when the request asks
# for a stream. Each request is logged as REQ<n> <path> model=<id> stream=<bool> kind=<kind>, where
# kind is "summary" for a request whose user text holds a </conversation> close tag (OpenCode's
# compaction prompt wraps the transcript in <conversation> tags), "title" for OpenCode's title
# prompt, and "turn" otherwise. A summary line adds transcript=yes when that block holds this
# probe's question and the turn's answer.
COMPACT_PROVIDER = r'''
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os

IDENTITY = os.environ["PROVIDER_IDENTITY"]
QUESTION, ANSWERS = json.loads(__CONSTANTS__)


def texts(request):
    found = []
    for message in request.get("messages") or []:
        content = message.get("content")
        if isinstance(content, str):
            found.append((message.get("role"), content))
        elif isinstance(content, list):
            found.extend((message.get("role"), str(part.get("text", "")))
                         for part in content if isinstance(part, dict))
    return found


def classify(request):
    found = texts(request)
    users = [text for role, text in found if role == "user"]
    if any("</conversation>" in text for text in users):
        carried = any(QUESTION in text and ANSWERS["turn"] in text for text in users)
        return "summary", " transcript=" + ("yes" if carried else "no")
    if any("You are a title generator" in text or "Generate a title for this conversation" in text
           for _, text in found):
        return "title", ""
    return "turn", ""


class Handler(BaseHTTPRequestHandler):
    requests = 0

    def log_message(self, *_args):
        return

    def _send(self, status, body=b"", content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        authorized = (
            self.path == "/v1/chat/completions"
            and self.headers.get("Authorization") == "Bearer fake-vendor-key"
        )
        if "swxtch_readiness_probe" in request:
            if not authorized:
                self._send(401)
                return
            self._send(200, json.dumps({
                "readiness": {"provider": IDENTITY, "nonce": request["swxtch_readiness_probe"]},
                "choices": [],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }).encode())
            return
        Handler.requests += 1
        kind, detail = classify(request)
        model = request.get("model")
        print("REQ%d %s model=%s stream=%s kind=%s%s" % (
            Handler.requests, self.path, model, request.get("stream"), kind, detail), flush=True)
        if not authorized:
            self._send(401)
            return
        answer = ANSWERS[kind]
        usage = {"prompt_tokens": 10, "completion_tokens": 5}
        if not request.get("stream"):
            self._send(200, json.dumps({
                "id": "chatcmpl-compact-rig", "object": "chat.completion", "created": 1, "model": model,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": answer},
                             "finish_reason": "stop"}],
                "usage": usage,
            }).encode())
            return
        frames = [
            {"choices": [{"index": 0, "delta": {"role": "assistant", "content": answer},
                          "finish_reason": None}], "usage": None},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}], "usage": usage},
        ]
        body = "".join(
            "data: " + json.dumps({"id": "chatcmpl-compact-rig", "object": "chat.completion.chunk",
                                   "created": 1, "model": model, **frame}) + "\n\n"
            for frame in frames
        ) + "data: [DONE]\n\n"
        self._send(200, body.encode(), "text/event-stream")


ThreadingHTTPServer(("0.0.0.0", 18081), Handler).serve_forever()
'''.replace("__CONSTANTS__", repr(json.dumps([QUESTION, ANSWERS])))

CLIENT = r'''
import json, sys, time, urllib.parse, uuid
sys.path.insert(0, "/src/components/orch/services/opencode-variant")
import opencode_client as oc

endpoint, password, mode = sys.argv[1], sys.argv[2], sys.argv[3]
client = oc.OpenCodeClient(endpoint=endpoint, password=password, directory="/workspace/project", timeout=30)
out = {"mode": mode}


def history(sid):
    events, after = [], None
    for _ in range(50):
        page = client.get_session_history_v2(sid, after_seq=after, limit=100)
        events += page["data"]
        if not page["hasMore"] or not page["data"]:
            return events
        after = page["data"][-1]["durable"]["seq"]
    return events


try:
    if mode == "v1-control":
        turn = client.open_turn(session_id="v1-control", message="Please describe an SRT input.")
        list(turn.events())
        response = turn.accumulator.build_response()
        out.update(status=response["status"], model=turn.accumulator.model_id)
    elif mode == "compact":
        # POST /api/session  {"location":{"directory":"/workspace/project"}}
        sid = client.create_session_v2()["id"]
        out["session"] = sid
        # POST /api/session/{sid}/prompt  {"id":"msg_...","prompt":{"text":"..."}}
        receipt = client.prompt_session_v2(sid, "msg_" + uuid.uuid4().hex[:16],
                                           {"text": "Please describe an SRT input."})
        out["admitted_seq"] = receipt.admitted_seq
        # POST /api/session/{sid}/wait  (no body)
        try:
            out["turn"] = client.wait_session_v2(receipt, deadline=time.monotonic() + 90).status
        except oc.OpenCodeError as exc:
            out["turn_error"] = str(exc)[:300]
        # POST /api/session/{sid}/compact  (no body), sent as interrupt_session_v2 sends its POST
        deadline = time.monotonic() + 120
        try:
            response = client._request(
                "compactSessionV2", f"/api/session/{urllib.parse.quote(sid, safe='')}/compact",
                method="POST", expected_status=(200, 204), include_directory=False, deadline=deadline,
            )
            out["compact_status"] = oc._response_status(response)
            oc._discard_response(response, deadline=deadline)
        except oc.OpenCodeError as exc:
            out["compact_status"] = exc.status
            out["compact_refused"] = str(exc)[:300]
        # GET /api/session/{sid}/history, every page; once the compact call has succeeded, read
        # until a compaction has ended (the call answers after the summary is recorded, so the
        # first read normally has it; 20 s bounds a missing one)
        until = time.monotonic() + (20 if out.get("compact_status") in (200, 204) else 0)
        while True:
            events = history(sid)
            ended = [e for e in events if e.get("type") == "session.next.compaction.ended"]
            if ended or time.monotonic() >= until:
                break
            time.sleep(1)
        out["history_types"] = [event.get("type") for event in events]
        out["compactions"] = [
            {"type": event.get("type"), "reason": (event.get("data") or {}).get("reason"),
             "text": str((event.get("data") or {}).get("text", ""))[:400]}
            for event in events if str(event.get("type", "")).startswith("session.next.compaction.")
            and not str(event.get("type")).endswith(".delta")
        ]
    else:
        if mode == "v2":
            # POST /api/session  {"location":{"directory":"/workspace/project"}}
            session = client.create_session_v2()
        else:
            deadline = time.monotonic() + 30
            body = {
                "location": {"directory": client.directory},
                "model": {"providerID": "swxtch-egress", "id": "accounts/fireworks/models/deepseek-v4p1-flash"},
            }
            response = client._request("createSessionV2", "/api/session", method="POST", body=body,
                                       include_directory=False, deadline=deadline)
            session = oc._v2_data_object(oc._read_json(response, "createSessionV2", deadline=deadline),
                                         "createSessionV2")
        sid = session["id"]
        out["session"] = sid
        # POST /api/session/{sid}/prompt  {"id":"msg_...","prompt":{"text":"..."}}
        receipt = client.prompt_session_v2(sid, "msg_" + uuid.uuid4().hex[:16],
                                           {"text": "Please describe an SRT input."})
        out["admitted_seq"] = receipt.admitted_seq
        events = []
        for _ in range(45):
            page = client.get_session_history_v2(sid)
            events = page.get("data") if isinstance(page.get("data"), list) else page.get("events", [])
            types = [event.get("type", "") for event in events]
            if any(t.endswith(("step.failed", "step.ended", "step.completed", ".idle")) for t in types):
                break
            time.sleep(1)
        out["history_types"] = [event.get("type") for event in events]
        out["steps"] = [
            {"type": event.get("type"), "model": (event.get("data") or {}).get("model"),
             "error": (event.get("data") or {}).get("error")}
            for event in events if str(event.get("type", "")).startswith("session.next.step")
        ]
        try:
            outcome = client.wait_session_v2(receipt, deadline=time.monotonic() + 20)
            out["wait"] = repr(outcome)[:300]
        except oc.OpenCodeError as exc:
            out["wait_error"] = str(exc)[:300]
except oc.OpenCodeError as exc:
    out["client_error"] = f"{type(exc).__name__}: {exc}"[:400]
print(json.dumps(out), flush=True)
'''


def compact_verdict(result: dict, requests: list[str]) -> list[str]:
    """Every unmet compact-mode condition, in the order the calls ran; empty means PASS."""
    unmet = []
    if not any(" kind=turn" in line for line in requests):
        unmet.append("no turn request reached the provider, so the session has no transcript to compact")
    if result.get("turn") != "completed":
        unmet.append(f"the turn did not complete: {result.get('turn') or result.get('turn_error') or result.get('client_error')}")
    if result.get("compact_status") not in (200, 204):
        unmet.append(f"the compact call answered {result.get('compact_status')}: {result.get('compact_refused')}")
    summaries = [line for line in requests if " kind=summary" in line]
    if not summaries:
        unmet.append("the provider received no summary request")
    elif not any(line.endswith(" transcript=yes") for line in summaries):
        unmet.append("no summary request carried this session's turn")
    ended = [entry for entry in result.get("compactions", [])
             if entry["type"] == "session.next.compaction.ended" and entry["reason"] == "manual"]
    if not ended:
        unmet.append("the history records no completed manual compaction (session.next.compaction.ended)")
    elif not any(entry["text"] == ANSWERS["summary"] for entry in ended):
        unmet.append("the recorded compaction text is not the provider's summary")
    return unmet


def run(mode: str) -> bool:
    network = f"jail-v2-egress-{os.getpid()}-{mode}"
    rig_options = {"provider_code": COMPACT_PROVIDER} if mode == "compact" else {}
    subprocess.run(["docker", "network", "create", "--internal", network], check=True, capture_output=True)
    try:
        with jail._brain_rig(network, **rig_options) as rig:
            client = subprocess.run(
                [
                    "docker", "run", "--rm", "--network", rig["network"],
                    "--read-only", "--cap-drop", "ALL", "--tmpfs", "/tmp:rw,size=16m",
                    "-v", f"{ROOT}:/src:ro", "--entrypoint", "python3", jail.JAIL_IMAGE,
                    "-c", CLIENT, rig["endpoint"], jail.SERVER_PASSWORD, mode,
                ],
                capture_output=True, text=True, timeout=300,
            )
            provider = subprocess.run(["docker", "logs", rig["provider"]], capture_output=True, text=True, timeout=15)
            daemon = subprocess.run(["docker", "logs", rig["name"]], capture_output=True, text=True, timeout=15)
    finally:
        subprocess.run(["docker", "network", "rm", network], capture_output=True)
    requests = [line for line in (provider.stdout + provider.stderr).splitlines() if line.startswith("REQ")]
    errors = [
        line for line in (daemon.stdout + daemon.stderr).splitlines()
        if any(key in line for key in ("Failed to drain", "ModelNotSelected", "ModelUnavailable", "level=ERROR"))
    ]
    client_line = (client.stdout.strip().splitlines() or ["<no output>"])[-1]
    if mode == "compact":
        try:
            unmet = compact_verdict(json.loads(client_line), requests)
        except (ValueError, KeyError, TypeError) as exc:
            unmet = [f"the client printed no readable result ({type(exc).__name__})"]
        passed = not unmet
    else:
        unmet = []
        passed = any(SIGNAL in line for line in requests)
    print(f"=== {mode}")
    print("client:", client_line)
    if client.returncode != 0:
        print("client stderr:", client.stderr.strip()[-800:])
    print("provider requests:", json.dumps(requests))
    for line in errors[-5:]:
        print("daemon:", line[:400])
    for reason in unmet:
        print("unmet:", reason)
    print("RESULT:", "PASS" if passed else "FAIL")
    return passed


if __name__ == "__main__":
    modes = sys.argv[1:] or ["v1-control", "v2", "v2-named"]
    unknown = [mode for mode in modes if mode not in MODES]
    if unknown:
        sys.exit(f"unknown mode(s): {' '.join(unknown)}")
    results = [run(mode) for mode in modes]
    sys.exit(0 if all(results) else 1)
